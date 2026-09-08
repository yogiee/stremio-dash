#!/usr/bin/env python3
"""
stremio-dash - read-only live dashboard for a Stremio streaming server.

The Stremio apps on tvOS and iPadOS show no network stats, but they stream through
a shared streaming server which knows everything. This surfaces it.

Strictly an observer: it never sits in the media byte path, so if it dies, playback
is unaffected.

Sources, in descending order of portability:
  1. GET /stats.json            - per-engine swarm stats, trackers, wires (peers)
  2. GET /{ih}/{idx}/stats.json - streamName / streamLen / streamProgress
  3. ffprobe over HTTP          - the bitrate playback actually requires
  4. docker logs -f             - range requests -> playhead byte offset, seeks
  5. ss -tina in the netns      - client sockets (real LAN IPs) + peer dial funnel

Only 1-3 work against a Stremio server on another host; 4 needs Docker access and
5 needs to reach that container's network namespace. See docs/modes.md.
"""

import http.client
import json
import os
import calendar
import re
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------- config
# Everything lives in config.json (see config.example.json). Environment variables
# still override it, so an existing env-driven deployment keeps working unchanged.
DEFAULTS = {
    "listen": {"host": "0.0.0.0", "port": 9481},
    "poll": {"interval_sec": 2.0, "detail_sec": 6.0, "settings_sec": 60.0, "history": 180},
    "rdns": True,
    "active": "local",
    "servers": [{
        "id": "local",
        "name": "Stremio",
        "url": "http://127.0.0.1:11470",
        "container": "stremio-docker",
        "cache_dir": None,          # null => measure inside the container instead
        "client_names": {},
    }],
}
CONFIG_PATHS = [os.environ.get("DASH_CONFIG"),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"),
                "/config/config.json"]


def load_config():
    for path in filter(None, CONFIG_PATHS):
        try:
            with open(path) as fh:
                user = json.load(fh)
        except FileNotFoundError:
            continue
        except Exception as exc:
            print(f"config: {path} unreadable ({exc}); using defaults", flush=True)
            break
        cfg = json.loads(json.dumps(DEFAULTS))       # deep copy
        for key, val in user.items():
            if isinstance(val, dict) and isinstance(cfg.get(key), dict):
                cfg[key].update(val)
            else:
                cfg[key] = val
        cfg["_path"] = path
        return cfg
    return json.loads(json.dumps(DEFAULTS))


CFG = load_config()
CFG_LOCK = threading.RLock()

# The active profile is switchable at runtime from the settings popup, so what used
# to be module constants now live in ACTIVE behind CFG_LOCK. Environment variables
# still win -- but they pin that field for the whole process, so the UI is TOLD and
# disables the control rather than pretending a switch took effect.
ENV_PINNED = {"url": "STREMIO_URL" in os.environ,
              "container": "STREMIO_CONTAINER" in os.environ,
              "cache_dir": "STREMIO_CACHE" in os.environ}
ACTIVE = {"id": None, "name": "", "url": "", "netloc": "", "scheme": "http",
          "container": "", "cache_dir": "", "client_names": {}}
# Bumped on every switch. Anything holding derived state compares its own copy and
# throws that state away when it moves -- history from server A must never be shown
# against server B.
GEN = 0

LISTEN_HOST  = os.environ.get("DASH_HOST", CFG["listen"]["host"])
LISTEN_PORT  = int(os.environ.get("DASH_PORT", CFG["listen"]["port"]))
POLL         = float(os.environ.get("DASH_POLL", CFG["poll"]["interval_sec"]))
# Every request we make is logged by the Stremio server, and Docker's json-file logs
# are unrotated by default. Only /stats.json needs the fast cadence; progress and
# settings move slowly, so they get their own longer intervals.
DETAIL_SEC   = float(os.environ.get("DASH_DETAIL_SEC", CFG["poll"]["detail_sec"]))
SETTINGS_SEC = float(os.environ.get("DASH_SETTINGS_SEC", CFG["poll"]["settings_sec"]))
HISTORY      = int(os.environ.get("DASH_HISTORY", CFG["poll"]["history"]))
DO_RDNS      = os.environ.get("DASH_RDNS", "1" if CFG.get("rdns", True) else "0") == "1"

CLIENT_PORTS = {11470, 12470}
WEBUI_PORTS  = {8080}
LIVE_STATES  = {"ESTAB", "CLOSE-WAIT", "FIN-WAIT-1", "FIN-WAIT-2"}
# The Docker bridge gateway is this dashboard polling the API, not a viewer. Its
# address differs per host and per network, so it is discovered rather than assumed.
IGNORE_CLIENT_IPS = {"127.0.0.1"}

STATE_LOCK = threading.Lock()
STATE = {
    "ok": False, "error": None, "ts": 0, "server_version": None,
    "server_name": "", "mode": None,
    "engines": {}, "clients": [], "funnel": {}, "cache": {}, "settings": {},
}
HIST     = defaultdict(lambda: deque(maxlen=HISTORY))   # infohash -> samples
PLAYHEAD = {}                                            # infohash -> playhead info
NEEDS    = {}                                            # infohash -> consumption-rate state
SOCK_HIST = defaultdict(lambda: deque(maxlen=150))       # socket key -> [(t, bytes_acked)]
# A player reads in bursts: it drains its buffer, then refills in one gulp and goes
# quiet. Measured on a real iPad playing a cached 1080p file, bursts were ~8.9 MB
# every ~30s. So "is it playing?" must be judged over a window wider than one burst,
# never on the instantaneous rate (which is 0 most of the time).
ACTIVE_WINDOW = 90.0
# Averaged over the whole retained history (~5 min), not a short window: a 4K player
# refills its buffer less often than 60s, which made a genuinely streaming client
# read "0 B/s". The long average is also the more honest figure here — it converges
# on the title's real bitrate.
RATE_WINDOW   = 1e9
RDNS = {}
RDNS_PENDING = set()
BITRATE = {}          # (infohash, idx) -> bytes/sec required for real-time playback
BITRATE_ERR = {}      # (infohash, idx) -> why the probe produced nothing
BITRATE_TRIED = {}    # (infohash, idx) -> last attempt ts


_CONN = {"c": None, "netloc": None}
_CONN_LOCK = threading.Lock()


def parse_server_url(url):
    """Validate a user-supplied server URL -> (normalised, scheme, netloc).

    The settings popup accepts free text, so this is the boundary that stops a typo
    -- or anything more deliberate -- from reaching http.client. The scheme is
    restricted to http/https because this value only ever addresses a Stremio
    server; it is never handed to a shell."""
    url = (url or "").strip()
    if re.search(r"[\s\x00-\x1f]", url):
        raise ValueError("URL contains whitespace or control characters")
    if "//" not in url:
        url = "http://" + url
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ("http", "https"):
        raise ValueError("URL must start with http:// or https://")
    if not u.hostname:
        raise ValueError("URL has no host")
    try:
        port = u.port
    except ValueError:
        raise ValueError("URL has an invalid port")
    host = f"[{u.hostname}]" if ":" in u.hostname else u.hostname   # IPv6 literal
    netloc = host if port is None else f"{host}:{port}"
    return f"{u.scheme}://{netloc}", u.scheme, netloc


def _connect(netloc, scheme, timeout):
    cls = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
    return cls(netloc, timeout=timeout)


def http_json(path, timeout=4):
    """Single keep-alive connection to the ACTIVE server. Reconnects on any error,
    retries once.

    Deliberately NOT one connection per request: at a 2s poll that left ~90
    TIME-WAIT sockets sitting in the container's table, which is both wasteful
    and pollutes the very client list this dashboard reports."""
    with CFG_LOCK:
        netloc, scheme = ACTIVE["netloc"], ACTIVE["scheme"]
    if not netloc:
        raise RuntimeError("no active server configured")
    with _CONN_LOCK:
        if _CONN["netloc"] != netloc:      # the profile was switched under us
            try:
                _CONN["c"].close()
            except Exception:
                pass
            _CONN.update(c=None, netloc=netloc)
        for attempt in (0, 1):
            try:
                if _CONN["c"] is None:
                    _CONN["c"] = _connect(netloc, scheme, timeout)
                c = _CONN["c"]
                c.request("GET", path, headers={"Connection": "keep-alive"})
                r = c.getresponse()
                body = r.read()
                if r.status != 200:
                    raise RuntimeError(f"HTTP {r.status} on {path}")
                return json.loads(body.decode("utf-8", "replace"))
            except Exception:
                try:
                    _CONN["c"].close()
                except Exception:
                    pass
                _CONN["c"] = None
                if attempt:
                    raise


def esc_html(t):
    return (str(t).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def human_addr(a):
    """[::ffff:192.0.2.10]:49840 -> (192.0.2.10, 49840)"""
    a = a.strip()
    if a.startswith("["):
        host, _, port = a.rpartition("]:")
        host = host[1:]
    else:
        host, _, port = a.rpartition(":")
    if host.startswith("::ffff:"):
        host = host[7:]
    try:
        port = int(port)
    except ValueError:
        port = 0
    return host, port


# ------------------------------------------------------------- container
_pid_cache = {"pid": None, "at": 0}
_TAILER = {"proc": None}


def container_pid():
    """Resolve the Stremio container's host PID, and learn its bridge gateway
    addresses. Traffic from a gateway is this dashboard polling the API, not a
    viewer -- and the bridge subnet differs per host, so it must be discovered
    rather than assumed."""
    with CFG_LOCK:
        container = ACTIVE["container"]
    if not container:
        return None                      # HTTP-only profile: mode A by definition
    now = time.time()
    if _pid_cache["pid"] and now - _pid_cache["at"] < 30:
        return _pid_cache["pid"]
    try:
        fmt = "{{.State.Pid}}{{range .NetworkSettings.Networks}} {{.Gateway}}{{end}}"
        out = subprocess.run(["docker", "inspect", "-f", fmt, container],
                             capture_output=True, text=True, timeout=8)
        parts = out.stdout.split()
        pid = int(parts[0])
        IGNORE_CLIENT_IPS.update(g for g in parts[1:] if g and g != "<no value>")
        if pid > 0:
            _pid_cache.update(pid=pid, at=now)
            return pid
    except Exception:
        pass
    _pid_cache.update(pid=None, at=now)
    return None


SS_KV = re.compile(r"(bytes_acked|bytes_sent|bytes_retrans|delivery_rate|rtt|retrans|"
                   r"rwnd_limited|backoff|lastsnd|lastrcv):(\S+)")


def read_sockets():
    """Client sockets (with delivered-byte rates) + BT peer dial funnel."""
    pid = container_pid()
    if not pid:
        with CFG_LOCK:
            bound = bool(ACTIVE["container"])
        return [], {"error": "container not running" if bound
                    else "no container bound - HTTP-only profile"}
    try:
        r = subprocess.run(["nsenter", "-t", str(pid), "-n", "ss", "-tina"],
                           capture_output=True, text=True, timeout=8)
        if r.returncode != 0:
            raise RuntimeError((r.stderr or "nsenter failed").strip()[:120])
        out = r.stdout
    except Exception as e:
        # Container was replaced (watchtower recreates it on a new pid) or died.
        _pid_cache.update(pid=None, at=0)
        SOCK_HIST.clear()
        return [], {"error": str(e)}

    clients, funnel = [], defaultdict(int)
    now = time.time()
    cur, seen = None, set()
    for line in out.splitlines():
        if not line.strip() or line.startswith("State"):
            continue
        if not line[0].isspace():                       # socket line
            f = line.split()
            if len(f) < 5:
                cur = None
                continue
            state, local, peer = f[0], f[3], f[4]
            lhost, lport = human_addr(local)
            rhost, rport = human_addr(peer)
            if state == "LISTEN" or lhost.startswith("127.0.0.11") or rhost.startswith("127.0.0.11"):
                cur = None
                continue
            if lport in CLIENT_PORTS:
                if rhost in IGNORE_CLIENT_IPS or state not in LIVE_STATES:
                    cur = None
                    continue
                cur = {"state": state, "ip": rhost, "port": rport, "via": lport,
                       "sendq": int(f[2]) if f[2].isdigit() else 0}
                clients.append(cur)
            elif lport in WEBUI_PORTS:
                cur = None
            else:                                        # outbound BT peer socket
                funnel[state] += 1
                cur = None
        elif cur is not None:                            # metrics line
            kv = dict(SS_KV.findall(line))
            acked = int(kv.get("bytes_acked", 0) or 0)
            key = f"{cur['ip']}:{cur['port']}->{cur['via']}"
            seen.add(key)
            h = SOCK_HIST[key]
            h.append((now, acked))

            # Average over a window, not between two polls: a burst-reading player
            # shows 0 B/s at almost any instant even while playing perfectly.
            base_t, base_a = h[0]          # oldest retained sample
            for t_, a_ in h:
                if now - t_ <= RATE_WINDOW:
                    base_t, base_a = t_, a_
                    break
            rate = (acked - base_a) / (now - base_t) if now > base_t else 0.0

            # Seconds since the client last actually took bytes.
            last_change = None
            for t_, a_ in reversed(h):
                if a_ != acked:
                    last_change = t_
                    break
            idle_for = (now - last_change) if last_change is not None else (now - h[0][0])

            cur.update(
                bytes_acked=acked,
                delivered_bps=max(0.0, rate),
                idle_for=idle_for,
                active=idle_for < ACTIVE_WINDOW,
                rtt=kv.get("rtt", "").split("/")[0],
                retrans=kv.get("bytes_retrans", "0"),
            )
            cur = None

    for k in list(SOCK_HIST):
        if k not in seen:
            SOCK_HIST.pop(k, None)

    est = funnel.get("ESTAB", 0)
    # Only SYN-SENT is a dial in flight. TIME-WAIT/LAST-ACK/FIN-WAIT are finished
    # connections lingering in the table and were inflating the "in flight" count.
    dialing = funnel.get("SYN-SENT", 0)
    return clients, {"established": est, "dialing": dialing,
                     "closing": sum(v for k, v in funnel.items()
                                    if k not in ("ESTAB", "SYN-SENT")),
                     "by_state": dict(funnel),
                     "hit_rate": (est / (est + dialing)) if (est + dialing) else 0.0}


# ------------------------------------------------------------- log tail
# The server logs range requests in two shapes depending on whether the client
# sent a query string:  "-> GET /<ih>/<idx> bytes=N-"  and  "... /<idx>? bytes=N-".
# The Stremio apps use the second; a bare curl produces the first. Match both.
LOG_RE = re.compile(r"-> GET /([0-9a-f]{40})/(\d+)(?:\?\S*)?\s+bytes=(\d+)-")
# docker logs -t prefixes RFC3339Nano UTC; we need the real time of each request so a
# replayed backlog does not look like it just happened.
TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d+)?Z\s")


def log_time(line, fallback):
    m = TS_RE.match(line)
    if not m:
        return fallback
    try:
        return calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S"))
    except Exception:
        return fallback


def log_tailer():
    """Follow the container log for range requests -> playhead byte offset."""
    while True:
        with CFG_LOCK:
            container, gen = ACTIVE["container"], GEN
        if not container:
            time.sleep(3)                # HTTP-only profile: no log to follow
            continue
        p = None
        try:
            p = subprocess.Popen(
                # Replay recent history so a dashboard restart mid-stream does not
                # lose the connection's start offset (clients rarely re-request).
                ["docker", "logs", "-f", "-t", "--tail", "3000", container],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            _TAILER["proc"] = p
            for line in p.stdout:
                if GEN != gen:           # switched servers: this log is now the wrong one
                    break
                m = LOG_RE.search(line)
                if not m:
                    continue
                ih, idx, start = m.group(1), int(m.group(2)), int(m.group(3))
                now = log_time(line, time.time())
                cur = PLAYHEAD.get(ih)
                if not cur:
                    PLAYHEAD[ih] = {"idx": idx, "offset": start, "at": now,
                                    "seeks": 0, "cand": None}
                    continue
                gap = abs(start - cur["offset"])
                if gap > 64 * 1024 * 1024:
                    # A lone far-away request is almost always the player reading the
                    # container index at the end of the file, not a seek. Require a
                    # second request nearby before believing the position moved.
                    cand = cur.get("cand")
                    if cand is not None and abs(start - cand) <= 64 * 1024 * 1024:
                        cur.update(offset=start, at=now, seeks=cur["seeks"] + 1, cand=None)
                    else:
                        cur["cand"] = start
                else:
                    cur.update(offset=start, at=now, cand=None)
        except Exception:
            time.sleep(5)
        finally:
            _TAILER["proc"] = None
            if p is not None:
                try:
                    p.terminate()
                except Exception:
                    pass
        # Stream ended: usually the container was recreated (watchtower) or
        # restarted. Re-attach by NAME, which resolves to the new container.
        time.sleep(3)


# ------------------------------------------------------------ rdns/cache
def rdns_worker(q):
    while True:
        ip = q.get()
        try:
            RDNS[ip] = socket.gethostbyaddr(ip)[0]
        except Exception:
            RDNS[ip] = ""
        finally:
            RDNS_PENDING.discard(ip)


def probe_worker(q):
    """One ffprobe per stream, in the background, to get the REQUIRED bitrate.

    The server's own /probe endpoint 500s on this build, but ffprobe against the
    stream URL returns in ~0.15s even when the file is only 2.7% downloaded (the
    header is at the front and already cached). This is the honest "how fast does
    playback need to be fed" number; deriving it from the playhead was unreliable
    because players issue non-playback range requests.

    Runs inside the container when one is bound -- the stream is always reachable at
    the container-local port, whatever it is published as. For an HTTP-only profile
    there is no container to exec into, so it falls back to ffprobe on THIS host
    against the server URL. That fallback is what makes the verdict work in mode A;
    without it a remote profile has no required-bitrate figure at all."""
    ARGS = ["-v", "quiet", "-print_format", "json",
            "-show_entries", "format=duration,bit_rate,size"]
    while True:
        ih, idx = q.get()
        key = (ih, idx)
        with CFG_LOCK:
            container, url = ACTIVE["container"], ACTIVE["url"]
        if container:
            cmd = ["docker", "exec", container, "ffprobe"] + ARGS + \
                  [f"http://127.0.0.1:11470/{ih}/{idx}"]
        else:
            cmd = ["ffprobe"] + ARGS + [f"{url}/{ih}/{idx}"]
        br, err = 0.0, ""
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            fmt = json.loads(r.stdout or "{}").get("format", {})
            if fmt.get("bit_rate"):
                br = float(fmt["bit_rate"]) / 8.0
            elif fmt.get("size") and fmt.get("duration"):
                br = float(fmt["size"]) / float(fmt["duration"])
            if not br:
                err = (r.stderr or "ffprobe returned no bitrate").strip()[:120]
        except FileNotFoundError:
            # Degrade explicitly: say the tool is missing rather than silently
            # showing no verdict forever.
            err = ("ffprobe not installed on the dashboard host"
                   if not container else "docker not available")
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:120]
        if br > 0:
            BITRATE[key] = br
            BITRATE_ERR.pop(key, None)
        else:
            BITRATE_ERR[key] = err or "unavailable"


# Stremio removes a cached file on eviction but leaves the infohash directory behind
# forever, so most entries are empty stubs and must not be counted as cached titles.
def cache_stats_host(cache_dir):
    """Measure a cache directory visible on this filesystem.

    -B1 is real disk usage: Stremio creates each file at full length and fills it
    sparsely, so apparent size (du -sb) overstates it badly."""
    out = subprocess.run(["du", "-s", "-B1", cache_dir],
                         capture_output=True, text=True, timeout=120).stdout.split()
    dirs = [d for d in os.scandir(cache_dir) if d.is_dir()]
    held = sum(1 for d in dirs if any(os.scandir(d.path)))
    return {"bytes": int(out[0]), "entries": held, "stubs": len(dirs) - held,
            "dir": cache_dir, "via": "host"}


def cache_stats_container(container):
    """Ask the container to measure its own cache -- no host path, works wherever the
    Docker API reaches. Note busybox du rejects -B1, but -sk is fine."""
    root = ((STATE.get("settings") or {}).get("cacheRoot") or "/root/.stremio-server")
    path = root.rstrip("/") + "/stremio-cache"
    script = (
        'D="%s"; [ -d "$D" ] || exit 3; '
        'du -sk "$D" | cut -f1; '
        'ls -1 "$D" 2>/dev/null | wc -l; '
        'n=0; for x in "$D"/*/; do [ -n "$(ls -A "$x" 2>/dev/null)" ] && n=$((n+1)); done; '
        'echo $n' % path)
    out = subprocess.run(["docker", "exec", container, "sh", "-c", script],
                         capture_output=True, text=True, timeout=120)
    kb, total, held = (int(v) for v in out.stdout.split()[:3])
    return {"bytes": kb * 1024, "entries": held, "stubs": total - held,
            "dir": path, "via": "container"}


def cache_worker():
    while True:
        with CFG_LOCK:
            cache_dir, container = ACTIVE["cache_dir"], ACTIVE["container"]
        try:
            if cache_dir:
                stats = cache_stats_host(cache_dir)
            elif container:
                stats = cache_stats_container(container)
            else:
                # Mode A: the cache lives on a host we cannot reach. Saying so beats
                # showing a blank tile.
                stats = {"error": "no container bound - HTTP-only profile",
                         "dir": "(unreachable)"}
            with STATE_LOCK:
                STATE["cache"] = stats
        except Exception as e:
            with STATE_LOCK:
                STATE["cache"] = {"error": str(e), "dir": cache_dir or "(container)"}
        time.sleep(60)


# --------------------------------------------------------- servers / config
# Everything below backs the settings popup. Two rules shape it, and they are not
# the same rule:
#
#   1. A server URL is free text -- that is the point, any host and any port -- but
#      it only ever reaches http.client, never a shell. parse_server_url() is the
#      boundary.
#   2. A CONTAINER NAME is never accepted from the client. Container names are handed
#      to `docker exec` and `nsenter`, so the popup may only choose from the set this
#      process itself discovered, by id, re-validated against a fresh `docker ps` at
#      write time. The UI therefore offers the container it found without ever being
#      able to name an arbitrary one -- which is what "no Docker endpoints from the
#      UI" was protecting against.
#
# There is still no authentication (settled: this is a LAN tool). Anyone who can
# reach the dashboard can repoint it. What they cannot do is make it run something.

PORT_RE = re.compile(r"(?:(?:\d+\.\d+\.\d+\.\d+|\[[^\]]+\]):)?(\d+)->(\d+)/tcp")


def published_port(ports, want):
    """0.0.0.0:11470->11470/tcp  ->  11470 (the HOST port, which may differ)."""
    for m in PORT_RE.finditer(ports or ""):
        if int(m.group(2)) == want:
            return int(m.group(1))
    return None


def discover_containers():
    """Stremio containers on the local Docker daemon, for the popup to offer.

    Read-only: `docker ps` and nothing else. Matching is on name or image because
    the image is what identifies a Stremio server build, while the name is what the
    user recognises."""
    try:
        r = subprocess.run(
            ["docker", "ps", "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Ports}}"],
            capture_output=True, text=True, timeout=8)
        if r.returncode != 0:
            return {"docker": False, "containers": [],
                    "error": (r.stderr or "docker ps failed").strip()[:160]}
    except FileNotFoundError:
        return {"docker": False, "containers": [],
                "error": "no docker CLI on the dashboard host"}
    except Exception as e:
        return {"docker": False, "containers": [], "error": f"{type(e).__name__}: {e}"[:160]}

    out = []
    for line in r.stdout.splitlines():
        f = line.split("\t")
        if len(f) < 4:
            continue
        cid, name, image, ports = f[0], f[1], f[2], f[3]
        if "stremio" not in f"{name} {image}".lower():
            continue
        hp = published_port(ports, 11470)
        out.append({"id": cid, "name": name, "image": image, "port": hp,
                    "url": f"http://127.0.0.1:{hp}" if hp else "",
                    "ports": ports})
    return {"docker": True, "containers": out, "error": None}


def resolve_discovered(container_id):
    """Map a container id the UI offered back to its real name, re-checking it still
    exists. This is the closed vocabulary: nothing else may set ACTIVE['container']."""
    if not container_id:
        return ""
    for c in discover_containers().get("containers") or []:
        if c["id"] == container_id or c["name"] == container_id:
            return c["name"]
    raise ValueError("that container is no longer running")


def _probe_pid(container):
    """Container PID without touching the live _pid_cache or IGNORE_CLIENT_IPS --
    this runs against servers that are not the active one."""
    try:
        r = subprocess.run(["docker", "inspect", "-f", "{{.State.Pid}}", container],
                           capture_output=True, text=True, timeout=8)
        pid = int((r.stdout or "0").strip() or 0)
        return pid or None
    except Exception:
        return None


def probe_server(url, container=""):
    """Is this URL a reachable Stremio server, and which mode would it achieve?

    Called before anything is written: a profile that cannot be reached is never
    saved. Also reports the mode so the popup can say what the addition will
    actually buy, rather than implying every server is equal."""
    try:
        norm, scheme, netloc = parse_server_url(url)
    except ValueError as e:
        return {"ok": False, "url": url, "error": str(e)}

    info = {"ok": False, "url": norm, "version": None, "mode": None, "notes": []}
    try:
        c = _connect(netloc, scheme, 4)
        try:
            c.request("GET", "/settings", headers={"Connection": "close"})
            r = c.getresponse()
            body = r.read()
        finally:
            c.close()
        if r.status != 200:
            info["error"] = f"HTTP {r.status} from {norm}/settings"
            return info
        vals = (json.loads(body.decode("utf-8", "replace")) or {}).get("values") or {}
    except Exception as e:
        info["error"] = f"{type(e).__name__}: {e}"[:160]
        return info

    if "serverVersion" not in vals:
        info["error"] = "reachable, but does not look like a Stremio streaming server"
        return info

    info.update(ok=True, version=vals.get("serverVersion"), mode="A")
    if container:
        pid = _probe_pid(container)
        if not pid:
            info["notes"].append(f"container '{container}' is not running - HTTP only")
        else:
            info["mode"] = "B"
            try:
                rr = subprocess.run(["nsenter", "-t", str(pid), "-n", "ss", "-tin"],
                                    capture_output=True, text=True, timeout=8)
                if rr.returncode == 0:
                    info["mode"] = "C"
                else:
                    info["notes"].append("no netns access - no clients or dial funnel")
            except Exception:
                info["notes"].append("nsenter unavailable - no clients or dial funnel")
    return info


# ------------------------------------------------------------ config writes
def find_server(cfg, sid):
    return next((s for s in cfg["servers"] if s.get("id") == sid), None)


def new_id(cfg, name):
    base = re.sub(r"[^a-z0-9]+", "-", (name or "server").lower()).strip("-") or "server"
    sid, n = base, 2
    while find_server(cfg, sid):
        sid, n = f"{base}-{n}", n + 1
    return sid


def save_config(cfg):
    """Atomic replace, so a crash mid-write cannot leave an unparseable config."""
    path = cfg.get("_path") or next((c for c in CONFIG_PATHS if c), None)
    if not path:
        raise RuntimeError("no config path to write to")
    body = {k: v for k, v in cfg.items() if not k.startswith("_")}
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(path)) or ".",
                               prefix=".config.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(body, fh, indent=2)
            fh.write("\n")
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise
    cfg["_path"] = path


def reset_derived():
    """History, playhead, sockets and bitrates all describe ONE server. Showing
    server A's sparkline under server B's name would be a lie, so a switch drops the
    lot and the UI simply refills over the next few ticks."""
    with _CONN_LOCK:
        try:
            _CONN["c"].close()
        except Exception:
            pass
        _CONN.update(c=None, netloc=None)
    for d in (HIST, PLAYHEAD, NEEDS, SOCK_HIST, BITRATE, BITRATE_ERR,
              BITRATE_TRIED, RDNS, RDNS_PENDING):
        d.clear()
    _pid_cache.update(pid=None, at=0)
    IGNORE_CLIENT_IPS.clear()
    IGNORE_CLIENT_IPS.add("127.0.0.1")
    proc = _TAILER.get("proc")          # unblock the tailer from the old container
    if proc is not None:
        try:
            proc.terminate()
        except Exception:
            pass
    with STATE_LOCK:
        STATE.update(ok=False, error=None, ts=0, server_version=None,
                     engines={}, clients=[], funnel={}, cache={}, settings={})


def set_active(srv):
    """Point the collector at a profile. Environment overrides still win."""
    global GEN
    norm, scheme, netloc = parse_server_url(os.environ.get("STREMIO_URL", srv.get("url", "")))
    with CFG_LOCK:
        ACTIVE.update(
            id=srv.get("id"), name=srv.get("name") or srv.get("id") or "Stremio",
            url=norm, scheme=scheme, netloc=netloc,
            container=os.environ.get("STREMIO_CONTAINER", srv.get("container") or ""),
            cache_dir=os.environ.get("STREMIO_CACHE", srv.get("cache_dir") or ""),
            client_names=dict(srv.get("client_names") or {}))
        GEN += 1
    reset_derived()


def config_view():
    """What the settings popup is allowed to see."""
    with CFG_LOCK:
        return {
            "active": ACTIVE["id"],
            "active_url": ACTIVE["url"],
            "env_pinned": ENV_PINNED,
            "path": CFG.get("_path") or "(defaults - nothing on disk yet)",
            "servers": [{"id": s.get("id"),
                         "name": s.get("name") or s.get("id"),
                         "url": s.get("url"),
                         "container": s.get("container") or "",
                         "client_names": s.get("client_names") or {}}
                        for s in CFG["servers"]],
        }


# -------------------------------------------------------------- collector
def main_file_idx(files):
    if not files:
        return None
    return max(range(len(files)), key=lambda i: files[i].get("length", 0))


def collect():
    import queue
    rq = queue.Queue()
    pq = queue.Queue()
    threading.Thread(target=probe_worker, args=(pq,), daemon=True).start()
    det_cache = {}          # ih -> (fetched_at, detail json)
    set_cache = {"at": 0.0, "v": {}}
    if DO_RDNS:
        threading.Thread(target=rdns_worker, args=(rq,), daemon=True).start()
    my_gen = GEN

    while True:
        t0 = time.time()
        if GEN != my_gen:          # switched profile: these caches describe the old one
            my_gen = GEN
            det_cache.clear()
            set_cache.update(at=0.0, v={})
        try:
            raw = http_json("/stats.json")
            engines = {}
            for ih, s in raw.items():
                files = s.get("files") or []
                idx = main_file_idx(files)
                prev = det_cache.get(ih)
                det = prev[1] if prev else {}
                if idx is not None and (prev is None or t0 - prev[0] >= DETAIL_SEC):
                    try:
                        det = http_json(f"/{ih}/{idx}/stats.json", timeout=4)
                        det_cache[ih] = (t0, det)
                    except Exception:
                        pass
                wires = [w for w in (s.get("wires") or []) if w.get("address")]
                for w in wires:
                    ip = w["address"].rsplit(":", 1)[0]
                    w["ip"] = ip
                    w["rdns"] = RDNS.get(ip, "")
                    if DO_RDNS and ip not in RDNS and ip not in RDNS_PENDING and rq.qsize() < 200:
                        RDNS_PENDING.add(ip)
                        rq.put(ip)

                speed = s.get("downloadSpeed") or 0
                ph = PLAYHEAD.get(ih)
                length = det.get("streamLen") or (files[idx]["length"] if idx is not None else 0)
                progress = det.get("streamProgress")

                sample = {"t": t0, "speed": speed, "peers": s.get("peers") or 0,
                          "unchoked": s.get("unchoked") or 0}
                HIST[ih].append(sample)
                hist = list(HIST[ih])

                # Empirical playback rate: how fast the playhead advances. Beats
                # ffprobe (the server's /probe 500s) and needs no duration metadata.
                need = 0.0
                if ph:
                    n = NEEDS.setdefault(ih, {"prev_offset": ph["offset"],
                                              "prev_at": ph["at"], "need": 0.0})
                    dt = ph["at"] - n["prev_at"]
                    do = ph["offset"] - n["prev_offset"]
                    if dt >= 20:
                        n["need"] = (do / dt) if do > 0 else 0.0   # <=0 == seek back/stopped
                        n["prev_offset"], n["prev_at"] = ph["offset"], ph["at"]
                    need = n["need"]

                engines[ih] = {
                    "infohash": ih,
                    "name": det.get("streamName") or s.get("name") or ih,
                    "torrent": s.get("name") or "",
                    "idx": idx,
                    "len": length,
                    "progress": progress,
                    "downloaded": s.get("downloaded") or 0,
                    "uploaded": s.get("uploaded") or 0,
                    "speed": speed,
                    "peers": s.get("peers") or 0,
                    "unchoked": s.get("unchoked") or 0,
                    "queued": s.get("queued") or 0,
                    "unique": s.get("unique") or 0,
                    "tries": s.get("connectionTries") or 0,
                    "swarm": s.get("swarmSize") or 0,
                    "swarm_conn": s.get("swarmConnections") or 0,
                    "paused": bool(s.get("swarmPaused")),
                    "searching": bool(det.get("peerSearchRunning")),
                    "sources": s.get("sources") or [],
                    "wires": wires,
                    "playhead": ph,
                    "need_bps": need,
                    "spark": [h["speed"] for h in hist],
                    "peer_spark": [h["peers"] for h in hist],
                }

            clients, funnel = read_sockets()
            total_ingest = sum(e["speed"] for e in engines.values())
            with CFG_LOCK:
                names = ACTIVE["client_names"]
            for c in clients:
                c["name"] = names.get(c["ip"], "")
                # Nothing arriving from peers while the client is consuming means the
                # bytes are coming off the local cache, not the swarm.
                c["from_cache"] = bool(c.get("active")) and total_ingest <= 0
            demand = sum(c["delivered_bps"] for c in clients if c.get("active"))
            consumers = sum(1 for c in clients if c.get("active"))
            # A client socket carries no infohash, but the LOG does: the engine whose
            # playhead moved recently is the one being read. That beats the old
            # "only if a single engine is loaded" rule, which gave up as soon as a
            # finished title was still sitting in the cache.
            being_read = [ih for ih, e in engines.items()
                          if e["playhead"] and t0 - e["playhead"]["at"] < 90]
            target = being_read[0] if len(being_read) == 1 else (
                list(engines)[0] if len(engines) == 1 else None)
            for ih, e in engines.items():
                on = (ih == target)
                e["cached"] = (e["progress"] or 0) >= 0.999
                e["being_read"] = ih in being_read
                e["demand_bps"] = demand if on else 0
                e["consumers"] = consumers if on else 0
                e["attributable"] = target is not None
                e["any_consumers"] = consumers > 0
                # Playback position = where this connection started + what the kernel
                # has actually delivered on it. Both reset together when a client
                # reconnects, so the pair stays consistent. This over-estimates the
                # true playhead by the player's own buffer, which errs toward warning
                # early rather than late.
                acked = sum(c["bytes_acked"] for c in clients if c.get("active"))
                ph = e["playhead"]
                consumed = (ph["offset"] + acked) if (on and ph and acked) else None
                cached_bytes = (e["progress"] or 0) * (e["len"] or 0)
                ahead = max(0.0, cached_bytes - consumed) if consumed is not None else None
                e["ahead_bytes"] = ahead

                key = (ih, e["idx"])
                e["required_bps"] = BITRATE.get(key, 0.0)
                e["required_err"] = BITRATE_ERR.get(key, "")
                req, sp = e["required_bps"], e["speed"]
                e["buffered_secs"] = (ahead / req) if (ahead is not None and req) else None
                if not req or ahead is None:
                    e["runway_secs"] = None      # unknown
                elif sp >= req or cached_bytes >= (e["len"] or 0):
                    e["runway_secs"] = -1        # sustainable / whole file in hand
                else:
                    e["runway_secs"] = ahead / (req - sp)
                if key not in BITRATE and t0 - BITRATE_TRIED.get(key, 0) > 120:
                    BITRATE_TRIED[key] = t0
                    pq.put(key)

            st = set_cache["v"]
            if t0 - set_cache["at"] >= SETTINGS_SEC:
                try:
                    st = http_json("/settings", timeout=3).get("values", {})
                    set_cache.update(at=t0, v=st)
                except Exception:
                    pass

            # Report the mode actually achieved, not the one configured: a bound
            # container that is not running buys nothing, and saying "B" then would
            # be exactly the kind of unsupported claim this dashboard avoids.
            with CFG_LOCK:
                bound, sname = bool(ACTIVE["container"]), ACTIVE["name"]
            ferr = funnel.get("error") or ""
            if not bound or "not running" in ferr:
                mode = "A"
            elif not ferr:
                mode = "C"
            else:
                mode = "B"
            with STATE_LOCK:
                STATE.update(ok=True, error=None, ts=t0, engines=engines,
                             clients=clients, funnel=funnel, settings=st,
                             server_version=st.get("serverVersion"),
                             server_name=sname, mode=mode)
            for ih in list(HIST):
                if ih not in engines:
                    HIST.pop(ih, None)
                    PLAYHEAD.pop(ih, None)
                    NEEDS.pop(ih, None)
                    det_cache.pop(ih, None)
        except Exception as e:
            with STATE_LOCK:
                STATE.update(ok=False, error=f"{type(e).__name__}: {e}", ts=t0)

        time.sleep(max(0.5, POLL - (time.time() - t0)))


# Activate the configured profile now that every helper it touches is defined. A
# broken URL in config.json must not stop the process booting -- the dashboard comes
# up and says so, which is far easier to fix than a service that will not start.
_initial = next((s for s in CFG["servers"] if s.get("id") == CFG.get("active")),
                CFG["servers"][0] if CFG["servers"] else {"id": "local", "url": ""})
try:
    set_active(_initial)
except ValueError as _e:
    print(f"config: active server '{_initial.get('id')}' has an unusable URL ({_e})",
          flush=True)
    with STATE_LOCK:
        STATE.update(ok=False, error=f"config: {_e}")


# ------------------------------------------------------------------ http
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, body, ctype, status=200):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0 or n > 65536:            # a profile list is never large
            return {}
        return json.loads(self.rfile.read(n).decode("utf-8", "replace")) or {}

    def do_GET(self):
        p = self.path.split("?")[0]
        if p == "/api/state":
            with STATE_LOCK:
                body = json.dumps(STATE)
            self._send(body, "application/json")
        elif p == "/api/config":
            self._json(config_view())
        elif p == "/api/discover":
            self._json(discover_containers())
        elif p == "/healthz":
            self._send("ok\n", "text/plain")
        elif p in ("/", "/index.html"):
            # 500 when the UI file is absent: a monitor polling / should not read a
            # placeholder as a healthy dashboard.
            self._send(PAGE, "text/html; charset=utf-8",
                       200 if PAGE_OK else 500)
        else:
            self.send_error(404)

    def do_POST(self):
        p = self.path.split("?")[0]
        try:
            body = self._body()
        except Exception:
            return self._json({"error": "malformed JSON body"}, 400)
        try:
            if p == "/api/test":
                return self._json(self._test(body))
            if p == "/api/servers":
                return self._json(self._add(body))
            if p == "/api/active":
                return self._json(self._activate(body))
            if p.startswith("/api/servers/"):
                rest = p[len("/api/servers/"):]
                if rest.endswith("/delete"):
                    return self._json(self._delete(rest[:-len("/delete")]))
                if rest.endswith("/test"):
                    return self._json(self._test_saved(rest[:-len("/test")]))
                return self._json(self._edit(rest, body))
        except ValueError as e:                 # user-fixable: bad URL, gone container
            return self._json({"error": str(e)}, 400)
        except Exception as e:
            return self._json({"error": f"{type(e).__name__}: {e}"}, 500)
        self.send_error(404)

    # -- the container name never comes from the client: only an id it was offered
    @staticmethod
    def _container_for(body):
        return resolve_discovered(body.get("container_id") or "")

    def _test(self, body):
        return probe_server(body.get("url") or "", self._container_for(body))

    def _test_saved(self, sid):
        """Probe a stored profile. Its container name came from config, not from the
        client, so it is used as-is."""
        with CFG_LOCK:
            srv = find_server(CFG, sid)
            if not srv:
                return {"error": "no such profile"}
            url, container = srv.get("url") or "", srv.get("container") or ""
        out = probe_server(url, container)
        out["id"] = sid
        return out

    def _add(self, body):
        name = (body.get("name") or "").strip()[:60]
        container = self._container_for(body)
        probe = probe_server(body.get("url") or "", container)
        # An unreachable server is never written. Half the value of the popup is
        # refusing to save something that will only fail silently later.
        if not probe.get("ok"):
            return {"error": probe.get("error") or "server is not reachable", "probe": probe}
        with CFG_LOCK:
            if any(s.get("url") == probe["url"] for s in CFG["servers"]):
                return {"error": "a profile with that URL already exists"}
            sid = new_id(CFG, name or urllib.parse.urlsplit(probe["url"]).hostname)
            CFG["servers"].append({"id": sid, "name": name or sid, "url": probe["url"],
                                   "container": container, "cache_dir": None,
                                   "client_names": {}})
            save_config(CFG)
        return {"ok": True, "id": sid, "probe": probe, "config": config_view()}

    def _edit(self, sid, body):
        with CFG_LOCK:
            srv = find_server(CFG, sid)
            if not srv:
                return {"error": "no such profile"}
            hard = False                    # a hard change invalidates collected history
            if "name" in body:
                srv["name"] = (body.get("name") or "").strip()[:60] or srv["id"]
            if "client_names" in body:
                cn = body.get("client_names") or {}
                srv["client_names"] = {str(k)[:45]: str(v)[:60] for k, v in cn.items()}
            if body.get("url"):
                probe = probe_server(body["url"], srv.get("container") or "")
                if not probe.get("ok"):
                    return {"error": probe.get("error") or "server is not reachable",
                            "probe": probe}
                hard = hard or srv.get("url") != probe["url"]
                srv["url"] = probe["url"]
            if "container_id" in body:
                c = self._container_for(body)
                hard = hard or (srv.get("container") or "") != c
                srv["container"] = c
            save_config(CFG)
            active = ACTIVE["id"] == sid
        if active:
            if hard:
                set_active(srv)
            else:                            # a rename must not throw away sparklines
                with CFG_LOCK:
                    ACTIVE["name"] = srv.get("name") or srv["id"]
                    ACTIVE["client_names"] = dict(srv.get("client_names") or {})
        return {"ok": True, "config": config_view()}

    def _delete(self, sid):
        with CFG_LOCK:
            srv = find_server(CFG, sid)
            if not srv:
                return {"error": "no such profile"}
            if len(CFG["servers"]) == 1:
                return {"error": "that is the only profile - add another first"}
            CFG["servers"].remove(srv)
            fallback = CFG["servers"][0] if ACTIVE["id"] == sid else None
            if fallback:
                CFG["active"] = fallback["id"]
            save_config(CFG)
        if fallback:
            set_active(fallback)
        return {"ok": True, "config": config_view()}

    def _activate(self, body):
        with CFG_LOCK:
            srv = find_server(CFG, body.get("id"))
            if not srv:
                return {"error": "no such profile"}
            CFG["active"] = srv["id"]
            save_config(CFG)
        set_active(srv)
        return {"ok": True, "config": config_view()}


# ------------------------------------------------------------------- ui
# page.html sits next to this module and IS the interface. It is read once at import,
# so the UI can be edited and redeployed without touching this file.
#
# A full copy of the UI used to live here as a fallback. It drifted silently: by the
# time the settings popup landed, the embedded copy was serving a dashboard several
# features behind with no way to tell from the browser. A stale UI presented as the
# real one is exactly the sort of unsupported claim this project refuses to make
# elsewhere, so a missing page.html now says so instead of quietly substituting.
_PF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "page.html")


def load_page():
    """-> (html, ok). ok is False when the placeholder is being served."""
    try:
        with open(_PF, encoding="utf-8") as fh:
            return fh.read(), True
    except Exception as exc:
        print(f"page.html unreadable ({exc}); serving the placeholder", flush=True)
        return f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>stremio-dash - UI missing</title>
<style>body{{margin:0;background:#0e1116;color:#e6edf3;padding:40px 22px;
font:15px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}}
main{{max-width:640px;margin:0 auto}}
code{{background:#1c222b;border:1px solid #262d38;border-radius:5px;padding:1px 5px;
font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px}}
.e{{background:rgba(248,81,73,.1);border:1px solid rgba(248,81,73,.3);color:#ff9a94;
padding:11px 14px;border-radius:8px;margin:0 0 18px}}
.m{{color:#8b98a8;font-size:13px}}</style>
<main>
<div class="e"><b>page.html is missing.</b> The dashboard UI is a separate file and
this process could not read it.</div>
<p>It should sit next to <code>stremio_dash.py</code>:<br><code>{esc_html(_PF)}</code></p>
<p class="m">Copy it from the repo, or re-run <code>deploy/install.sh</code>, which ships
both files. The collector itself is unaffected and still running &mdash; the JSON API is
live at <code>/api/state</code>, and <code>/healthz</code> still answers.</p>
<p class="m">There is deliberately no built-in copy of the UI: an embedded fallback drifted
out of date and served an old dashboard without saying so.</p>
</main>
""", False


PAGE, PAGE_OK = load_page()


if __name__ == "__main__":
    threading.Thread(target=collect, daemon=True).start()
    threading.Thread(target=log_tailer, daemon=True).start()
    threading.Thread(target=cache_worker, daemon=True).start()
    srv = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    srv.daemon_threads = True
    print(f"stremio-dash on http://{LISTEN_HOST}:{LISTEN_PORT}  -> "
          f"{ACTIVE['url'] or '(no server configured)'}"
          f"{' via ' + ACTIVE['container'] if ACTIVE['container'] else ' (HTTP only)'}",
          flush=True)
    srv.serve_forever()
