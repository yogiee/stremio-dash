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
  4. docker logs -f             - range requests -> which file plays, playhead, seeks
  5. ss -tina in the netns      - client sockets (real LAN IPs) + peer dial funnel

Only 1-3 work against a Stremio server on another host; 4 needs Docker access and
5 needs to reach that container's network namespace -- by nsenter from the host, or,
when this runs in a container itself, through the probe sidecar. See docs/modes.md.
"""

import http.client
import json
import os
import calendar
import re
import shutil
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
        # Unbound until configured, or until bootstrap() finds the one Stremio
        # container on this Docker host. Guessing a name here meant every fresh
        # install without that exact container reported Docker errors.
        "container": "",
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
FILE_LEN = {}                                            # (infohash, idx) -> bytes
FILES_SEEN = threading.Event()     # the collector has read /stats.json at least once
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
BITRATE_TRIED = {}    # (infohash, idx) -> (last attempt ts, delay before the next)
# Every ffprobe opens the stream, and an open stream keeps Stremio's engine alive for
# another 120s. Retrying at a fixed 120s could therefore hold a stalled engine open
# forever, so a failing probe backs off.
PROBE_RETRY_MIN, PROBE_RETRY_MAX = 120, 1800
# Our ffprobe lands in the server log exactly like a player's range request. Tagging
# its URL lets the log tailer skip it, including in the backlog replayed after a
# restart; otherwise a probe would move the playhead or switch the file being shown.
PROBE_TAG = "stremio-dash=probe"

IH_RE = re.compile(r"^[0-9a-f]{40}$")
# Engines this dashboard stopped -> when. Any GET /{ih}/{idx} re-creates an engine, so
# our own ffprobe must leave a stopped title alone; and if it comes back anyway, some
# client asked for it again, which the card says rather than hiding.
STOPPED = {}
STOP_QUIET = 60        # s: no probing a title we just stopped
STOP_NOTE = 300        # s: how long a re-opened title is flagged as such
# infohash -> title. A cache directory holds no name, so titles are remembered as
# engines are seen, and persisted beside the config (pruned to what is still cached).
NAMES = {}
_NAMES = {"dirty": False, "saved": 0.0}
CACHE_KICK = threading.Event()     # re-measure the cache now


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
                fresh = _CONN["c"] is None
                if fresh:
                    _CONN["c"] = _connect(netloc, scheme, timeout)
                c = _CONN["c"]
                c.request("GET", path, headers={"Connection": "keep-alive"})
                if fresh:
                    note_own_address(c.sock)
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


def note_own_address(sock):
    """The address this dashboard polls FROM is not a viewer. On the host that is
    127.0.0.1, but a containerised dashboard on the Stremio container's network polls
    from its own address, which would otherwise be listed as a client."""
    try:
        ip = sock.getsockname()[0]
    except Exception:
        return
    IGNORE_CLIENT_IPS.add(ip[7:] if ip.startswith("::ffff:") else ip)


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
_pid_cache = {"pid": None, "id": "", "why": "", "at": 0}
_SOCK = {"key": None, "at": 0, "argv": None, "via": "", "why": ""}
_TAILER = {"proc": None}
_SELF = {}


def first_line(text, default=""):
    lines = (text or "").strip().splitlines()
    return lines[0][:160] if lines else default


def self_container_id():
    """This process's own container id, or "" on a plain host.

    Docker bind-mounts /etc/hostname from .../containers/<id>/hostname, which holds
    even when the hostname was overridden. Only that mount point counts: a HOST's
    mountinfo also lists .../containers/<id>/mounts/shm for every running container,
    and matching those would make the host believe it was a container."""
    if "id" not in _SELF:
        _SELF["id"] = ""
        try:
            with open("/proc/self/mountinfo") as fh:
                for line in fh:
                    f = line.split()
                    if len(f) > 4 and f[4] == "/etc/hostname":
                        m = re.search(r"/containers/([0-9a-f]{64})/", f[3])
                        if m:
                            _SELF["id"] = m.group(1)
                            break
        except OSError:
            pass
    return _SELF["id"]


# Inside a container, 127.0.0.1 is this dashboard, host PIDs are not visible, and
# nsenter cannot work -- the probe sidecar is the way into Stremio's namespace.
IN_CONTAINER = os.path.exists("/.dockerenv") or bool(self_container_id())


def inspect_container(container):
    """-> (host pid, full id, bridge gateways, why-not). pid is None when there is
    nothing to reach, and `why` says whether that is the container or Docker access."""
    fmt = "{{.State.Pid}} {{.Id}}{{range .NetworkSettings.Networks}} {{.Gateway}}{{end}}"
    try:
        r = subprocess.run(["docker", "inspect", "-f", fmt, container],
                           capture_output=True, text=True, timeout=8)
    except FileNotFoundError:
        return None, "", [], "no docker CLI on the dashboard host"
    except Exception as e:
        return None, "", [], f"docker inspect failed ({type(e).__name__})"
    if r.returncode != 0:
        err = first_line(r.stderr, "docker inspect failed")
        if "No such" in err:
            return None, "", [], f"container '{container}' not found"
        return None, "", [], f"no Docker access: {err}"
    parts = r.stdout.split()
    if not parts or not parts[0].isdigit() or int(parts[0]) <= 0:
        return None, "", [], f"container '{container}' is not running"
    return (int(parts[0]), parts[1] if len(parts) > 1 else "",
            [g for g in parts[2:] if g != "<no value>"], "")


def container_pid():
    """Resolve the ACTIVE Stremio container -> (host pid, full id, why-not), and learn
    its bridge gateway addresses. Traffic from a gateway is this dashboard polling the
    API, not a viewer -- and the bridge subnet differs per host, so it must be
    discovered rather than assumed."""
    with CFG_LOCK:
        container = ACTIVE["container"]
    if not container:
        return None, "", "no container bound - HTTP-only profile"
    now = time.time()
    if now - _pid_cache["at"] < (30 if _pid_cache["pid"] else 5):
        return _pid_cache["pid"], _pid_cache["id"], _pid_cache["why"]
    pid, cid, gateways, why = inspect_container(container)
    IGNORE_CLIENT_IPS.update(g for g in gateways if g)
    _pid_cache.update(pid=pid, id=cid, why=why, at=now)
    return pid, cid, why


def netns_inode(container):
    """net:[4026532...] -- the identity of a container's network namespace."""
    try:
        r = subprocess.run(["docker", "exec", container, "readlink", "/proc/self/ns/net"],
                           capture_output=True, text=True, timeout=8)
        return r.stdout.strip() if r.returncode == 0 else ""
    except Exception:
        return ""


def find_probe(container, cid):
    """The probe sidecar: a running container sharing the Stremio container's network
    namespace (this image with `command: probe`). -> (name, why-not)

    NetworkMode alone is not trusted. A sidecar joined to a container that was later
    recreated -- watchtower does exactly that -- keeps the OLD namespace alive, and its
    empty socket table would read as "nobody watching". So a match is confirmed by
    comparing namespace inodes."""
    try:
        ids = subprocess.run(["docker", "ps", "-q", "--no-trunc"],
                             capture_output=True, text=True, timeout=8).stdout.split()
        fmt = "{{.Id}}\t{{.Name}}\t{{.HostConfig.NetworkMode}}\t{{json .Config.Cmd}}"
        r = subprocess.run(["docker", "inspect", "-f", fmt] + ids,
                           capture_output=True, text=True, timeout=8) if ids else None
    except Exception as e:
        return None, f"cannot list containers ({type(e).__name__})"
    rows = [f for f in (l.split("\t") for l in (r.stdout if r else "").splitlines())
            if len(f) == 4]
    running = {f[0] for f in rows} | {f[1].lstrip("/") for f in rows}
    want, stale = None, []
    for _id, name, netmode, cmd in rows:
        if not netmode.startswith("container:"):
            continue
        name, target = name.lstrip("/"), netmode.split(":", 1)[1]
        is_probe = cmd.strip() == '["probe"]'
        if target not in (cid, container) and not is_probe:
            continue
        if want is None:
            want = netns_inode(container)
        if want:
            if netns_inode(name) == want:
                return name, ""
            if is_probe and target not in running:
                stale.append(name)
        elif target in (cid, container):     # cannot verify: trust the binding
            return name, ""
    if stale:
        return None, (f"probe sidecar '{stale[0]}' is joined to a {container} container "
                      "that no longer exists - recreate it")
    return None, "no network-namespace access - add the probe sidecar (see README)"


def ss_works(argv):
    """-> (ok, why-not)"""
    try:
        r = subprocess.run(argv + ["-tn", "state", "listening"],
                           capture_output=True, text=True, timeout=8)
    except FileNotFoundError:
        return False, f"{argv[0]} not found"
    except Exception as e:
        return False, type(e).__name__
    return r.returncode == 0, first_line(r.stderr, f"exit {r.returncode}")


def netns_access(container, pid, cid):
    """How to run `ss` inside a Stremio container's network namespace, best first:
    nsenter from the host, else `docker exec` into the probe sidecar.
    -> (argv prefix, via, why-not)"""
    why = ""
    if not IN_CONTAINER and os.path.exists(f"/proc/{pid}/ns/net"):
        argv = ["nsenter", "-t", str(pid), "-n", "ss"]
        ok, err = ss_works(argv)
        if ok:
            return argv, "nsenter", ""
        why = f"nsenter: {err}"
    name, pwhy = find_probe(container, cid)
    if not name:
        return None, "", f"{why}; {pwhy}" if why else pwhy
    argv = ["docker", "exec", name, "ss"]
    ok, err = ss_works(argv)
    if ok:
        return argv, f"probe {name}", ""
    return None, "", f"probe sidecar '{name}' cannot run ss ({err})"


def socket_source():
    """-> (argv, via, level, why) for the ACTIVE profile. `level` is the mode achieved
    when sockets are unavailable: A if the container itself is unreachable, else B.

    A working route is re-checked every 5 min, a missing one every 30s so a sidecar
    started later is picked up. A failed `ss` run invalidates it at once."""
    pid, cid, why = container_pid()
    if not pid:
        return None, "", "A", why
    now = time.time()
    key = (GEN, pid)
    if _SOCK["key"] == key and now - _SOCK["at"] < (300 if _SOCK["argv"] else 30):
        return _SOCK["argv"], _SOCK["via"], "B", _SOCK["why"]
    with CFG_LOCK:
        container = ACTIVE["container"]
    argv, via, why = netns_access(container, pid, cid)
    _SOCK.update(key=key, at=now, argv=argv, via=via, why=why)
    return argv, via, "B", why


SS_KV = re.compile(r"(bytes_acked|bytes_sent|bytes_retrans|delivery_rate|rtt|retrans|"
                   r"rwnd_limited|backoff|lastsnd|lastrcv):(\S+)")


def read_sockets():
    """Client sockets (with delivered-byte rates) + BT peer dial funnel."""
    argv, via, level, why = socket_source()
    if not argv:
        return [], {"error": why, "level": level}
    try:
        r = subprocess.run(argv + ["-tina"], capture_output=True, text=True, timeout=8)
        if r.returncode != 0:
            raise RuntimeError(first_line(r.stderr, f"{via}: ss failed"))
        out = r.stdout
    except Exception as e:
        # Container was replaced (watchtower recreates it on a new pid) or died, or
        # the sidecar went away with it. Re-resolve both on the next tick.
        _pid_cache.update(at=0)
        _SOCK.update(at=0)
        SOCK_HIST.clear()
        return [], {"error": str(e), "level": "B"}

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
                     "by_state": dict(funnel), "via": via,
                     "hit_rate": (est / (est + dialing)) if (est + dialing) else 0.0}


def by_device(socks):
    """One row per client address, in the order ss lists them.

    A player does not keep to one connection: one stalled on a file that is still
    downloading re-requests every ~15s on a fresh socket, and the server holds the old
    ones open, so a single device can own several. Clients behind one proxy share an
    address and merge too -- they were indistinguishable already."""
    rows = {}
    for s in socks:
        d = rows.get(s["ip"])
        if d is None:
            rows[s["ip"]] = dict(s, conns=1)
            continue
        d["conns"] += 1
        for k in ("bytes_acked", "delivered_bps", "sendq"):
            d[k] = d.get(k, 0) + s.get(k, 0)
        d["retrans"] = str(int(d.get("retrans") or 0) + int(s.get("retrans") or 0))
        d["active"] = d.get("active", False) or s.get("active", False)
        if s["state"] == "ESTAB":
            d["state"] = "ESTAB"
        if s.get("idle_for", 1e9) < d.get("idle_for", 1e9):
            # RTT and ports belong to the connection that most recently moved bytes.
            d.update({k: s[k] for k in ("idle_for", "rtt", "port", "via") if k in s})
    return list(rows.values())


# ------------------------------------------------------------- log tail
# The server logs range requests in two shapes depending on whether the client
# sent a query string:  "-> GET /<ih>/<idx> bytes=N-"  and  "... /<idx>? bytes=N-".
# The Stremio apps use the second; a bare curl produces the first. Match both.
LOG_RE = re.compile(r"-> GET /([0-9a-f]{40})/(\d+)(\?\S*)?\s+bytes=(\d+)-(\d*)")
# A player opens a file with a burst: the header, the index at the END of the file
# (MKV cues, an MP4's moov) -- often as a pair of requests -- and then ONE open-ended
# request from where it resumes, held as a single long connection. So a bounded range
# or a read of the file's tail is never the position, and the last request that is
# neither IS the position, however far it jumped. The old rule ("believe a far jump
# only after a second request nearby") got both wrong: it accepted the paired tail
# reads and threw away the lone resume request, parking the playhead at 99.9%.
TAIL_ZONE_MIN = 16 * 1024 * 1024
SEEK_GAP = 64 * 1024 * 1024


def is_index_read(ih, idx, start, end):
    n = FILE_LEN.get((ih, idx))
    return bool(end) or (bool(n) and start >= n - max(TAIL_ZONE_MIN, n // 200))


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


def new_playhead(idx, start, now):
    return {"idx": idx, "offset": start, "at": now, "seeks": 0, "cand_idx": None}


def log_tailer():
    """Follow the container log for range requests -> which file is playing, and the
    playhead byte offset within it."""
    while True:
        with CFG_LOCK:
            container, gen = ACTIVE["container"], GEN
        if not container:
            time.sleep(3)                # HTTP-only profile: no log to follow
            continue
        p = None
        # Telling an index read from playback needs file lengths, which come from the
        # collector's /stats.json. Wait briefly for them, or the replayed backlog below
        # would be read without them.
        FILES_SEEN.wait(10)
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
                if not m or PROBE_TAG in (m.group(3) or ""):
                    continue
                ih, idx, start = m.group(1), int(m.group(2)), int(m.group(4))
                index = is_index_read(ih, idx, start, m.group(5))
                now = log_time(line, time.time())
                cur = PLAYHEAD.get(ih)
                if cur and idx != cur["idx"]:
                    # Another file of the same torrent: a season pack moving on to the
                    # next episode. Offsets in two files are not comparable, so this is
                    # a new position, not a seek. A player opening a file sends several
                    # requests at once; a lone one (a next-episode preload, say) must
                    # not move the card to a file nobody is watching, because a player
                    # holding one long connection may never re-request the real one.
                    if cur.get("cand_idx") != idx:
                        cur["cand_idx"] = idx
                        continue
                    cur = None
                if not cur:
                    PLAYHEAD[ih] = new_playhead(idx, 0 if index else start, now)
                    continue
                cur["cand_idx"] = None
                if index:
                    continue
                if abs(start - cur["offset"]) > SEEK_GAP:
                    cur["seeks"] += 1
                cur.update(offset=start, at=now)
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


DOCKER_ERR = re.compile(r"Error response from daemon|Cannot connect to the Docker daemon|"
                        r"permission denied while trying to connect|No such container")


def probe_worker(q):
    """One ffprobe per stream, in the background, to get the REQUIRED bitrate.

    The server's own /probe endpoint 500s on this build, but ffprobe against the
    stream URL returns in ~0.15s even when the file is only 2.7% downloaded (the
    header is at the front and already cached). This is the honest "how fast does
    playback need to be fed" number; deriving it from the playhead was unreliable
    because players issue non-playback range requests.

    Runs inside the container when one is bound -- the stream is always reachable at
    the container-local port, whatever it is published as. Otherwise, or when that
    exec itself fails (no Docker access, no ffprobe in that image), it runs ffprobe on
    THIS host against the server URL. That fallback is what makes the verdict work in
    mode A; without it a remote profile has no required-bitrate figure at all. The
    container image bundles ffprobe for exactly this."""
    ARGS = ["-v", "quiet", "-print_format", "json",
            "-show_entries", "format=duration,bit_rate,size"]
    while True:
        ih, idx = q.get()
        key = (ih, idx)
        if time.time() - STOPPED.get(ih, 0) < STOP_QUIET:
            continue            # queued before the stop: probing would re-create it
        with CFG_LOCK:
            container, url = ACTIVE["container"], ACTIVE["url"]
        attempts = [(["ffprobe"] + ARGS + [f"{url}/{ih}/{idx}?{PROBE_TAG}"], "here")]
        if container:
            attempts.insert(0, (["docker", "exec", container, "ffprobe"] + ARGS +
                                [f"http://127.0.0.1:11470/{ih}/{idx}?{PROBE_TAG}"],
                                "container"))
        br, err = 0.0, ""
        for cmd, where in attempts:
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            except FileNotFoundError:
                # Degrade explicitly: say the tool is missing rather than silently
                # showing no verdict forever.
                err = ("no docker CLI" if where == "container"
                       else "ffprobe not installed on the dashboard host")
                continue
            except Exception as e:
                err = f"{type(e).__name__}: {e}"[:120]
                break
            # Docker could not run the exec at all. That says nothing about the
            # stream, so try from here. Any other result is ffprobe's own answer
            # (it runs with -v quiet, so its stderr is empty), and asking again
            # elsewhere would only open the stream a second time.
            if where == "container" and r.returncode != 0 and (
                    r.returncode in (125, 126, 127) or DOCKER_ERR.search(r.stderr or "")):
                err = first_line(r.stderr, "docker exec failed")[:120]
                continue
            try:
                fmt = json.loads(r.stdout or "{}").get("format", {})
            except ValueError:
                fmt = {}
            if fmt.get("bit_rate"):
                br = float(fmt["bit_rate"]) / 8.0
            elif fmt.get("size") and fmt.get("duration"):
                br = float(fmt["size"]) / float(fmt["duration"])
            if not br:
                err = (r.stderr or "ffprobe returned no bitrate").strip()[:120]
            break
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


def container_cache_path():
    root = ((STATE.get("settings") or {}).get("cacheRoot") or "/root/.stremio-server")
    return root.rstrip("/") + "/stremio-cache"


def cache_stats_container(container):
    """Ask the container to measure its own cache -- no host path, works wherever the
    Docker API reaches. Note busybox du rejects -B1, but -sk is fine."""
    path = container_cache_path()
    # The path comes from the server's /settings, so it is passed as $1 rather than
    # spliced into the script.
    script = ('D="$1"; [ -d "$D" ] || exit 3; '
              'du -sk "$D" | cut -f1; '
              'ls -1 "$D" 2>/dev/null | wc -l; '
              'n=0; for x in "$D"/*/; do [ -n "$(ls -A "$x" 2>/dev/null)" ] && n=$((n+1)); done; '
              'echo $n')
    out = subprocess.run(["docker", "exec", container, "sh", "-c", script, "sh", path],
                         capture_output=True, text=True, timeout=120)
    if out.returncode == 3:
        # Stremio creates the directory with its first engine, so on a fresh server
        # its absence means "nothing cached yet", not a fault.
        return {"bytes": 0, "entries": 0, "stubs": 0, "dir": path, "via": "container"}
    if out.returncode != 0:
        raise RuntimeError(first_line(out.stderr, f"cannot measure {path}"))
    kb, total, held = (int(v) for v in out.stdout.split()[:3])
    return {"bytes": kb * 1024, "entries": held, "stubs": total - held,
            "dir": path, "via": "container"}


def cache_worker():
    while True:
        with CFG_LOCK:
            cache_dir, container, gen = ACTIVE["cache_dir"], ACTIVE["container"], GEN
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
        except Exception as e:
            stats = {"error": str(e), "dir": cache_dir or "(container)"}
        with STATE_LOCK:
            if GEN == gen:          # a switch mid-measurement: this is the old server
                STATE["cache"] = stats
        # Once a minute, but at once after a server switch (reset_derived() blanks
        # the tile) or a deletion (the figure is now wrong).
        for _ in range(30):
            if CACHE_KICK.wait(2):
                CACHE_KICK.clear()
                break
            if GEN != gen:
                break


# ------------------------------------------------------------ control actions
# The dashboard was read-only until these two. They are the only writes it makes to a
# Stremio server, and /settings stays untouched.
#
#   stop   GET /{ih}/remove -- Stremio destroys the engine and its swarm at once,
#          instead of 120s after the last client lets go (or never, while a player
#          leaks the connection). It does NOT delete the cached data.
#   cache  delete stremio-cache/<ih>/ for titles that are not loaded. Stremio has no
#          endpoint for this; its own eviction only runs when an engine is created.
#
# Deleting under a loaded engine would pull the file out from under a live stream, so
# every deletion is re-checked against a fresh /stats.json, not the last tick.

def names_path():
    base = CFG.get("_path") or next((p for p in CONFIG_PATHS if p), "")
    return os.path.join(os.path.dirname(os.path.abspath(base)), "names.json") if base else ""


def load_names():
    try:
        with open(names_path()) as fh:
            NAMES.update({k: str(v)[:200] for k, v in json.load(fh).items() if IH_RE.match(k)})
    except Exception:
        pass


def remember_name(ih, name):
    if name and name != ih and NAMES.get(ih) != name:
        NAMES[ih] = name[:200]
        _NAMES["dirty"] = True


def save_names(force=False):
    """Atomic, at most every 30s. A failure only costs names after a restart."""
    now = time.time()
    if not _NAMES["dirty"] or (not force and now - _NAMES["saved"] < 30):
        return
    path = names_path()
    if not path:
        return
    _NAMES.update(dirty=False, saved=now)
    try:
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".names.", suffix=".tmp")
        with os.fdopen(fd, "w") as fh:
            json.dump(dict(NAMES), fh, indent=1)
        os.replace(tmp, path)
    except Exception as e:
        print(f"names: not saved ({e})", flush=True)


def forget_engine(ih):
    for d in (HIST, PLAYHEAD, NEEDS):
        d.pop(ih, None)
    for d in (BITRATE, BITRATE_ERR, BITRATE_TRIED, FILE_LEN):
        for k in [k for k in d if k[0] == ih]:
            d.pop(k, None)
    with STATE_LOCK:
        STATE["engines"] = {k: v for k, v in STATE["engines"].items() if k != ih}


def cache_reach():
    """-> (where, how) or raise ValueError when this profile cannot touch the cache."""
    with CFG_LOCK:
        cache_dir, container = ACTIVE["cache_dir"], ACTIVE["container"]
    if cache_dir:
        return cache_dir, "host"
    if container:
        pid, _cid, why = container_pid()
        if not pid:
            raise ValueError(f"cache unreachable: {why}")
        return container, "container"
    raise ValueError("the cache is not reachable in mode A - no container bound")


# One line per entry: name, KiB used, file count, newest mtime. Busybox-compatible.
LIST_SCRIPT = (
    'D="$1"; [ -d "$D" ] || exit 3; cd "$D" || exit 4; '
    'for x in */; do x="${x%/}"; [ -d "$x" ] || continue; '
    'n=$(ls -A "$x" | wc -l); '
    't=$( (stat -c %Y "$x" "$x"/* 2>/dev/null || true) | sort -n | tail -1); '
    'printf "%s\t%s\t%s\t%s\n" "$x" "$(du -sk "$x" | cut -f1)" "$n" "${t:-0}"; done')


def list_cache():
    """Every entry in the cache directory, with size, file count, last write, the title
    if one was ever seen, and whether an engine currently holds it."""
    where, how = cache_reach()
    rows = []
    if how == "host":
        if not os.path.isdir(where):
            raise ValueError(f"cache directory {where} does not exist")
        for d in os.scandir(where):
            if not d.is_dir(follow_symlinks=False):
                continue
            used, n, newest = 0, 0, d.stat().st_mtime
            for f in os.scandir(d.path):
                st = f.stat(follow_symlinks=False)
                used += st.st_blocks * 512        # real usage: files are sparse
                n += 1
                newest = max(newest, st.st_mtime)
            rows.append((d.name, used, n, newest))
        path = where
    else:
        path = container_cache_path()
        r = subprocess.run(["docker", "exec", where, "sh", "-c", LIST_SCRIPT, "sh", path],
                           capture_output=True, text=True, timeout=120)
        if r.returncode == 3:
            return {"dir": path, "via": how, "entries": []}
        if r.returncode != 0:
            raise RuntimeError(first_line(r.stderr, f"cannot list {path}"))
        for line in r.stdout.splitlines():
            f = line.split("\t")
            if len(f) == 4 and f[1].isdigit() and f[2].isdigit():
                rows.append((f[0], int(f[1]) * 1024, int(f[2]),
                             int(f[3]) if f[3].isdigit() else 0))
    with STATE_LOCK:
        loaded = {ih: e.get("name") for ih, e in STATE["engines"].items()}
    entries = [{"infohash": name, "bytes": used, "files": n, "mtime": newest,
                "name": loaded.get(name) or NAMES.get(name, ""),
                "loaded": name in loaded, "deletable": bool(IH_RE.match(name))}
               for name, used, n, newest in rows]
    entries.sort(key=lambda e: (-e["bytes"], -e["mtime"]))
    # Keep remembered names bounded to what is still on disk or playing.
    keep = {e["infohash"] for e in entries} | set(loaded)
    for ih in [k for k in NAMES if k not in keep]:
        NAMES.pop(ih, None)
        _NAMES["dirty"] = True
    save_names(force=True)
    return {"dir": path, "via": how, "entries": entries}


def delete_cache(infohashes=(), all_inactive=False):
    where, how = cache_reach()
    if all_inactive:
        infohashes = [e["infohash"] for e in list_cache()["entries"]
                      if e["deletable"] and not e["loaded"]]
    live = set(http_json("/stats.json").keys())       # fresh, not the last tick
    todo, skipped = [], []
    for ih in dict.fromkeys(infohashes):
        if not isinstance(ih, str) or not IH_RE.match(ih):
            skipped.append({"infohash": str(ih)[:60], "why": "not an infohash"})
        elif ih in live:
            skipped.append({"infohash": ih, "why": "loaded - stop it first"})
        else:
            todo.append(ih)
    if todo:
        if how == "host":
            for ih in todo:
                shutil.rmtree(os.path.join(where, ih), ignore_errors=True)
        else:
            # Names are validated hex, and argv is not a shell: nothing to inject.
            base = container_cache_path()
            r = subprocess.run(["docker", "exec", where, "rm", "-rf", "--"] +
                               [f"{base}/{ih}" for ih in todo],
                               capture_output=True, text=True, timeout=120)
            if r.returncode != 0:
                raise RuntimeError(first_line(r.stderr, "rm failed"))
    CACHE_KICK.set()
    return {"ok": True, "deleted": todo, "skipped": skipped}


def stop_engine(ih, delete=False):
    if not isinstance(ih, str) or not IH_RE.match(ih):
        raise ValueError("not an infohash")
    if ih not in http_json("/stats.json"):
        raise ValueError("that stream is not loaded any more")
    # "In progress" until Stremio confirms, then the time it did. A tick whose stats were
    # fetched before that moment still lists the engine, and must neither show it nor
    # probe it (a probe would re-create it).
    STOPPED[ih] = float("inf")
    try:
        http_json(f"/{ih}/remove", timeout=10)    # answers once the engine is destroyed
    except Exception:
        STOPPED.pop(ih, None)
        raise
    STOPPED[ih] = time.time()
    forget_engine(ih)
    out = {"ok": True, "stopped": ih}
    if delete:
        try:
            out["cache"] = delete_cache([ih])
        except (ValueError, RuntimeError) as e:
            out["cache"] = {"error": str(e)}
    print(f"stop: {NAMES.get(ih) or ih}{' + cache' if delete else ''}", flush=True)
    return out


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
            ["docker", "ps", "--no-trunc", "--format",
             "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Command}}"],
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
        if len(f) < 5:
            continue
        cid, name, image, ports, command = f[0], f[1], f[2], f[3], f[4]
        if "stremio" not in f"{name} {image}".lower():
            continue
        # This dashboard and its probe sidecar are "stremio-dash" too. The entrypoint
        # identifies this image however it was tagged; the image name catches a run
        # with the entrypoint overridden.
        if command.strip('"').startswith("stremio-dash") or "stremio-dash" in image:
            continue
        hp = published_port(ports, 11470)
        out.append({"id": cid, "name": name, "image": image, "port": hp,
                    "url": f"http://127.0.0.1:{hp}" if hp else "",
                    "ports": ports})
    if IN_CONTAINER and out:
        route_from_container(out)
    return {"docker": True, "containers": out, "error": None}


def route_from_container(found):
    """Inside a container, 127.0.0.1 is this dashboard, not the host. Reach a Stremio
    container over a network shared with it (compose puts a project's services on
    one) on its own port 11470; failing that, via this container's gateway -- the
    host -- on the published port. Rewrites each entry's url in place."""
    me = self_container_id()
    try:
        r = subprocess.run(["docker", "inspect"] + ([me] if me else []) +
                           [c["id"] for c in found],
                           capture_output=True, text=True, timeout=8)
        data = json.loads(r.stdout or "[]")
    except Exception:
        data = []
    nets_of = {d.get("Id", ""): (d.get("NetworkSettings") or {}).get("Networks") or {}
               for d in data}
    mine = nets_of.get(me, {})
    if "host" in mine:
        return                  # host networking: 127.0.0.1 really is the host
    gateway = next((n.get("Gateway") for n in mine.values() if n.get("Gateway")), "")
    for c in found:
        nets = next((v for k, v in nets_of.items() if k.startswith(c["id"])), {})
        shared = [n for n in nets if n in mine]
        if shared:
            # Names only resolve on user-defined networks, not the default bridge.
            host = c["name"] if shared[0] != "bridge" else nets[shared[0]].get("IPAddress")
            c["url"] = f"http://{host}:11470" if host else ""
        elif c["port"] and gateway:
            c["url"] = f"http://{gateway}:{c['port']}"
        else:
            c["url"] = ""


def resolve_discovered(container_id):
    """Map a container id the UI offered back to its real name, re-checking it still
    exists. This is the closed vocabulary: nothing else may set ACTIVE['container']."""
    if not container_id:
        return ""
    for c in discover_containers().get("containers") or []:
        if c["id"] == container_id or c["name"] == container_id:
            return c["name"]
    raise ValueError("that container is no longer running")


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
        # Deliberately not container_pid(): that caches for, and learns gateways of,
        # the ACTIVE profile, and this may be a different one.
        pid, cid, _gw, why = inspect_container(container)
        if not pid:
            info["notes"].append(f"{why} - HTTP only")
        else:
            info["mode"] = "B"
            argv, via, why = netns_access(container, pid, cid)
            if argv:
                info["mode"] = "C"
                info["notes"].append(f"sockets via {via}")
            else:
                info["notes"].append(f"{why} - no clients or dial funnel")
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
    for d in (HIST, PLAYHEAD, NEEDS, FILE_LEN, SOCK_HIST, BITRATE, BITRATE_ERR,
              BITRATE_TRIED, RDNS, RDNS_PENDING, STOPPED):
        d.clear()
    _pid_cache.update(pid=None, id="", why="", at=0)
    _SOCK.update(key=None, at=0, argv=None, via="", why="")
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


def playing_idx(ph, files):
    """-> (file index, known). The file a client last opened, as the log says.

    In a season pack the largest file is just the longest episode, so it is only a
    fallback for when no request has been seen -- and mode A has no log at all. Showing
    it as though it were playing once reported one episode "fully cached" while
    another was stalling."""
    if ph and 0 <= ph["idx"] < len(files):
        return ph["idx"], True
    return main_file_idx(files), len(files) <= 1


def collect():
    import queue
    rq = queue.Queue()
    pq = queue.Queue()
    threading.Thread(target=probe_worker, args=(pq,), daemon=True).start()
    det_cache = {}          # ih -> (fetched_at, file idx, detail json)
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
                for i, f in enumerate(files):
                    FILE_LEN[(ih, i)] = f.get("length") or 0
                # Read once: the tailer replaces this dict when playback moves to
                # another file, so it cannot disagree with the index taken from it.
                ph = PLAYHEAD.get(ih)
                idx, known = playing_idx(ph, files)
                if ph and ph["idx"] != idx:
                    ph = None                    # a position in a file we cannot show
                prev = det_cache.get(ih)
                # Detail is per FILE: once playback moves to another file, the last
                # one's progress must not be shown even for a tick.
                det = prev[2] if prev and prev[1] == idx else {}
                if idx is not None and (prev is None or prev[1] != idx
                                        or t0 - prev[0] >= DETAIL_SEC):
                    try:
                        det = http_json(f"/{ih}/{idx}/stats.json", timeout=4)
                        det_cache[ih] = (t0, idx, det)
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
                    n = NEEDS.get(ih)
                    if not n or n["idx"] != idx:     # offsets in another file
                        n = NEEDS[ih] = {"idx": idx, "prev_offset": ph["offset"],
                                         "prev_at": ph["at"], "need": 0.0}
                    dt = ph["at"] - n["prev_at"]
                    do = ph["offset"] - n["prev_offset"]
                    if dt >= 20:
                        n["need"] = (do / dt) if do > 0 else 0.0   # <=0 == seek back/stopped
                        n["prev_offset"], n["prev_at"] = ph["offset"], ph["at"]
                    need = n["need"]

                name = (det.get("streamName")
                        or (files[idx].get("name") if idx is not None else None)
                        or s.get("name") or ih)
                # A cache entry holds the whole torrent, so a season pack is filed
                # under the pack's name, not whichever episode happened to be playing.
                remember_name(ih, s.get("name") if len(files) > 1 else name)
                engines[ih] = {
                    "infohash": ih,
                    "name": name,
                    "torrent": s.get("name") or "",
                    "idx": idx,
                    "files": len(files),
                    "file_known": known,
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

            FILES_SEEN.set()

            # Fetched before a stop completed: stale for that engine, and showing it would
            # read as "re-opened".
            for ih in [k for k in engines if STOPPED.get(k, 0) > t0]:
                engines.pop(ih)

            socks, funnel = read_sockets()
            clients = by_device(socks)
            with CFG_LOCK:
                names = ACTIVE["client_names"]
            for c in clients:
                c["name"] = names.get(c["ip"], "")
            demand = sum(c["delivered_bps"] for c in socks if c.get("active"))
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
                e["cached"] = e["file_known"] and (e["progress"] or 0) >= 0.999
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
                acked = sum(c["bytes_acked"] for c in socks if c.get("active"))
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
                since_stop = t0 - STOPPED.get(ih, -1e9)
                e["reopened_after_stop"] = since_stop if since_stop < STOP_NOTE else None
                last, delay = BITRATE_TRIED.get(key, (0.0, PROBE_RETRY_MIN))
                # Opening a file makes Stremio download ALL of it, so only a file a
                # client has already opened is probed. Probing the largest file of a
                # season pack once pulled a whole episode that nobody watched.
                if (e["file_known"] and key not in BITRATE and since_stop >= STOP_QUIET
                        and t0 - last >= delay):
                    BITRATE_TRIED[key] = (t0, min(delay * 2, PROBE_RETRY_MAX) if last else delay)
                    pq.put(key)
            # Only a file that is completely on disk is certainly being served from it.
            # "No swarm ingest" is not enough: a client stalled on a file that is still
            # downloading sees none either, and used to be reported as streaming from
            # cache.
            read = [engines[target]] if target else list(engines.values())
            done = bool(read) and all(e["cached"] for e in read)
            for c in clients:
                c["from_cache"] = bool(c.get("active")) and done
            for ih in [k for k, t in STOPPED.items() if t0 - t >= STOP_NOTE]:
                STOPPED.pop(ih, None)
            save_names()

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
                sname = ACTIVE["name"]
            mode = funnel.get("level", "B") if funnel.get("error") else "C"
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
            for k in [k for k in FILE_LEN if k[0] not in engines]:
                FILE_LEN.pop(k, None)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            with CFG_LOCK:
                host = urllib.parse.urlsplit(ACTIVE["url"]).hostname or ""
            if IN_CONTAINER and host in ("127.0.0.1", "localhost", "::1"):
                err += (" - inside a container, 127.0.0.1 is this dashboard, not the "
                        "host. Set STREMIO_URL or pick the server from the gear menu.")
            with STATE_LOCK:
                STATE.update(ok=False, error=err, ts=t0)

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


def bootstrap():
    """First start with no config on disk -- the normal first run of the container.
    If exactly one Stremio container is visible and answers, make it the active
    profile instead of the built-in 127.0.0.1 default, which inside a container
    points at this dashboard. Anything ambiguous is left for the gear menu.

    Retries with backoff: compose's depends_on only waits for the Stremio container
    to START, and its server takes a few seconds more to answer. It stops as soon as
    a config exists, whether this wrote it or the gear menu did."""
    said, delay = set(), 3
    while not CFG.get("_path") and not any(ENV_PINNED.values()):
        found = [c for c in discover_containers().get("containers") or [] if c["url"]]
        if len(found) == 1:
            c = found[0]
            probe = probe_server(c["url"], c["name"])
            if probe.get("ok"):
                break
            msg = f"{c['name']} at {c['url']} is not answering yet ({probe.get('error')})"
        elif found:
            msg = f"{len(found)} Stremio containers found - choose one from the gear menu"
        else:
            msg = "no Stremio container visible - waiting for one, or use the gear menu"
        if msg not in said:
            said.add(msg)
            print(f"bootstrap: {msg}", flush=True)
        time.sleep(delay)
        delay = min(delay * 2, 60)
    else:
        return
    with CFG_LOCK:
        if CFG.get("_path"):          # the gear menu got there first
            return
        srv = find_server(CFG, CFG.get("active")) or CFG["servers"][0]
        srv.update(name=c["name"], url=probe["url"], container=c["name"])
        CFG["active"] = srv["id"]
        try:
            save_config(CFG)
        except Exception as e:
            print(f"bootstrap: config not saved ({e}); using it for this run only",
                  flush=True)
    set_active(srv)
    print(f"bootstrap: using {c['name']} at {probe['url']} (mode {probe['mode']})",
          flush=True)


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
        elif p == "/api/cache":
            try:
                self._json(list_cache())
            except (ValueError, RuntimeError) as e:
                self._json({"error": str(e), "entries": []})
            except Exception as e:
                self._json({"error": f"{type(e).__name__}: {e}", "entries": []}, 500)
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
        # There is no authentication, so a page on another origin must not be able to
        # fire these. A cross-origin JSON POST needs a CORS preflight, which this server
        # never grants; the form-style content types that skip the preflight are refused.
        ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if ctype != "application/json":
            return self._json({"error": "Content-Type must be application/json"}, 415)
        try:
            body = self._body()
        except Exception:
            return self._json({"error": "malformed JSON body"}, 400)
        try:
            if p == "/api/cache/delete":
                return self._json(delete_cache(body.get("infohashes") or [],
                                               all_inactive=bool(body.get("all_inactive"))))
            m = re.fullmatch(r"/api/engines/([0-9a-f]{40})/stop", p)
            if m:
                return self._json(stop_engine(m.group(1), delete=bool(body.get("delete_cache"))))
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
        except RuntimeError as e:               # the server or Docker said no
            return self._json({"error": str(e)}, 502)
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
    load_names()
    threading.Thread(target=collect, daemon=True).start()
    threading.Thread(target=log_tailer, daemon=True).start()
    threading.Thread(target=cache_worker, daemon=True).start()
    threading.Thread(target=bootstrap, daemon=True).start()
    srv = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    srv.daemon_threads = True
    print(f"stremio-dash on http://{LISTEN_HOST}:{LISTEN_PORT}  -> "
          f"{ACTIVE['url'] or '(no server configured)'}"
          f"{' via ' + ACTIVE['container'] if ACTIVE['container'] else ' (HTTP only)'}",
          flush=True)
    srv.serve_forever()
