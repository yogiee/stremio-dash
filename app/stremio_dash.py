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
import threading
import time
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
SRV = next((s for s in CFG["servers"] if s.get("id") == CFG.get("active")), CFG["servers"][0])

SERVER       = os.environ.get("STREMIO_URL", SRV["url"])
CONTAINER    = os.environ.get("STREMIO_CONTAINER", SRV.get("container") or "stremio")
LISTEN_HOST  = os.environ.get("DASH_HOST", CFG["listen"]["host"])
LISTEN_PORT  = int(os.environ.get("DASH_PORT", CFG["listen"]["port"]))
POLL         = float(os.environ.get("DASH_POLL", CFG["poll"]["interval_sec"]))
# Every request we make is logged by the Stremio server, and Docker's json-file logs
# are unrotated by default. Only /stats.json needs the fast cadence; progress and
# settings move slowly, so they get their own longer intervals.
DETAIL_SEC   = float(os.environ.get("DASH_DETAIL_SEC", CFG["poll"]["detail_sec"]))
SETTINGS_SEC = float(os.environ.get("DASH_SETTINGS_SEC", CFG["poll"]["settings_sec"]))
HISTORY      = int(os.environ.get("DASH_HISTORY", CFG["poll"]["history"]))
# Empty => ask the container to measure its own cache (portable; no host path).
CACHE_DIR    = os.environ.get("STREMIO_CACHE", SRV.get("cache_dir") or "")
DO_RDNS      = os.environ.get("DASH_RDNS", "1" if CFG.get("rdns", True) else "0") == "1"
# IP -> friendly name, per server: different servers sit on different LANs, so this
# cannot be global. Unknown addresses show as the raw IP.
CLIENT_NAMES = SRV.get("client_names") or {}

CLIENT_PORTS = {11470, 12470}
WEBUI_PORTS  = {8080}
LIVE_STATES  = {"ESTAB", "CLOSE-WAIT", "FIN-WAIT-1", "FIN-WAIT-2"}
# The Docker bridge gateway is this dashboard polling the API, not a viewer. Its
# address differs per host and per network, so it is discovered rather than assumed.
IGNORE_CLIENT_IPS = {"127.0.0.1"}

STATE_LOCK = threading.Lock()
STATE = {
    "ok": False, "error": None, "ts": 0, "server_version": None,
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
BITRATE_TRIED = {}    # (infohash, idx) -> last attempt ts


_CONN = {"c": None}
_CONN_LOCK = threading.Lock()
_HOSTPORT = SERVER.split("//", 1)[-1].rstrip("/")


def http_json(path, timeout=4):
    """Single keep-alive connection. Reconnects on any error, retries once.

    Deliberately NOT one connection per request: at a 2s poll that left ~90
    TIME-WAIT sockets sitting in the container's table, which is both wasteful
    and pollutes the very client list this dashboard reports."""
    with _CONN_LOCK:
        for attempt in (0, 1):
            try:
                if _CONN["c"] is None:
                    _CONN["c"] = http.client.HTTPConnection(_HOSTPORT, timeout=timeout)
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


def container_pid():
    """Resolve the Stremio container's host PID, and learn its bridge gateway
    addresses. Traffic from a gateway is this dashboard polling the API, not a
    viewer -- and the bridge subnet differs per host, so it must be discovered
    rather than assumed."""
    now = time.time()
    if _pid_cache["pid"] and now - _pid_cache["at"] < 30:
        return _pid_cache["pid"]
    try:
        fmt = "{{.State.Pid}}{{range .NetworkSettings.Networks}} {{.Gateway}}{{end}}"
        out = subprocess.run(["docker", "inspect", "-f", fmt, CONTAINER],
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
        return [], {"error": "container not running"}
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
        try:
            p = subprocess.Popen(
                # Replay recent history so a dashboard restart mid-stream does not
                # lose the connection's start offset (clients rarely re-request).
                ["docker", "logs", "-f", "-t", "--tail", "3000", CONTAINER],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            for line in p.stdout:
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

    The server's own /probe endpoint 500s on this build, but ffprobe inside the
    container against the local stream URL returns in ~0.15s even when the file is
    only 2.7% downloaded (the header is at the front and already cached). This is
    the honest "how fast does playback need to be fed" number; deriving it from the
    playhead was unreliable because players issue non-playback range requests."""
    while True:
        ih, idx = q.get()
        key = (ih, idx)
        br = 0.0
        try:
            r = subprocess.run(
                ["docker", "exec", CONTAINER, "ffprobe", "-v", "quiet",
                 "-print_format", "json", "-show_entries", "format=duration,bit_rate,size",
                 f"http://127.0.0.1:11470/{ih}/{idx}"],
                capture_output=True, text=True, timeout=30)
            fmt = json.loads(r.stdout or "{}").get("format", {})
            if fmt.get("bit_rate"):
                br = float(fmt["bit_rate"]) / 8.0
            elif fmt.get("size") and fmt.get("duration"):
                br = float(fmt["size"]) / float(fmt["duration"])
        except Exception:
            br = 0.0
        if br > 0:
            BITRATE[key] = br


# Stremio removes a cached file on eviction but leaves the infohash directory behind
# forever, so most entries are empty stubs and must not be counted as cached titles.
def cache_stats_host():
    """Measure a cache directory visible on this filesystem.

    -B1 is real disk usage: Stremio creates each file at full length and fills it
    sparsely, so apparent size (du -sb) overstates it badly."""
    out = subprocess.run(["du", "-s", "-B1", CACHE_DIR],
                         capture_output=True, text=True, timeout=120).stdout.split()
    dirs = [d for d in os.scandir(CACHE_DIR) if d.is_dir()]
    held = sum(1 for d in dirs if any(os.scandir(d.path)))
    return {"bytes": int(out[0]), "entries": held, "stubs": len(dirs) - held,
            "dir": CACHE_DIR, "via": "host"}


def cache_stats_container():
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
    out = subprocess.run(["docker", "exec", CONTAINER, "sh", "-c", script],
                         capture_output=True, text=True, timeout=120)
    kb, total, held = (int(v) for v in out.stdout.split()[:3])
    return {"bytes": kb * 1024, "entries": held, "stubs": total - held,
            "dir": path, "via": "container"}


def cache_worker():
    while True:
        try:
            stats = cache_stats_host() if CACHE_DIR else cache_stats_container()
            with STATE_LOCK:
                STATE["cache"] = stats
        except Exception as e:
            with STATE_LOCK:
                STATE["cache"] = {"error": str(e), "dir": CACHE_DIR or "(container)"}
        time.sleep(60)


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

    while True:
        t0 = time.time()
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
            for c in clients:
                c["name"] = CLIENT_NAMES.get(c["ip"], "")
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

            with STATE_LOCK:
                STATE.update(ok=True, error=None, ts=t0, engines=engines,
                             clients=clients, funnel=funnel, settings=st,
                             server_version=st.get("serverVersion"))
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


# ------------------------------------------------------------------ http
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, body, ctype):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        p = self.path.split("?")[0]
        if p == "/api/state":
            with STATE_LOCK:
                body = json.dumps(STATE)
            self._send(body, "application/json")
        elif p == "/healthz":
            self._send("ok\n", "text/plain")
        elif p in ("/", "/index.html"):
            self._send(PAGE, "text/html; charset=utf-8")
        else:
            self.send_error(404)


PAGE = r"""<title>Stremio Server</title>
<style>
:root{--bg:#0e1116;--panel:#161b22;--panel2:#1c222b;--bd:#262d38;--fg:#e6edf3;--mut:#8b98a8;
--grn:#3fb950;--amb:#d29922;--red:#f85149;--blu:#58a6ff;--pur:#bc8cff}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
.num{font-variant-numeric:tabular-nums;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
header{display:flex;align-items:center;gap:12px;padding:14px 20px;border-bottom:1px solid var(--bd);
background:var(--panel);position:sticky;top:0;z-index:5;flex-wrap:wrap}
h1{font-size:15px;margin:0;font-weight:600;letter-spacing:.2px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--grn);box-shadow:0 0 8px var(--grn)}
.dot.bad{background:var(--red);box-shadow:0 0 8px var(--red)}
.sub{color:var(--mut);font-size:12px}
main{padding:18px 20px 60px;max-width:1200px;margin:0 auto}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:18px}
.tile{background:var(--panel);border:1px solid var(--bd);border-radius:10px;padding:12px 14px}
.tile .k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.6px}
.tile .v{font-size:22px;font-weight:600;margin-top:3px}
.tile .x{font-size:11px;color:var(--mut);margin-top:2px}
.card{background:var(--panel);border:1px solid var(--bd);border-radius:12px;padding:16px;margin-bottom:14px}
.chead{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
.title{font-weight:600;font-size:15px;word-break:break-word}
.badges{display:flex;gap:6px;flex-shrink:0;flex-wrap:wrap}
.b{font-size:10px;font-weight:600;letter-spacing:.5px;padding:3px 8px;border-radius:20px;text-transform:uppercase}
.b.on{background:rgba(63,185,80,.15);color:var(--grn);border:1px solid rgba(63,185,80,.3)}
.b.pz{background:rgba(210,153,34,.15);color:var(--amb);border:1px solid rgba(210,153,34,.3)}
.b.off{background:rgba(139,152,168,.12);color:var(--mut);border:1px solid var(--bd)}
.b.sr{background:rgba(88,166,255,.13);color:var(--blu);border:1px solid rgba(88,166,255,.3)}
.meta{color:var(--mut);font-size:12px;margin:6px 0 10px}
.bar{position:relative;height:9px;background:var(--panel2);border-radius:5px;overflow:hidden;border:1px solid var(--bd)}
.fill{position:absolute;inset:0 auto 0 0;background:linear-gradient(90deg,#1f6feb,#58a6ff);border-radius:5px}
.ph{position:absolute;top:-3px;width:2px;height:15px;background:var(--pur);box-shadow:0 0 6px var(--pur)}
.row{display:flex;gap:18px;align-items:center;margin-top:14px;flex-wrap:wrap}
.speed{font-size:28px;font-weight:650;line-height:1}
.speed small{font-size:12px;color:var(--mut);font-weight:400;margin-left:3px}
.chips{display:flex;gap:8px;flex-wrap:wrap;margin-left:auto}
.chip{background:var(--panel2);border:1px solid var(--bd);border-radius:7px;padding:5px 10px;font-size:12px}
.chip b{font-weight:650}
.chip span{color:var(--mut);font-size:11px;margin-right:5px}
.verdict{margin-top:12px;padding:9px 12px;border-radius:8px;font-size:12.5px;border:1px solid}
.v-ok{background:rgba(63,185,80,.08);border-color:rgba(63,185,80,.3);color:#7ee787}
.v-bad{background:rgba(248,81,73,.08);border-color:rgba(248,81,73,.3);color:#ff9a94}
.v-idle{background:var(--panel2);border-color:var(--bd);color:var(--mut)}
details{margin-top:12px;border-top:1px solid var(--bd);padding-top:10px}
summary{cursor:pointer;color:var(--mut);font-size:12px;user-select:none}
summary:hover{color:var(--fg)}
table{width:100%;border-collapse:collapse;margin-top:9px;font-size:12px}
th{text-align:left;color:var(--mut);font-weight:500;padding:4px 8px;border-bottom:1px solid var(--bd);font-size:11px;text-transform:uppercase;letter-spacing:.4px}
td{padding:4px 8px;border-bottom:1px solid rgba(38,45,56,.5)}
tr:last-child td{border-bottom:0}
.r{text-align:right}
.mut{color:var(--mut)}
.seed{color:var(--grn)}
.err{background:rgba(248,81,73,.1);border:1px solid rgba(248,81,73,.3);color:#ff9a94;padding:10px 14px;border-radius:8px;margin-bottom:14px}
.empty{color:var(--mut);text-align:center;padding:36px;border:1px dashed var(--bd);border-radius:12px}
h2{font-size:12px;text-transform:uppercase;letter-spacing:.7px;color:var(--mut);margin:22px 0 10px;font-weight:600}
.foot{color:var(--mut);font-size:11px;margin-top:26px;line-height:1.7}
</style>

<header>
  <div class="dot" id="dot"></div>
  <h1>Stremio streaming server</h1>
  <div class="sub" id="hdr"></div>
</header>
<main>
  <div id="err"></div>
  <div class="tiles" id="tiles"></div>
  <div id="engines"></div>
  <h2>Clients</h2>
  <div id="clients"></div>
  <div class="foot">
    Read-only observer &mdash; polls <code>/stats.json</code>, the container socket table and the
    container log. It is not in the media byte path.<br>
    Swarm stats belong to the <em>engine</em> (one per infohash) and are shared if two devices play
    the same title. <code>uploadSpeed</code> is not shown: the server mirrors it from
    <code>downloadSpeed</code> and it is meaningless.
  </div>
</main>

<script>
const B=(n)=>{n=n||0;const u=['B','KB','MB','GB','TB'];let i=0;while(n>=1024&&i<4){n/=1024;i++}
  return n.toFixed(n<10&&i>0?2:i?1:0)+' '+u[i]};
const S=(n)=>{n=n||0;return n<1024?n.toFixed(0)+' B/s':(n<1048576?(n/1024).toFixed(0)+' KB/s':(n/1048576).toFixed(2)+' MB/s')};
const N=(n)=>(n||0).toLocaleString();
const esc=(s)=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

function spark(a,w,h,col){
  if(!a||a.length<2)return '';
  const mx=Math.max(...a,1),n=a.length;
  const pts=a.map((v,i)=>`${(i/(n-1)*w).toFixed(1)},${(h-(v/mx)*h).toFixed(1)}`).join(' ');
  return `<svg width="${w}" height="${h}" style="display:block">
    <polyline points="${pts}" fill="none" stroke="${col}" stroke-width="1.6"
      stroke-linejoin="round" stroke-linecap="round"/>
    <polyline points="0,${h} ${pts} ${w},${h}" fill="${col}" opacity=".12" stroke="none"/></svg>`;
}

function engineCard(e){
  const prog=(e.progress==null?0:Math.min(e.progress,1));
  const ph=e.playhead, phPct=(ph&&e.len)?Math.min(ph.offset/e.len,1)*100:null;
  const active=!e.paused&&e.speed>0;
  const badge=active?'<span class="b on">active</span>'
    :(e.paused?'<span class="b pz">swarm paused</span>':'<span class="b off">idle</span>');
  const search=e.searching?'<span class="b sr">peer search</span>':'';

  let verdict='<div class="verdict v-idle">Idle &mdash; no bytes moving. Peer counts below are the '
    +'engine holding its swarm open.</div>';
  if(e.need_bps>0){
    const head=e.speed-e.need_bps, ok=head>=0;
    verdict=`<div class="verdict ${ok?'v-ok':'v-bad'}">Playback needs <b>${S(e.need_bps)}</b>,
      swarm delivers <b>${S(e.speed)}</b> &mdash; ${ok?'keeping up':'short by <b>'+S(-head)+'</b>'}
      ${ok?'':'&middot; expect buffering'}</div>`;
  } else if(active){
    verdict=`<div class="verdict v-ok">Downloading at <b>${S(e.speed)}</b>. Playback rate not
      measured yet (needs ~30s of continuous play).</div>`;
  }

  const tr=(e.sources||[]).filter(s=>s.url).map(s=>{
    const nm=s.url.replace(/^tracker:\/*/,'').replace(/^(udp|http|https):\/*/,'').split('/')[0];
    const dead=(s.numFound||0)===0;
    return `<tr><td class="${dead?'mut':''}">${esc(nm)}</td>
      <td class="r num">${N(s.numFound)}</td><td class="r num">${N(s.numFoundUniq)}</td>
      <td class="r num mut">${N(s.numRequests)}</td></tr>`}).join('');

  const wr=(e.wires||[]).map(w=>`<tr>
      <td class="num">${esc(w.ip||w.address)}</td>
      <td class="mut">${esc((w.rdns||'').slice(0,42))}</td>
      <td>${w.isSeeder?'<span class="seed">seed</span>':'<span class="mut">peer</span>'}</td>
      <td class="r num">${S(w.downSpeed)}</td>
      <td class="r num mut">${S(w.upSpeed)}</td>
      <td class="r num mut">${N(w.requests)}</td></tr>`).join('');

  return `<div class="card">
    <div class="chead">
      <div class="title">${esc(e.name)}</div>
      <div class="badges">${search}${badge}</div>
    </div>
    <div class="meta">
      ${B(e.len)} &middot; ${(prog*100).toFixed(1)}% cached &middot; ${B(e.downloaded)} downloaded
      ${phPct!=null?` &middot; <span style="color:var(--pur)">playhead ${phPct.toFixed(1)}%</span>`:''}
      ${ph&&ph.seeks?` &middot; ${ph.seeks} seek${ph.seeks>1?'s':''}`:''}
      &middot; <span class="num" style="opacity:.5">${esc(e.infohash.slice(0,12))}</span>
    </div>
    <div class="bar"><div class="fill" style="width:${(prog*100).toFixed(2)}%"></div>
      ${phPct!=null?`<div class="ph" style="left:${phPct.toFixed(2)}%"></div>`:''}</div>
    <div class="row">
      <div><div class="speed num">${S(e.speed)}</div>
        <div class="sub" style="margin-top:3px">swarm ingest</div></div>
      <div style="width:150px">${spark(e.spark,150,34,'#58a6ff')}</div>
      <div class="chips">
        <div class="chip"><span>connected</span><b class="num">${e.peers}</b></div>
        <div class="chip"><span>unchoked</span><b class="num" style="color:${e.unchoked?'var(--grn)':'var(--red)'}">${e.unchoked}</b></div>
        <div class="chip"><span>queued</span><b class="num">${N(e.queued)}</b></div>
        <div class="chip"><span>known</span><b class="num">${N(e.unique)}</b></div>
        <div class="chip"><span>swarm</span><b class="num">${e.swarm_conn}/${e.swarm}</b></div>
        <div class="chip"><span>dial attempts</span><b class="num">${N(e.tries)}</b></div>
      </div>
    </div>
    ${verdict}
    <details${(e.wires||[]).length?' open':''}>
      <summary>Peers &mdash; ${(e.wires||[]).length} wire(s) of ${N(e.unique)} known</summary>
      ${wr?`<table><tr><th>Address</th><th>Reverse DNS</th><th></th><th class="r">Down</th>
        <th class="r">Up</th><th class="r">Req</th></tr>${wr}</table>`
        :'<div class="mut" style="padding:8px 0;font-size:12px">No wires held right now.</div>'}
    </details>
    <details><summary>Trackers &mdash; ${(e.sources||[]).length}</summary>
      <table><tr><th>Tracker</th><th class="r">Found</th><th class="r">Unique</th>
        <th class="r">Announces</th></tr>${tr}</table></details>
  </div>`;
}

async function tick(){
  let d;
  try{ d=await (await fetch('/api/state',{cache:'no-store'})).json(); }
  catch(e){ document.getElementById('dot').className='dot bad'; return; }

  document.getElementById('dot').className='dot'+(d.ok?'':' bad');
  const age=d.ts?Math.round(Date.now()/1000-d.ts):0;
  document.getElementById('hdr').textContent=
    `v${d.server_version||'?'} · updated ${age}s ago`;
  document.getElementById('err').innerHTML=d.error?`<div class="err">${esc(d.error)}</div>`:'';

  const es=Object.values(d.engines||{}).sort((a,b)=>b.speed-a.speed);
  const tot=es.reduce((s,e)=>s+(e.speed||0),0);
  const f=d.funnel||{}, cl=(d.clients||[]).filter(c=>c.state==='ESTAB');
  const cache=d.cache||{};
  document.getElementById('tiles').innerHTML=`
    <div class="tile"><div class="k">Total ingest</div><div class="v num">${S(tot)}</div>
      <div class="x">${es.length} engine${es.length===1?'':'s'} loaded</div></div>
    <div class="tile"><div class="k">Clients</div><div class="v num">${cl.length}</div>
      <div class="x">${esc(cl.map(c=>c.name||c.ip).join(', ')||'none connected')}</div></div>
    <div class="tile"><div class="k">Peer dial funnel</div>
      <div class="v num">${f.established||0} <span style="color:var(--mut);font-size:14px">/ ${(f.established||0)+(f.dialing||0)}</span></div>
      <div class="x">${((f.hit_rate||0)*100).toFixed(1)}% of dials connect &middot; ${f.dialing||0} in flight</div></div>
    <div class="tile"><div class="k">Cache</div><div class="v num">${cache.bytes!=null?B(cache.bytes):'—'}</div>
      <div class="x">${cache.entries!=null?cache.entries+' entries':(cache.error?'unreadable':'measuring…')}</div></div>`;

  document.getElementById('engines').innerHTML=es.length?es.map(engineCard).join('')
    :'<div class="empty">No active engines. Start playing something in Stremio.</div>';

  document.getElementById('clients').innerHTML=(d.clients||[]).length?
    `<div class="card" style="padding:8px 16px 14px"><table>
      <tr><th>Client</th><th>Address</th><th>Port</th><th class="r">Delivered rate</th>
      <th class="r">Total sent</th><th class="r">RTT</th><th>State</th></tr>`+
    d.clients.map(c=>`<tr>
      <td>${esc(c.name||'—')}</td><td class="num">${esc(c.ip)}</td>
      <td class="num mut">${c.via}</td>
      <td class="r num">${S(c.delivered_bps)}</td>
      <td class="r num mut">${B(c.bytes_acked)}</td>
      <td class="r num mut">${esc(c.rtt||'—')}ms</td>
      <td>${c.stalled?'<span style="color:var(--amb)">paused / not reading</span>':
        (c.state==='ESTAB'?'<span class="seed">streaming</span>':esc(c.state))}</td></tr>`).join('')
    +'</table></div>'
    :'<div class="empty">No client connected to :11470 / :12470.</div>';
}
tick(); setInterval(tick,2000);
</script>
"""


# page.html next to this file wins over the embedded copy, so the UI can be
# edited and redeployed without rebuilding this module.
_PF = os.path.join(os.path.dirname(os.path.abspath(__file__)), "page.html")
if os.path.exists(_PF):
    try:
        PAGE = open(_PF, encoding="utf-8").read()
    except Exception:
        pass


if __name__ == "__main__":
    threading.Thread(target=collect, daemon=True).start()
    threading.Thread(target=log_tailer, daemon=True).start()
    threading.Thread(target=cache_worker, daemon=True).start()
    srv = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    srv.daemon_threads = True
    print(f"stremio-dash on http://{LISTEN_HOST}:{LISTEN_PORT}  -> {SERVER}", flush=True)
    srv.serve_forever()
