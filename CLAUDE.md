# stremio-dash

## Stack
python, javascript, docker, systemd, self-hosted

## What this is

A read-only live dashboard for a Stremio **streaming server** (the `server.js` HTTP
service on :11470, not the Stremio app). The tvOS/iPadOS apps show no network stats;
they stream through a shared server that knows everything, and this surfaces it.

Single-file Python collector (`app/stremio_dash.py`, stdlib only) + a single-file UI
(`app/page.html`). No frameworks, no build step, no third-party packages. Keep it that way.

## Non-negotiables

1. **It is an observer. It must never sit in the media byte path.** If it dies, playback
   is unaffected. Never proxy stream bytes.
2. **Never commit `config.json` or `NOTES.homelab.md`** — real LAN addresses and device
   names. Before every commit: `git diff --cached | grep -nEi '192\.168\.|10\.[0-9]+\.|\.local|<your-domain>'`.
   A homelab-specific README once reached the staging area; the sweep caught it.
3. **Poll interval ≥ 2s.** Target hosts are small (2 vCPU). Every request is also written
   to the Stremio container's log, which is unrotated by default.
4. Commits use the repo-local identity in `.git/config`, not the global one.

## Architecture

Five data sources, in descending order of portability. See `docs/modes.md` for the full
capability matrix — it decides what any given deployment can show.

| # | source | gives |
|---|---|---|
| 1 | `GET /stats.json` | engines, peers (`wires`), trackers, speeds, progress |
| 2 | `GET /{ih}/{idx}/stats.json` | `streamName`, `streamLen`, `streamProgress` |
| 3 | `ffprobe` over HTTP | the bitrate playback actually requires |
| 4 | `docker logs -f` | range requests → playhead offset, seeks |
| 5 | `ss -tina` in the container's netns | client sockets, delivered bytes, dial funnel |

1–3 work against any reachable server. 4 needs Docker access. 5 needs host root
(`nsenter`) or a sidecar joined to that netns.

## Gotchas that will bite you

These were all established empirically against a live server. Re-deriving them is expensive.

**Stremio API**
- `wires[]` is populated *only while the swarm is active* — empty when paused, not absent.
- `streamProgress` / `streamLen` exist **only** on the per-file endpoint, never the global one.
  `streamLen == files[idx].length`, so the largest-file heuristic picks the right index.
- `/probe?mediaURL=` returns **HTTP 500** on this build. Use `ffprobe` against the stream URL.
- `uploadSpeed` is bogus — the server mirrors it from `downloadSpeed`. `uploaded` is real.
- There is **no playback-position signal anywhere**: `selections` stays `[]`, and clients hold
  **one long-lived connection** rather than re-requesting. Position must be derived as
  `connection start offset + socket bytes_acked`; both reset together on reconnect.

**Reading the log**
- Range requests appear in **two shapes**: `-> GET /<ih>/<idx> bytes=N-` and
  `... /<idx>? bytes=N-`. Match both or playhead tracking silently never fires.
- Players read the **file tail** for metadata. A lone far-off request is not a seek —
  require a second nearby request before believing the position moved.
- Tail with `-t` and a backlog (`--tail 3000`) so a restart mid-stream keeps the start offset;
  without timestamps a replayed backlog looks like it just happened.

**Reading sockets**
- **No `ss` in the Stremio image**, and `/proc/net/tcp` has queue depths but **no byte
  counters**. `bytes_acked` requires `ss -i` (inet_diag) inside that netns. There is no
  way around this.
- **Players read in bursts** — flat for ~30s, then one ~9 MB gulp. Never judge activity on
  an instantaneous rate; average over a window wider than one burst.
- `rwnd_limited` is a **cumulative lifetime counter**, not a current state. It sat at 98%
  during healthy playback. Do not use it as a live signal.
- The Docker bridge gateway is *this dashboard polling the API*, not a viewer. Discover it
  via `docker inspect`; it differs per host.
- Poll over **one keep-alive connection**. A fresh connection per poll left ~90 TIME-WAIT
  sockets in the container's table, polluting the very client list being reported.

**Interpreting it**
- **"No swarm traffic" ≠ "not playing."** A fully cached title is served from disk with zero
  peers. Distinguish, or a happily-playing client reads as paused.
- The `needs X` figure must come from **ffprobe**, never the client's delivered rate — for an
  uncached title the client can only receive as fast as the swarm supplies, so it would always
  read "keeping up" precisely while buffering.
- Weigh the **buffer already banked ahead of the playhead**. A title 40% cached with the
  playhead at the start is fine at half the required rate.
- A client socket carries no infohash and the log line carries no IP. Attribute by **playhead
  recency**; if two engines are being read, withhold rather than guess.

**Disk / tooling**
- `du -sb` is *apparent* size and reports ~30 bytes for a directory. Use `du -s -B1` for real
  usage — except busybox (in the container), which rejects `-B1`; there use `du -sk`.
- Stremio leaves an empty infohash directory behind on eviction, so most cache entries are
  stubs. Count only directories holding data.

**UI**
- **Never rebuild the card DOM on a tick.** Update in place via `data-r` slots. Re-rendering
  `innerHTML` destroys and recreates `<details>`, so panels flap open/closed.
- A `<details open>` built via `innerHTML` fires a **spurious `toggle` on insertion**. Panel
  state is therefore session-scoped and updated only on a real `click` on the `<summary>`.
- Collapsed panels are not rendered at all; only their summary count updates.
- Degrade **explicitly**. Say "unknown" rather than asserting a state the data doesn't support.

## Layout

```
app/{stremio_dash.py,page.html}   collector + UI
deploy/install.sh                 systemd install, HOST= and DEST= parameterised
docs/{modes.md,ROADMAP.md}        capability matrix + remaining work
config.example.json               committed; config.json is not
```

## Where to start

`docs/ROADMAP.md` — current phase, decisions already settled, and what is deliberately
out of scope. The maintainer's own deployment specifics are in `NOTES.homelab.md`
(gitignored).
