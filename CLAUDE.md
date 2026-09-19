# stremio-dash

## Stack
python, javascript, docker, systemd, self-hosted

## What this is

A live dashboard for a Stremio **streaming server** (the `server.js` HTTP
service on :11470, not the Stremio app). The tvOS/iPadOS apps show no network stats;
they stream through a shared server that knows everything, and this surfaces it.

Single-file Python collector (`app/stremio_dash.py`, stdlib only) + a single-file UI
(`app/page.html`). No frameworks, no build step, no third-party packages. Keep it that way.

## Non-negotiables

1. **It is an observer. It must never sit in the media byte path.** If it dies, playback
   is unaffected. Never proxy stream bytes. Its only writes are the two user-requested
   actions, stop and cache delete (ROADMAP Phase 6). It never POSTs `/settings`, and it
   never opens a file no client opened: that makes Stremio download it.
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
  `streamLen == files[idx].length`, but the largest file is the right index **only for a
  single-file torrent**. In a season pack it is just the longest episode: the card once said
  "fully cached" about E11 while E08 was stalling. The file being played comes from the log;
  the largest is a fallback, labelled as a guess, for when no request has been seen.
- `/probe?mediaURL=` returns **HTTP 500** on this build. Use `ffprobe` against the stream URL.
- `uploadSpeed` is bogus — the server mirrors it from `downloadSpeed`. `uploaded` is real.
- There is **no playback-position signal anywhere**: `selections` stays `[]`, and clients hold
  **one long-lived connection** rather than re-requesting. Position must be derived as
  `connection start offset + socket bytes_acked`; both reset together on reconnect.
  That holds while data flows. A player **stalled** on a piece that is not there yet
  re-requests the same offset every ~15 s on a new connection, and the server keeps the
  old ones open.
- **Engine lifecycle** (read from `server.js` 4.20.x):
  - An engine is destroyed **120 s after its last stream connection closes**.
  - Its swarm only pauses once the file is **fully cached**.
  - So a switched-away stream keeps downloading for two minutes by design. It keeps going
    for as long as a player leaks the old connection, which has been seen at ~66 min.
- `GET /{ih}/remove` destroys an engine but **never deletes cached data**. There is no pause
  endpoint and no clear-cache endpoint.
- **Any `GET /{ih}/{idx}` creates the engine**, including our own ffprobe. `stats.json`
  requests do not.
- **Opening a file makes Stremio download all of it.** Probing the largest file of a season
  pack pulled a whole 595 MB episode nobody watched, competing with the one being played.
  Probe only a file a client has opened. Probe URLs carry `?stremio-dash=probe` so the
  log tailer can tell them from players, backlog included.
- The server closes an idle keep-alive connection after ~5 s. Our 2 s poll keeps ours open.
- The Stremio log carries **no client IP**. Only the socket table knows who is connected.

**Reading the log**
- Range requests appear in **two shapes**: `-> GET /<ih>/<idx> bytes=N-` and
  `... /<idx>? bytes=N-`. Match both or playhead tracking silently never fires. The
  Stremio apps send `?tr=…`; the only bare requests seen so far were curl and our ffprobe.
- The path carries the **file index**, and a season pack switches files inside one engine.
  Believe a switch after **two** requests on the new file (opening sends several at once);
  a lone one must not move the card, because nothing may ever re-request the real file.
- Players open a file with a **burst**: header, the index at the file's **end** (often as a
  *pair* of requests), then **one** open-ended request from the resume point, held as a
  single connection. A bounded range (`bytes=N-M`) or a read of the last
  `max(16 MiB, 0.5%)` is never the position; the last request that is neither is. The old
  rule, "believe a far jump only after a second nearby request", got both wrong: it took
  the paired tail reads and dropped the lone resume request, parking the playhead at 99.9%.
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
- One device can hold **several sockets** (stalled re-requests, a leaked old connection).
  The Clients table is one row per address; the engine maths (demand, playback position)
  stays per socket.

**Interpreting it**
- **"No swarm traffic" ≠ "not playing."** A fully cached title is served from disk with zero
  peers. Distinguish, or a happily-playing client reads as paused.
- **Nor does it mean "served from cache".** A client stalled on a file that is still
  downloading sees zero ingest too. Say "from cache" only when the file being read is complete.
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

**Control actions**
- Every POST must be `Content-Type: application/json`. That is the CSRF guard, since there
  is no auth: a cross-origin JSON POST needs a preflight this server never grants. The
  page's `api()` sets the header. Anything new that POSTs must too.
- `STOPPED[ih]` is `inf` while `/remove` is in flight, then the completion time. A tick
  whose stats were fetched before completion (`STOPPED > t0`) drops that engine: showing
  it would read as "re-opened". Probing it would re-create it, so ffprobe also stays
  quiet for 60 s after a stop.
- Cache deletion re-checks a **fresh** `/stats.json`, not the last tick. Directory names must
  match 40-hex, and `rm` gets argv, not a shell.
- The Stop confirmation and the cache panel are built on click, into slots the 2 s tick
  never writes. Otherwise the tick would close them mid-decision.

**Running in a container**
- Inside a container, **127.0.0.1 is the dashboard**, not the host. Discovery rewrites a
  Stremio container's URL to its name on a shared network (its IP on the default bridge,
  where names do not resolve), or to our gateway plus the published port.
- Container detection is `/.dockerenv`, or the **source of the `/etc/hostname` mount**.
  Never match any `/containers/<id>/` in mountinfo: a *host* lists every running
  container's shm mount there and would believe it was a container.
- On a shared network, the dashboard's own polling connection shows up in `ss` as a
  client. `note_own_address()` ignores whatever address the keep-alive socket was bound to.
- `NetworkMode: container:X` is not proof of a live sidecar. After X is recreated, the
  sidecar still holds X's **old** namespace, and its empty socket table reads as
  "nobody watching". Compare `readlink /proc/self/ns/net` in both.
  `--network container:<name>` is stored as the full id.
- `depends_on` waits for the Stremio container to start, not to answer. First-start
  binding must retry.
- Discovery matches "stremio" in names and images, and that includes this image's own
  containers. Exclude them by entrypoint and image name.
- If Docker Desktop containers get **connection refused from every LAN host**, check macOS
  Local Network permission for Docker first. An OS update reset it once.
- There is no buildx on the dev Mac, and the legacy builder rejects `COPY --chmod`.

**UI**
- **Never rebuild the card DOM on a tick.** Update in place via `data-r` slots. Re-rendering
  `innerHTML` destroys and recreates `<details>`, so panels flap open/closed.
- A `<details open>` built via `innerHTML` fires a **spurious `toggle` on insertion**. Panel
  state is therefore session-scoped and updated only on a real `click` on the `<summary>`.
- Collapsed panels are not rendered at all; only their summary count updates.
- A CSS rule with `display:` **outranks the UA rule for `[hidden]`**. The settings overlay
  is `.ovl{display:flex}`, so without an explicit `.ovl[hidden]{display:none}` the popup
  is open on page load. Any new `display:`-carrying class that also uses `hidden` needs
  the same pairing.
- Degrade **explicitly**. Say "unknown" rather than asserting a state the data doesn't support.

## Layout

```
app/{stremio_dash.py,page.html}   collector + UI
Dockerfile, compose.yaml          container image (roles: serve, probe) + example stack
deploy/docker-entrypoint.sh       the image's entrypoint
deploy/install.sh                 systemd install, HOST= and DEST= parameterised
.github/workflows/image.yml       build + mode-C test on push; GHCR publish on vX.Y.Z tags
docs/{modes.md,ROADMAP.md}        capability matrix + remaining work
docs/plan-combined-image.md       Stremio server + dashboard in one image (on hold)
config.example.json               committed; config.json is not
```

`.dockerignore` is a **whitelist**. Anything the image needs must be added there explicitly.
That is deliberate: the working tree holds files that must never ship.

## Where to start

`docs/ROADMAP.md` — current phase, decisions already settled, and what is deliberately
out of scope. The maintainer's own deployment specifics are in `NOTES.homelab.md`
(gitignored).
