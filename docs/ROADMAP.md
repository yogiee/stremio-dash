# Roadmap

Status: **working and in daily use** as a systemd service, with multi-server switching.
The container image is built and tested; it still needs a live-playback parity check and
its first release.

## Settled decisions

Do not relitigate these; they were argued through and chosen deliberately.

| decision | choice | why |
|---|---|---|
| Multi-server model | **one active, switch between** | the collector polls one thing; typical servers sit on different networks and are rarely both reachable |
| Remote fidelity | **degrade explicitly by mode** | see `modes.md`; blanks are worse than a stated limitation |
| Packaging | **container image + compose** (systemd path kept) | matches how a Stremio server is already run — a sidecar container next to it, like gluetun/gluetun-webui |
| Settings popup scope | **names + URLs only** | there is no auth; letting the UI set Docker endpoints would be unauthenticated RCE. Docker settings stay file-only |
| Panel open/closed state | **session-scoped, not persisted** | `<details open>` via `innerHTML` fires a spurious `toggle`, which corrupted a stored preference |
| Licence / identity | MIT, personal GitHub account | not the work identity |
| Probe sidecar image | **the dashboard image itself**, run as `command: probe` | one image to build and publish, no version skew between the two, and the layers are shared so the sidecar costs no extra disk. Replaces the planned separate `probe/Dockerfile` |
| Default container binding | **none**; discovered on first start | guessing `stremio-docker` made every install without that exact name report Docker errors. `bootstrap()` binds the one Stremio container it sees, and leaves several to the gear menu |
| Combined server + dashboard image | **on hold** (2026-09-17) | worth doing, as a derived `FROM tsaridas/stremio-docker` image rather than a fork. Design and costs in [plan-combined-image.md](plan-combined-image.md). The standalone image ships first |

## Phase 1 — container image  *(built; parity check pending)*

- `Dockerfile`: alpine 3.24 + python3, docker-cli, **ffprobe** (ffmpeg) and iproute2. ~280 MB,
  most of it ffmpeg's codec libraries.
- One image, two roles: `serve` (default) and `probe`. The probe idles with
  `network_mode: "service:<stremio>"`, and the collector `docker exec`s `ss -tina` into it.
  It is found by namespace-inode match, not configured, and a probe stranded by a
  recreated Stremio container is reported by name.
- `compose.yaml`: dashboard + probe beside an example Stremio service.
- In a container, the collector:
  - reaches a discovered Stremio container over the shared compose network, or via its
    gateway and the published port;
  - ignores its own polling address as a client;
  - binds itself on first start, retrying until Stremio answers.
- Verified on Docker Desktop against a local `tsaridas/stremio-docker`. It reached mode C
  unaided (compose and `docker run --network container:`), a simulated viewer was listed,
  and the stranded-probe case was detected and recovered from. The host `nsenter` path
  is unchanged, checked by running the new detection code on the live systemd host.

**Remaining acceptance:** parity with the systemd deployment during real playback, with
clients, funnel, playhead and runway all present. Needs a Linux host with a client
streaming (Phase 5 does this).

**Follow-up:** the image is heavy for what ffprobe does here (reading container duration).
Either build a minimal static ffprobe (demuxers + http only) or read Matroska/MP4
duration in Python and drop ffmpeg. Either takes the image to roughly 70-90 MB.

Mode A was also verified from the container against a real remote server (HTTP only,
no engines loaded). The bundled ffprobe has not yet run against a live stream.

## Phase 2 — probe backends and mode badging

- ~~Pluggable client/funnel probe: `nsenter` (on host) → sidecar (containerised) → none.~~
  **Done** with Phase 1: `netns_access()`.
- ~~Detect the achieved mode at runtime and badge it in the UI.~~ **Done**, with the settings
  popup: `/api/state` carries the achieved mode and the header and profile rows badge it.
  It reports what was *achieved*, not what was configured — a bound container that is not
  running reads A, not B.
- Optional: approximate the runway in mode B from the container's aggregate `tx_bytes`
  (Docker stats API). **Measure before trusting** — it conflates clients and any seeding.

## Phase 3 — settings popup  *(done)*

Gear icon → modal: profile list with a live mode badge, switch, add, delete, and a Test
button; auto-detected Stremio containers offered from `docker ps`; a free-text URL field
for a server anywhere on the network. `GET /api/config`, `GET /api/discover`,
`POST /api/test`, `POST /api/servers`, `POST /api/servers/{id}[/test|/delete]`,
`POST /api/active`. The active profile is switched in place — no restart — and every
derived signal (history, playhead, sockets, bitrates) is dropped on switch, because that
state describes one server and would otherwise be shown under another's name.

Two things fell out of it and are now settled:

| decision | choice | why |
|---|---|---|
| Container binding from the UI | **closed vocabulary only** | the settled rule was "no Docker endpoints from the UI", to stop an unauthenticated LAN user reaching `docker exec`. The popup still needs to offer the container it found, so the client sends an **id from the server's own `docker ps`**, re-validated at write time. A container name is never taken as free text. URLs stay free text — they only reach `http.client` |
| ffprobe in mode A | **fall back to host ffprobe** | `docs/modes.md` always claimed the bitrate verdict worked over plain HTTP, but the code only ever ran `docker exec <container> ffprobe`, so a remote profile silently had no verdict at all. It now execs in the container when one is bound and uses host ffprobe otherwise |

Still file-only, deliberately: Docker socket paths, `cache_dir`, poll intervals.
`client_names` editing is wired in the API (`POST /api/servers/{id}`) but has no UI yet.

## Phase 4 — publish

- ~~GitHub Actions → GHCR on tag.~~ **Written**, not yet run: `.github/workflows/image.yml`.
  - Every push and PR builds and runs a real mode-C test next to `tsaridas/stremio-docker`.
  - Only a `vX.Y.Z` tag publishes `linux/amd64` + `linux/arm64` as `X.Y.Z`, `X.Y`, `X`
    and `latest`, so `:latest` moves on releases only.
- First release: push a version tag. Then make the GHCR package public: new packages start
  private.
- Screenshots in the README.

## Phase 5 — migrate the existing deployment

Run the container on a second port **alongside** the systemd service, confirm parity, then
retire the unit and move the reverse-proxy entry. Do not swap a working service in place.

## Phase 6 — control actions  *(agreed and built 2026-09-17; live test pending)*

This lifts the read-only rule for exactly two actions. `/settings` stays untouched.
Built and tested against a fake Stremio server and a fake cache. Still needs a live
test with real players.

**Stop stream** — `GET /{ih}/remove`.
- It destroys the engine, which stops the swarm, but does **not** delete cached data.
- Any later `GET /{ih}/{idx}` recreates the engine, so after a Stop the collector's
  ffprobe must skip that key.
- Confirm first, and show whether a client is still attached *and receiving*. On
  16 Sep a player held a stale connection to an abandoned stream for ~66 min, so
  "attached" alone does not mean "watching".

**Clear inactive cache** — delete `stremio-cache/<ih>/` for engines that are not loaded.
Stremio has no endpoint for this: its own eviction only runs when an engine is created.
Needs mode B or better.

**ffprobe retry backoff** — **done**. Retries back off from 120 s to 30 min. The old fixed
120 s equalled Stremio's engine timeout, so a hanging probe could keep a stalled engine
alive.

Not built: an **abandoned badge**. Range requests are no signal here, because a player
reading on one long-lived connection makes none, so "no requests for N minutes" would
flag healthy playback.

Background: Stremio destroys an engine 120 s after its last stream connection closes, and
only pauses its swarm once the file is fully cached. So a switched-away stream keeps
downloading for two minutes by design, longer if the player leaks the connection.

## Out of scope

- Writing to the Stremio server, beyond the two actions in Phase 6. The dashboard never
  POSTs `/settings`.
- Authentication. It is a LAN tool. If that changes, the settings popup needs rethinking.
- Historical storage. Everything is in-memory ring buffers; a restart loses history by design.
