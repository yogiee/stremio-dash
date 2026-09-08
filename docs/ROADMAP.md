# Roadmap

Status: **working and in daily use** as a systemd service. The container packaging and
multi-server support are the remaining work.

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

## Phase 1 — container image  *(next)*

- `Dockerfile`: python:alpine + **ffprobe bundled**, so the required-bitrate verdict works
  in every mode without Docker access to the Stremio container.
- `probe/Dockerfile`: alpine + iproute2, ~10 MB. Runs `sleep infinity` with
  `network_mode: "service:<stremio>"`; the collector `docker exec`s `ss -tina` into it.
  This is the only way to reach `bytes_acked` without host root — there is no `ss` in the
  Stremio image and `/proc/net/tcp` has no byte counters.
- `docker-compose.yml`: dashboard + probe, documented as a fragment to drop beside an
  existing Stremio service.
- Ship the sidecar **from the start**. Without it the container is a straight downgrade on
  the existing deployment: no clients, no funnel, no runway.

**Acceptance:** container reaches parity with the systemd deployment — clients, funnel,
playhead and runway all present — verified against a live stream, not just a healthy start.

Note: mounting the Docker socket is root-equivalent on that host. Document it plainly;
offer a `docker-socket-proxy` restricted to containers/logs/exec as the hardened option.

## Phase 2 — probe backends and mode badging

- Pluggable client/funnel probe: `nsenter` (on host) → sidecar (containerised) → none.
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

- GitHub Actions → GHCR on tag. **Multi-arch `linux/amd64` + `linux/arm64`** is a hard
  requirement: a common target is a Raspberry Pi.
- Publish semver tags, not just `latest`. Anyone running watchtower against `:latest` would
  otherwise have their dashboard auto-update on every push — including the maintainer's own.
- Screenshots in the README before it goes public.

## Phase 5 — migrate the existing deployment

Run the container on a second port **alongside** the systemd service, confirm parity, then
retire the unit and move the reverse-proxy entry. Do not swap a working service in place.

## Out of scope

- Writing to the Stremio server. The dashboard is read-only; it never POSTs `/settings`.
- Authentication. It is a LAN tool. If that changes, the settings popup needs rethinking.
- Historical storage. Everything is in-memory ring buffers; a restart loses history by design.
