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
- Detect the achieved mode at runtime and badge it in the UI.
- Optional: approximate the runway in mode B from the container's aggregate `tx_bytes`
  (Docker stats API). **Measure before trusting** — it conflates clients and any seeding.

## Phase 3 — settings popup

- `config.json` gains hot reload (currently read once at startup).
- `GET/POST /api/config`, `POST /api/servers/{id}/test` (report achieved mode),
  `POST /api/active`.
- Gear icon → modal: profile list with mode badge and reachability dot, radio to switch,
  add/edit/delete, Test button. Edits names/URLs/client-names only.
- `client_names` is per profile — different servers, different LANs.

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
