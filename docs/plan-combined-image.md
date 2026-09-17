# Plan: Stremio server + dashboard in one image

**Status: on hold** (agreed 2026-09-17). The standalone dashboard image ships first. This file
records the design so it can be picked up without redoing the research.

## Goal

One image, `FROM tsaridas/stremio-docker:<tag>`, that adds the dashboard. Stremio's server is not
touched: it runs from upstream's own layers exactly as upstream built it. The dashboard is bound to
its own server, so there is no server list, no switching and no mode configuration.

**Not a fork.** A fork means rebasing someone else's repo on every release for nothing. This is a
derived image built from a Dockerfile in this repo (`combined/`).

## Why it is worth doing

Inside the server's container, every data source is local:

| source | standalone container | combined image |
|---|---|---|
| `/stats.json`, per-file stats | HTTP | HTTP to `127.0.0.1:11470` |
| ffprobe | `docker exec` or bundled copy | upstream's `/usr/bin/ffprobe` |
| range requests (playhead) | `docker logs -f` | mirrored log file (below) |
| client sockets, dial funnel | `nsenter` or probe sidecar | `ss -tina`, same netns |
| cache usage / clearing | `docker exec` | local filesystem |

That is **full mode C with no Docker socket, no nsenter and no sidecar**. Stop-stream and
clear-cache become local operations too.

## Upstream facts this relies on

Read from `tsaridas/stremio-docker` on 2026-09-17 (v1.3.10):

- Final stage is `alpine` with `nodejs`, `nginx`, and jellyfin-ffmpeg copied to `/usr/bin/ffprobe`.
- `CMD ["./stremio-web-service-run.sh"]` (`sh -e`) does the following:
  - starts `node --no-deprecation -r ./server-log-prefix.js server.js &` in the background;
  - then runs `nginx -g "daemon off;"` in the foreground.
- The server's stdout **is** the container's stdout. Every line gets a `[server]` prefix from a
  `node -r` preload, so upstream already uses a preload hook instead of patching `server.js`.
- `restart_if_idle.sh` restarts node when `/stats.json` is `{}`.
- Releases come out about monthly. Upstream publishes amd64, arm64, arm/v6, arm/v7 and ppc64le.
- Their bundled web UI is Stremio's own web app, pointed at app.strem.io. It offers nothing the
  dashboard needs, and we do not depend on it.
- The cache lives at `/root/.stremio-server/stremio-cache/<infohash>/{0,bitfield}`.

## Design

### Additions to the image

- `python3` and `iproute2` (for `ss`), plus `app/` copied to `/srv/stremio-dash/`.
- A wrapper entrypoint, `dash-run.sh`, which does two things:
  - Starts the dashboard in a restart loop, niced, with a memory cap (`ulimit -v`). It is not
    under `set -e`: a dashboard crash must never end the container.
  - Then `exec ./stremio-web-service-run.sh`, so upstream's process tree is unchanged.

### Log capture (the main integration risk)

The collector needs the server's range-request lines, and inside the container there is no
`docker logs`. Use the same trick upstream already uses:

- Set `NODE_OPTIONS=--require /srv/stremio-dash/log-mirror.js`. The preload wraps
  `process.stdout.write` and also appends each line to `/run/stremio-dash/server.log`.
- The mirror must **never** block or throw into the server:
  - write with a non-blocking append;
  - catch every error;
  - drop lines rather than queue them.
- The collector truncates the file past ~10 MB and its tailer reopens when the size shrinks.
  Upstream's own json-file log is unrotated. This file must not add a second unbounded one.
- Rejected: piping stdout through `tee`. If `tee` dies, node gets EPIPE on stdout, and a FIFO
  blocks the server when the dashboard stops reading. Both put the dashboard in the server's path.
- `NODE_OPTIONS` also reaches `certificate.js`. That is harmless, but verify it.

### Collector changes (most land earlier, in the standalone work)

- `DASH_EMBEDDED=1` selects one fixed profile:
  - URL `http://127.0.0.1:11470`;
  - socket backend `local`;
  - log source `file`;
  - cache directory from the server's `cacheRoot`.
- In embedded mode, the settings popup and `/api/servers*` are hidden. `client_names` come from
  the config file or env.
- Pluggable backends:
  - sockets: add `local` to `nsenter` / `sidecar` / none;
  - logs: add `file` to `docker`.
- The funnel must exclude the dashboard's own listen port. The dashboard now shares the netns,
  so browser connections to `:9481` would otherwise count as peer sockets. `8080` (nginx) is
  already excluded.
- Busybox `du` rejects `-B1`, so the host-path cache measurement needs `du -sk` there.

### Ports

`9481` is exposed next to upstream's `11470`, `12470` and `8080`. It is not served through
upstream's nginx: that config is theirs, and patching it couples us to it.

### Tags and CI

- Tag format is `<upstream tag>-dash<our semver>`, e.g. `v1.3.10-dash0.4.0`. `latest` is the newest pair.
- `linux/amd64` and `linux/arm64` at minimum, matching the standalone image. arm/v7 only if asked.
- A daily scheduled workflow checks upstream's latest release. When no image exists for that
  release, it builds one on the current dashboard release. A dashboard release rebuilds on the
  current upstream tag.
- CI smoke test:
  - start the container;
  - wait for `:11470/settings` and `:9481/healthz`;
  - assert `/api/state` reports mode `C` (this needs no playback).

## Costs, accepted knowingly

- **Weaker isolation.** The dashboard shares the server's cgroup, so a runaway dashboard can get
  node OOM-killed. Mitigate with the memory cap and nice level, and verify by killing and
  starving the dashboard during playback.
- **Updates cut playback.** A new dashboard means a new image, and so a container restart.
  Document "update when idle".
- **Release coupling.** Every upstream release needs a rebuild. It is automated, but ongoing.
- **Licensing.** `server.js` is Stremio's closed-source build, fetched from dl.strem.io. No
  licence text was found for it. The MIT (tsaridas) and GPL-2.0 (official) licences cover those
  repos' Dockerfiles only. Both images redistribute `server.js` anyway; ours would too. Label the
  image unofficial and do not lead its name with "Stremio".

## Acceptance

1. Parity with a mode-C systemd deployment during real playback: clients, funnel, playhead and
   runway are all present.
2. Playback survives `kill -9` of the dashboard, and survives the dashboard hitting its memory cap.
3. The server's output is unchanged, and the mirror file stays bounded over a long session.

## Open questions for the maintainer

- Image name.
- GHCR only, or also Docker Hub?
- Follow every upstream release automatically, or bump by hand?
