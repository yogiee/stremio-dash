# stremio-dash

A read-only live dashboard for a [Stremio](https://www.stremio.com/) streaming server.

The Stremio apps on **tvOS and iPadOS show no network stats** — they can't run torrent
addons locally, so they stream through a shared streaming server instead. That server
knows everything: peers, speeds, swarm health, what each client is pulling. None of it
is surfaced. This surfaces it.

It is strictly an observer. It never sits in the media byte path, so if it dies,
playback is unaffected.

## What it shows

**Per stream** — title, size, % cached, progress bar with playhead marker, swarm ingest
speed with a rolling sparkline, connected / unchoked / queued / known peers, swarm size,
dial attempts, a tracker health table, and a peer table with reverse DNS.

**The verdict** — the number no Stremio client gives you:

> Playback needs **1.84 MB/s**, swarm delivers **1.96 MB/s** — keeping up, 1.1× headroom

The requirement comes from `ffprobe` against the stream itself. When the swarm falls
behind, it weighs what is already cached ahead of the playhead and reports the runway
rather than crying wolf — a title 40% cached with the playhead at the start is fine even
at half the required rate.

**Peer dial funnel** — established versus attempted connections. Stremio has no
inbound-connectable port, so it only ever reaches peers it dials out to; this makes
that visible.

**Clients** — which devices are connected, their delivered rate, and whether each is
streaming, serving from cache, or idle. Inferred from the socket, because the streaming
server is never told play/pause.

## Requirements

- A Stremio streaming server (the one bundled with Stremio Desktop, or a container
  image such as `tsaridas/stremio-docker`)
- Python 3.9+ — no third-party packages
- `ffprobe` for the bitrate verdict — inside the Stremio container when the dashboard is
  bound to one, otherwise on the dashboard host
- Optional: Docker access to the Stremio container for the deeper signals

See [docs/modes.md](docs/modes.md) for exactly what each level of access buys you.

## Configure

Copy `config.example.json` to `config.json` and set the server URL:

```json
{
  "active": "local",
  "servers": [{
    "id": "local",
    "name": "Stremio",
    "url": "http://127.0.0.1:11470",
    "container": "stremio-docker",
    "client_names": { "192.0.2.10": "Living room TV" }
  }]
}
```

`client_names` is per server — different servers sit on different LANs. Anything not
listed shows as its raw address. Every value can also be overridden by environment
variable (`STREMIO_URL`, `DASH_PORT`, …); anything pinned that way is flagged in the UI,
which then refuses to pretend a switch took effect.

### Servers, from the UI

The gear icon opens the server list. It offers any Stremio container it finds on the
local Docker daemon, and takes a full URL with port for a server anywhere else on the
network. **Nothing is saved until the address answers as a Stremio server**, and the
popup says which mode the addition would achieve before you commit to it. Switching the
active server takes effect immediately — no restart.

Container bindings can only be picked from what was detected locally. That is deliberate:
there is no authentication, and a container name is handed to `docker exec`, so it is
never accepted as free text. URLs are free text because they only ever reach an HTTP
client. Docker socket paths, cache directories and poll intervals stay file-only.

## Run

```bash
python3 app/stremio_dash.py
```

Then open `http://<host>:9481`.

For a systemd service, see [`deploy/`](deploy/).

## Status

Working and in daily use, with multi-server switching. The container image is the
remaining work — see the roadmap in `docs/`.

## Licence

MIT
