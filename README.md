# stremio-dash

A live dashboard for a [Stremio](https://www.stremio.com/) streaming server.

The Stremio apps on **tvOS and iPadOS show no network stats** — they can't run torrent
addons locally, so they stream through a shared streaming server instead. That server
knows everything: peers, speeds, swarm health, what each client is pulling. None of it
is surfaced. This surfaces it.

It is an observer. It never sits in the media byte path, so if it dies, playback is
unaffected. The only changes it makes are the two you ask for: stopping a stream and
deleting cached data.

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

## What it can do

**Stop a stream.** Switch to a different stream of the same title and the old one keeps
downloading. Stremio only drops an engine two minutes after the last connection to it
closes, and a player that never lets go of its old connection keeps it alive
indefinitely. **Stop** on a card ends it at once.

Before stopping, it tells you who is receiving data right now. A socket carries no
infohash, so with several streams loaded it says when it cannot tell which one is being
watched. What is already cached stays unless you also choose to delete it. If a player
asks for the stream again, the card comes back marked **re-opened**.

**Clear the cache.** **Cache** in the header lists every cached title with its size and
when it was last written. You can delete selected titles, or everything not in use.
Anything playing is refused, checked against the live server at the moment of deletion.
Stremio has no endpoint for any of this: left alone, it only frees space when a new
stream starts and the cache is over its cap. Titles are remembered in `names.json` next
to the config, because the cache folder holds none. Needs Docker access, or the cache
directory on the dashboard host.

## Requirements

- A Stremio streaming server (the one bundled with Stremio Desktop, or a container
  image such as `tsaridas/stremio-docker`)
- Either Docker, for the container image, or Python 3.9+ with no third-party packages
- Optional: Docker access to the Stremio container for the deeper signals

See [docs/modes.md](docs/modes.md) for exactly what each level of access buys you.

## Run with Docker

The image is `ghcr.io/yogiee/stremio-dash`, for `linux/amd64` and `linux/arm64`.

Copy the two `stremio-dash` services from [`compose.yaml`](compose.yaml) into the compose
file that already runs your Stremio server, rename `stremio` in them to that service's
name, and `docker compose up -d`. Then open `http://<host>:9481`.

There is nothing to configure for the common case. On first start, with no config yet,
the dashboard binds to the one Stremio container it can see, waits for it to answer, and
saves that to `/config/config.json`. If it sees several, pick one from the gear menu.

On Linux, `network_mode: host` is worth considering: the dashboard then reaches the server
at `127.0.0.1:11470` just as a copy running on the host would, so one config file works
either way and moving between them needs no edit.

| you add | you get |
|---|---|
| nothing but `STREMIO_URL=http://<server>:11470` | mode A: swarm, peers, trackers, bitrate verdict |
| the Docker socket | mode B: + playhead, seeks, cache usage |
| the `stremio-dash-probe` sidecar | mode C: + clients, delivered rates, dial funnel, runway |

The probe is the same image run as `command: probe` inside the Stremio container's network
namespace (`network_mode: "service:<stremio>"`, or `--network container:<name>` for a
Stremio started with `docker run`). The dashboard runs `ss` in it over `docker exec`: the
per-client byte counters exist only in that namespace, and there is no `ss` in the
Stremio image. If only the Stremio container is recreated, as watchtower does, the probe
is left behind in the old namespace. The dashboard detects that and says so, and
`docker compose up -d` puts it right.

`ffprobe` is bundled. It is used when no container is bound, or when the Stremio image
has none of its own. It is also most of the image's ~280 MB.

### Docker access

Mounting `/var/run/docker.sock` gives the dashboard control of the Docker host. That is
root-equivalent, and `:ro` does not change it: that flag protects the socket *file*, not
the API behind it. The dashboard itself only runs `docker ps`, `inspect`, `logs` and
`exec` against Stremio and its probe, and it never takes a container name from the
browser. It has no authentication, though, so keep it on your LAN.

A socket proxy such as `tecnativa/docker-socket-proxy` with `CONTAINERS=1`, reached via
`DOCKER_HOST=tcp://<proxy>:2375`, narrows that to reading containers and their logs. The
playhead and seek tracking still work. Cache usage, the probe sidecar and the in-container
ffprobe do not, because `docker exec` is a `POST`, and allowing `POST` hands most of that
power straight back.

## Configure

The container writes its config to `/config/config.json`. Run directly, copy
`config.example.json` to `config.json` next to the script and set the server URL:

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

## Run without Docker

```bash
python3 app/stremio_dash.py
```

Then open `http://<host>:9481`.

For a systemd service, see [`deploy/`](deploy/). Running on the Docker host itself is the
one way to get mode C without a sidecar: it reaches the Stremio container's network
namespace with `nsenter`, which needs root.

## Status

Working and in daily use, with multi-server switching and a container image. See the
roadmap in `docs/` for what is next.

## Licence

MIT
