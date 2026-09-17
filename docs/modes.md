# Capability modes

The dashboard has five data sources and only some of them cross a network boundary.
What you get depends on how it can reach the Stremio server.

| | A: HTTP only | B: + Docker socket | C: + netns probe |
|---|:---:|:---:|:---:|
| engines, title, size, % cached, downloaded | ✅ | ✅ | ✅ |
| swarm speed + sparkline | ✅ | ✅ | ✅ |
| connected / unchoked / queued / known / swarm / dial attempts | ✅ | ✅ | ✅ |
| peers table (address, reverse DNS, seed, up/down) | ✅ | ✅ | ✅ |
| trackers table | ✅ | ✅ | ✅ |
| required bitrate + `needs X / delivers Y` verdict | ✅ | ✅ | ✅ |
| cache **cap** | ✅ | ✅ | ✅ |
| cache **used** | — | ✅ | ✅ |
| playhead position + seek count | — | ✅ | ✅ |
| banked buffer + runway (the amber "will it stall" state) | — | ~ | ✅ |
| clients table (who, delivered rate, RTT, idle/streaming/cache) | — | — | ✅ |
| peer dial funnel | — | — | ✅ |
| "serving from cache" state | — | ~ | ✅ |
| **stop a stream** | ✅ | ✅ | ✅ |
| **delete cached data** | — | ✅ | ✅ |
| who is watching, shown before a stop | — | — | ✅ |

`~` = approximable from the container's aggregate TX counter, which conflates
multiple clients and any seeding. Badged as estimated where used.

## Why

- **HTTP** gets you everything the Stremio server itself knows: swarm state, peers,
  trackers, progress. `ffprobe` runs against the stream URL, so the required-bitrate
  verdict needs no special access either — but note *where* it runs: inside the container
  when one is bound, otherwise on the dashboard host. Mode A therefore needs `ffprobe`
  on the dashboard host, and says so in the verdict when it is missing. The container
  image bundles it, and also falls back to it when the bound container has none.
- **Docker access** adds `docker logs` (range requests → playhead) and letting the
  container measure its own cache.
- **The network namespace** is the only place `bytes_acked` per client socket exists.
  There is no `ss` in the Stremio image and `/proc/net/tcp` carries queue depths, not
  byte counters — so client identity, delivered rates and the dial funnel require
  either host root (`nsenter`) or a sidecar joined to that container's netns.

## How mode C is reached

| dashboard runs | route into the namespace |
|---|---|
| on the Docker host, as root (systemd) | `nsenter -t <pid> -n ss` |
| on the host without root, or in a container | `docker exec <probe> ss`, where the probe is this image run as `command: probe` with `network_mode: "service:<stremio>"` |

The sidecar is found, not configured: it is whichever running container shares the
Stremio container's network namespace. That is checked by comparing namespace inodes,
not by trusting `NetworkMode`, because a sidecar outlives a recreated Stremio container
while still holding the *old* namespace. Its empty socket table would otherwise read as
"nobody watching". The dashboard reports that case by name and drops to mode B.

A working route is re-checked every five minutes, and at once if `ss` fails. A missing one
is re-checked every 30 seconds, so a sidecar started later is picked up without a restart.

Mode A degrades explicitly — panels say what is unavailable rather than showing blanks.
The one that matters: without the buffer runway, the verdict reverts to raw
speed-vs-requirement, so it reads red while a large chunk is already cached ahead.
