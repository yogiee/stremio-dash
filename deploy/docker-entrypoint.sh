#!/bin/sh
# stremio-dash container entrypoint. One image, two roles:
#
#   serve   (default) the dashboard
#   probe   sidecar: idles inside the Stremio container's network namespace
#           (network_mode: "service:<stremio>") so the dashboard can `docker exec`
#           ss there -- the only place per-client byte counters exist
#   health  HEALTHCHECK for whichever role this container is playing
#
# Anything else is run as a command, e.g. `docker run --rm <image> ffprobe -version`.
set -eu

case "${1:-serve}" in
  serve)
    # The observer must always lose to playback on a small box.
    exec nice -n 10 python3 /app/stremio_dash.py
    ;;
  probe)
    : > /tmp/.stremio-dash-probe
    trap 'exit 0' TERM INT
    while :; do sleep 3600 & wait $!; done
    ;;
  health)
    if [ -e /tmp/.stremio-dash-probe ]; then
      exec ss -tn state listening >/dev/null
    fi
    exec wget -q -O /dev/null "http://127.0.0.1:${DASH_PORT:-9481}/healthz"
    ;;
  *)
    exec "$@"
    ;;
esac
