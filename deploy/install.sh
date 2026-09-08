#!/usr/bin/env bash
# Install (or update) stremio-dash as a systemd service.
#
#   ./deploy/install.sh                 # install on this machine
#   HOST=myserver ./deploy/install.sh   # install on a remote host over ssh
#   DEST=/opt/stremio-dash ./deploy/install.sh
#
# Idempotent: safe to re-run to push code changes.
set -euo pipefail

HOST="${HOST:-}"
DEST="${DEST:-/opt/stremio-dash}"
PORT="${PORT:-9481}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() { if [ -n "$HOST" ]; then ssh "$HOST" "$@"; else bash -c "$*"; fi; }
put() { if [ -n "$HOST" ]; then scp -q "$1" "$HOST:$2"; else cp "$1" "$2"; fi; }

echo "-> installing to ${HOST:+$HOST:}$DEST"
run "mkdir -p $DEST"
put "$SRC/app/stremio_dash.py" "$DEST/"
put "$SRC/app/page.html"       "$DEST/"
put "$SRC/config.example.json" "$DEST/"

# Never overwrite an existing config: it holds this deployment's addresses.
run "[ -f $DEST/config.json ] || cp $DEST/config.example.json $DEST/config.json"

run "cat > /etc/systemd/system/stremio-dash.service <<UNIT
[Unit]
Description=stremio-dash - read-only dashboard for a Stremio streaming server
After=docker.service
Wants=docker.service

[Service]
Type=simple
WorkingDirectory=$DEST
ExecStart=/usr/bin/python3 $DEST/stremio_dash.py
Restart=on-failure
RestartSec=10
# The observer must always lose to playback on a small box.
Nice=10
CPUWeight=20
IOWeight=20
Environment=DASH_PORT=$PORT

[Install]
WantedBy=multi-user.target
UNIT"

run "systemctl daemon-reload && systemctl restart stremio-dash"
sleep 2
run "systemctl --no-pager --lines=0 status stremio-dash | head -5"
echo "-> http://${HOST:-localhost}:$PORT"
