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

# The target is not necessarily reached as root: a Pi running Stremio in Docker is
# typically an ordinary user with passwordless sudo. Escalate only when we must.
SUDO=""
if [ "$(run 'id -u')" != "0" ]; then
  run "sudo -n true" 2>/dev/null || { echo "!! need root or passwordless sudo on the target" >&2; exit 1; }
  SUDO="sudo"
fi

# Written through tee rather than scp: DEST stays root-owned, so an unprivileged
# user cannot edit code that the (root) service will execute.
put() {
  dst="$2"
  case "$dst" in */) dst="$dst$(basename "$1")" ;; esac
  if [ -n "$HOST" ]; then ssh "$HOST" "$SUDO tee $dst >/dev/null" < "$1"
  else $SUDO tee "$dst" >/dev/null < "$1"; fi
}

echo "-> installing to ${HOST:+$HOST:}$DEST"
run "$SUDO mkdir -p $DEST"
put "$SRC/app/stremio_dash.py" "$DEST/"
put "$SRC/app/page.html"       "$DEST/"
put "$SRC/config.example.json" "$DEST/"

# Never overwrite an existing config: it holds this deployment's addresses.
run "[ -f $DEST/config.json ] || $SUDO cp $DEST/config.example.json $DEST/config.json"

run "$SUDO tee /etc/systemd/system/stremio-dash.service >/dev/null <<UNIT
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

run "$SUDO systemctl daemon-reload && $SUDO systemctl enable stremio-dash >/dev/null && $SUDO systemctl restart stremio-dash"
sleep 2
run "$SUDO systemctl --no-pager --lines=0 status stremio-dash | head -5"
echo "-> http://${HOST:-localhost}:$PORT"
