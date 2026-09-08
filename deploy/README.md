# systemd deployment

Runs the dashboard directly on a Linux host. This is the mode with the fullest access:
being on the host means it can `nsenter` into the Stremio container's network namespace
for client and dial-funnel data (see [../docs/modes.md](../docs/modes.md)).

```bash
./install.sh                                  # this machine, /opt/stremio-dash
HOST=myserver DEST=/srv/stremio-dash ./install.sh
```

Then edit `$DEST/config.json` on the target and `systemctl restart stremio-dash`.
`install.sh` never overwrites an existing `config.json`.

Requires root (for `nsenter` and the Docker CLI), Python 3.9+, and `ffprobe` on PATH.
