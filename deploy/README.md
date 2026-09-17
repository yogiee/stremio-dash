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

Requires Python 3.9+ on the target, and either root or passwordless sudo -- `nsenter`
needs privileges and the unit file lands in `/etc/systemd/system`. `install.sh` escalates
only where it has to, and leaves `$DEST` root-owned.

`ffprobe` is looked for **inside the Stremio container** first, as
`docker exec <container> ffprobe` against the container-local stream URL. If that exec cannot
run at all, the dashboard falls back to `ffprobe` on the host.

For the container image instead, see "Run with Docker" in the top-level README.
