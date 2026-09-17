# stremio-dash: read-only live dashboard for a Stremio streaming server.
# See README.md ("Run with Docker") and docs/modes.md.
FROM alpine:3.24

# python3     the collector -- stdlib only, no pip
# docker-cli  `docker logs` / `docker exec` against the Stremio container, when the
#             Docker socket is mounted
# ffmpeg      for ffprobe, so the required-bitrate verdict also works against a server
#             with no container bound (mode A)
# iproute2    `ss`, for when this image runs as the probe sidecar
RUN apk add --no-cache python3 docker-cli ffmpeg iproute2

WORKDIR /app
COPY app/stremio_dash.py app/page.html config.example.json ./
COPY deploy/docker-entrypoint.sh /usr/local/bin/stremio-dash
RUN chmod 755 /usr/local/bin/stremio-dash

# config.json lives on a volume: the settings popup writes it, and it holds this
# deployment's addresses, so it must outlive the container.
ENV DASH_CONFIG=/config/config.json \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN mkdir -p /config
VOLUME /config

EXPOSE 9481
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s CMD ["stremio-dash", "health"]

ENTRYPOINT ["stremio-dash"]
CMD ["serve"]
