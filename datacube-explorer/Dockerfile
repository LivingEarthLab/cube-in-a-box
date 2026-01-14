# syntax=docker/dockerfile:1
FROM git.unepgrid.ch/nostradamus/explorer:3.1.2@sha256:9d4a5f81c167ac45156830c93b7c27bf42bee19d09c91b85dc1482dbdf88f88c AS runtime

LABEL org.opencontainers.image.source="https://git.unepgrid.ch/NOSTRADAMUS/cube-in-a-box-jupyter" \
      org.opencontainers.image.description="Datacube Explorer with planetary-computer support" \
      org.opencontainers.image.licenses="Apache-2.0"

# Bring a real uv binary (the base image may have a 0-byte uv in deployment builds)
FROM ghcr.io/astral-sh/uv:0.9.21@sha256:15f68a476b768083505fe1dbfcc998344d0135f0ca1b8465c4760b323904f05a AS uv

FROM runtime AS builder

ENV PATH=/app/bin:$PATH \
    VIRTUAL_ENV=/app \
    UV_PYTHON=/app/bin/python \
    UV_PYTHON_DOWNLOADS=never

COPY --from=uv /uv /uvx /usr/local/bin/

# Install additional dependency into the /app environment
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install "planetary-computer"

COPY ./cubedash/_utils.py /app/lib/python3.12/site-packages/cubedash/_utils.py

# Final image: keep base entrypoint/CMD; only ship the updated /app environment
FROM runtime
COPY --from=builder --chown=1000:1000 /app /app
