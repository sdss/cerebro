FROM ghcr.io/astral-sh/uv:0.5.12-python3.13-bookworm-slim

LABEL org.opencontainers.image.authors="Jose Sanchez-Gallego, gallegoj@uw.edu"
LABEL org.opencontainers.image.source=https://github.com/sdss/cerebro

WORKDIR /opt

COPY . cerebro

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

ENV PATH="$PATH:/opt/cerebro/.venv/bin"

# Sync the project
RUN cd cerebro && uv sync --frozen --no-cache

CMD ["/opt/cerebro/.venv/bin/cerebro", "start", "--debug"]
