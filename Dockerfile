# One image, three targets, all built from the same lockfile and the same source tree:
#   base      the shared code and base dependencies, so research and live import the same
#             definitions
#   app       base plus the server services' dependencies (the `app` dependency group)
#   research  base plus the workstation research stack (the `research` dependency group)
#
# Classic Dockerfile syntax only: the workstation builds with the legacy builder, so no
# BuildKit-only features such as RUN --mount.

FROM ghcr.io/astral-sh/uv:0.10.10 AS uv

FROM python:3.12-slim AS runtime
COPY --from=uv /uv /usr/local/bin/uv
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH
RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin agb
WORKDIR /app
COPY pyproject.toml uv.lock ./

FROM runtime AS base
# Dependencies before source, so a code change does not reinstall them.
RUN uv sync --frozen --no-default-groups --no-install-project
COPY README.md ./
COPY src ./src
COPY migrations ./migrations
RUN uv sync --frozen --no-default-groups
USER agb

FROM runtime AS app
# pg_dump for the backup service. Its major version must be at least the server's. The
# pipeline ranks with lightgbm, whose shared library links the GNU OpenMP runtime.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends postgresql-client-17 libgomp1 \
    && rm -rf /var/lib/apt/lists/*
RUN uv sync --frozen --no-default-groups --group app --no-install-project
COPY --from=base /app /app
RUN uv sync --frozen --no-default-groups --group app
USER agb
ARG AGB_VERSION=dev
ENV AGB_VERSION=$AGB_VERSION

FROM runtime AS research
# lightgbm's shared library links the GNU OpenMP runtime, which the slim image lacks.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
RUN uv sync --frozen --no-default-groups --group research --no-install-project
COPY --from=base /app /app
RUN uv sync --frozen --no-default-groups --group research
USER agb
ARG AGB_VERSION=dev
# numba caches vectorbt's compiled functions beside the package, which the container's user
# cannot write; keep them in a scratch directory instead.
ENV AGB_VERSION=$AGB_VERSION \
    NUMBA_CACHE_DIR=/tmp/numba-cache
