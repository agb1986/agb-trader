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
RUN uv sync --frozen --no-default-groups --group app --no-install-project
COPY --from=base /app /app
RUN uv sync --frozen --no-default-groups --group app
USER agb
ARG AGB_VERSION=dev
ENV AGB_VERSION=$AGB_VERSION

FROM runtime AS research
RUN uv sync --frozen --no-default-groups --group research --no-install-project
COPY --from=base /app /app
RUN uv sync --frozen --no-default-groups --group research
USER agb
ARG AGB_VERSION=dev
ENV AGB_VERSION=$AGB_VERSION
