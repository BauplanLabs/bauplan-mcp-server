ARG UV_VERSION="0.10.5"
ARG PYTHON_VERSION="3.12"


#
# Stage: builder
# Uses the combined uv+python image for faster builds
FROM ghcr.io/astral-sh/uv:${UV_VERSION}-python${PYTHON_VERSION}-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --locked --no-install-project --no-dev --extra otel

COPY mcp_bauplan/ mcp_bauplan/
COPY main.py README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --locked --no-dev --extra otel


#
# Stage: final
FROM python:${PYTHON_VERSION}-slim-trixie AS final

ENV TZ=UTC
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ENV PYTHONFAULTHANDLER=1
ENV PYTHONHASHSEED=random
ENV PYTHONUNBUFFERED=1

ENV TRACELOOP_TRACE_CONTENT=false

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --create-home bauplan

ENV PATH="/app/.venv/bin:${PATH}"

USER bauplan

COPY --from=builder --chown=bauplan:bauplan /app/.venv /app/.venv
COPY --chown=bauplan:bauplan main.py CLAUDE.md entrypoint.sh ./
COPY --chown=bauplan:bauplan mcp_bauplan/ mcp_bauplan/

ENV PORT=8000
EXPOSE 8000

ARG CI_DOCKER_SHA
ENV CI_DOCKER_SHA=${CI_DOCKER_SHA}

ARG CI_DOCKER_REF
ENV CI_DOCKER_REF=${CI_DOCKER_REF}

ARG CI_DOCKER_IMAGE_TAG
ENV CI_DOCKER_IMAGE_TAG=${CI_DOCKER_IMAGE_TAG}

ARG CI_DOCKER_BUILD_DATETIME
ENV CI_DOCKER_BUILD_DATETIME=${CI_DOCKER_BUILD_DATETIME}

CMD ["./entrypoint.sh"]
