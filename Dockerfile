ARG CI_BUILD_VERSION="3.14"
ARG UV_VERSION="0.11.21"


#
# Stage: uv
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv


#
# Stage: builder
FROM debian:trixie-slim AS builder

COPY --from=uv /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV UV_PYTHON_INSTALL_DIR=/usr/local/uv/python

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

ARG CI_BUILD_VERSION
RUN --mount=type=cache,target=/root/.cache/uv \
  uv python install ${CI_BUILD_VERSION}

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --locked --python ${CI_BUILD_VERSION} --no-install-project --no-dev --extra otel

COPY mcp_bauplan/ mcp_bauplan/
COPY main.py README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --locked --python ${CI_BUILD_VERSION} --no-dev --extra otel


#
# Stage: final
FROM debian:trixie-slim AS final

ENV TZ=UTC
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ENV PYTHONFAULTHANDLER=1
ENV PYTHONHASHSEED=random
ENV PYTHONUNBUFFERED=1

ENV TRACELOOP_TRACE_CONTENT=false

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --create-home bauplan

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

USER bauplan

COPY --from=builder /usr/local/uv/python /usr/local/uv/python
COPY --from=builder --chown=bauplan:bauplan /app/.venv /app/.venv
COPY --chown=bauplan:bauplan main.py entrypoint.sh ./
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
