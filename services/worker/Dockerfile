# Build context: repository root (needs the uv workspace and libs/).
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.10.3 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src

COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY services ./services
RUN uv sync --frozen --no-dev --package sijil-worker

RUN useradd --system --uid 10001 sijil
USER sijil
ENV PATH="/opt/venv/bin:$PATH"
CMD ["celery", "-A", "sijil_worker.main", "worker", "-Q", "default,provisioning,ingest,extraction", "--loglevel", "INFO"]
