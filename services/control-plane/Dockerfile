# Build context: repository root (needs the uv workspace and libs/).
FROM python:3.13-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.10.3 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src

COPY pyproject.toml uv.lock ./
COPY libs ./libs
# The invoice PDF's labels: the product's one message catalogue (sijil_core.invoicing).
COPY packages/i18n/messages ./packages/i18n/messages
COPY services ./services
RUN uv sync --frozen --no-dev --package sijil-control-plane

RUN useradd --system --uid 10001 sijil
USER sijil
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /src/services/control-plane
EXPOSE 8000
# Migrations run separately as the schema owner: `alembic upgrade head` (compose service `migrate`).
CMD ["sijil-cp", "serve"]
