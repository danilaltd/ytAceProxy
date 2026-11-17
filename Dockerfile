FROM ghcr.io/astral-sh/uv:0.13.5 AS uv
FROM python:3.11-slim

COPY --from=uv /uv /uvx /usr/local/bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . ./

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 80

CMD ["python", "-m", "proxy_ace.app"]
