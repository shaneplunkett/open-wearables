FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

COPY . .

EXPOSE 8000

CMD ["uv", "run", "--frozen", "fastmcp", "run", "app/main.py:mcp", "--transport", "streamable-http", "--host", "0.0.0.0", "--port", "8000"]
