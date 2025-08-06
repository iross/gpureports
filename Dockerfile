FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
RUN uv venv
ADD . .
# Install dependencies using uv
RUN uv sync

CMD ["uv", "run", "get_gpu_state.py", "./"]
