FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cache-friendly layer)
RUN uv venv
COPY pyproject.toml uv.lock ./
RUN uv sync

# Copy only the files needed for get_gpu_state.py and emailer.sh
COPY get_gpu_state.py usage_stats.py gpu_utils.py device_name_mappings.py ./
COPY emailer.sh methodology.md masked_hosts.yaml chtc_owned ./

CMD ["uv", "run", "get_gpu_state.py", "./"]
