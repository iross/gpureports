FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cache-friendly layer)
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN uv venv $VIRTUAL_ENV
COPY pyproject.toml uv.lock ./
RUN uv sync
RUN uv pip install htcondor

COPY collector.py get_job_pressure.py usage_stats.py gpu_utils.py gpu_utils_polars.py device_name_mappings.py ./
COPY _emailer.sh stats_calculations.py emailer.sh methodology.md masked_hosts.yaml chtc_owned ./

CMD ["uv", "run", "collector.py", "./"]
