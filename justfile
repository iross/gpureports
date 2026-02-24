last-day:
    uv run usage_stats_polars.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --group-by-device
weekly-overview:
    uv run weekly_gpu_hours_analysis.py --plot --databases  gpu_state_*.db
weekly-allocation:
    uv run scripts/weekly_allocation_plot.py --databases gpu_state_*.db
week:
    uv run scripts/weekly_summary.py --databases gpu_state_*.db
dashboard:
    uv run uvicorn dashboard.server:app --reload --port 8051
