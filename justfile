last-day:
    uv run usage_stats_polars.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --group-by-device
last-day-html:
    uv run usage_stats_polars.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --group-by-device --output-format html --output-file last-day_polars.html
weekly-overview:
    uv run weekly_gpu_hours_analysis.py --plot --databases  gpu_state_*.db
weekly-allocation:
    uv run scripts/weekly_allocation_plot.py --databases gpu_state_*.db
week:
    uv run scripts/weekly_summary.py --databases gpu_state_*.db
dashboard:
    uv run uvicorn dashboard.server:app --reload --port 8051
last-hour:
    uv run usage_stats_polars.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 1 --group-by-device
sync-dbs month=`date +%Y-%m`:
    scp "deepdivesubmit2000.chtc.wisc.edu:/home/iaross/gpureports/*{{month}}.db" .

isye-report hours="168" exclude="tvang9":
    uv run python scripts/host_report.py --host isye --hours-back {{hours}} --exclude-users {{exclude}}
