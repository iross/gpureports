last-day:
    uv run report.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24
last-day-html:
    uv run report.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --output-format html --output-file last-day.html
weekly-overview:
    uv run weekly_gpu_hours_analysis.py --plot --databases  gpu_state_*.db
weekly-allocation:
    uv run scripts/plot_weekly_allocation.py
week:
    uv run scripts/weekly_summary.py --databases gpu_state_*.db
dashboard:
    uv run uvicorn dashboard.server:app --reload --port 8051
last-hour:
    uv run report.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 1
sync-dbs month=`date +%Y-%m`:
    scp "deepdivesubmit2000.chtc.wisc.edu:/home/iaross/gpureports/*{{month}}.db" .

isye-report hours="168" exclude="tvang9":
    uv run python scripts/host_report.py --host isye --hours-back {{hours}} --exclude-users {{exclude}}

# Smoke-test both collectors (collector.py and get_job_pressure.py) against the live pool inside
# a smolvm microVM, writing to a scratch dir (default: a fresh temp dir). Needs smolvm. The guest
# is arm64 Linux on Apple Silicon; pip installs htcondor there. Job pressure is polled twice so
# the second poll proves intervals are extended.
smolvm-collectors data_dir="":
    #!/usr/bin/env bash
    set -euo pipefail
    data="{{data_dir}}"
    [ -n "$data" ] || data="$(mktemp -d)"
    mkdir -p "$data"
    data="$(cd "$data" && pwd)"
    echo "Writing to $data"
    smolvm machine run --net --image python:3.12-slim \
        -v "$PWD:/app:ro" -v "$data:/data" --timeout 900s -- sh -c '
        set -e
        pip install -q --root-user-action=ignore htcondor polars-lts-cpu typer duckdb pandas pyyaml pyarrow
        cd /app
        python collector.py /data
        python get_job_pressure.py /data
        sleep 20
        python get_job_pressure.py /data
    '
    uv run python - "$data" <<'EOF'
    import glob
    import sys

    import polars as pl

    d = sys.argv[1]
    gs = pl.read_parquet(glob.glob(f"{d}/gpu_state_*.parquet")[0])
    jp = pl.read_parquet(glob.glob(f"{d}/job_pressure_*.parquet")[0])
    extended = jp.filter(pl.col("last_seen") > pl.col("first_seen"))
    print(f"gpu_state: {gs.height} rows across {gs['Machine'].n_unique()} machines")
    print(jp.group_by("JobState").agg(pl.len().alias("rows")).sort("JobState"))
    print(f"job_pressure: {extended.height} of {jp.height} intervals extended by the second poll")
    assert gs.height > 0, "collector.py wrote no rows"
    assert jp.height > 0, "get_job_pressure.py wrote no rows"
    assert extended.height > 0, "second poll extended no intervals"
    print("OK")
    EOF
