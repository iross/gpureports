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

# Smoke-test both collectors (collector.py and get_job_pressure.py for the CHTC and OSPool streams)
# against the live pools inside a smolvm microVM, writing to a scratch dir (default: a fresh temp
# dir). Needs smolvm. The guest is arm64 Linux on Apple Silicon; pip installs htcondor there. Job
# pressure is polled twice per pool so the second poll proves intervals are extended.
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
        python get_job_pressure.py /data --pool ospool
        sleep 20
        python get_job_pressure.py /data
        python get_job_pressure.py /data --pool ospool
    '
    uv run python - "$data" <<'EOF'
    import glob
    import sys

    import polars as pl

    d = sys.argv[1]
    gs = pl.read_parquet(glob.glob(f"{d}/gpu_state_*.parquet")[0])
    print(f"gpu_state: {gs.height} rows across {gs['Machine'].n_unique()} machines")
    assert gs.height > 0, "collector.py wrote no rows"
    for prefix in ("job_pressure", "ospool_job_pressure"):
        jp = pl.read_parquet(glob.glob(f"{d}/{prefix}_*.parquet")[0])
        extended = jp.filter(pl.col("last_seen") > pl.col("first_seen"))
        print(jp.group_by("ScheddName", "JobState").agg(pl.len().alias("rows")).sort("ScheddName", "JobState"))
        print(f"{prefix}: {extended.height} of {jp.height} intervals extended by the second poll")
        print(f"{prefix}: ProjectName non-empty on {(jp['ProjectName'] != '').sum()} of {jp.height} rows")
        assert jp.height > 0, f"get_job_pressure.py wrote no {prefix} rows"
        assert extended.height > 0, f"second poll extended no {prefix} intervals"
    print("OK")
    EOF
