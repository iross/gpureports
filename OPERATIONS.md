# Operations Guide

This system collects GPU state data from HTCondor and sends allocation reports via
email on a daily/weekly/monthly schedule. It runs as containers on Kubernetes:
collector.py, get_job_pressure.py, and the emailer all ship in one image
(`hub.opensciencegrid.org/xdd/gpu_reporting`, built from `Dockerfile`); the dashboard
ships separately (`hub.opensciencegrid.org/xdd/gpu_dashboard`, `Dockerfile.dashboard`).
Both are built and pushed by `.github/workflows/build-stat-collector.yml` on every push
to `main`. The k8s manifests themselves (CronJob schedules, namespace, etc.) are not
in this repo -- see the cluster's manifest source for exact schedules and rollout
status.

## Data flow

```
HTCondor collector
    → collector.py (intended: every 5 min)
    → gpu_state_YYYY-MM.parquet (one file per calendar month)
    → usage_stats.py (via emailer.sh)
    → email report

HTCondor schedds (all)
    → get_job_pressure.py (intended: every 5 min)
    → job_pressure_YYYY-MM.db (SQLite, one file per calendar month)
```

collector.py, get_job_pressure.py, and emailer.sh all read/write through a shared PVC
(`gpu-stats-data-pvc`) mounted at `/data` in these containers -- see
[backlog/decisions/task-48-dashboard-pvc-concurrency.md](backlog/decisions/task-48-dashboard-pvc-concurrency.md)
for the storage class and concurrency details (it's ReadWriteOnce, so anything else
mounting the same volume needs same-node affinity).

## Report schedule

`emailer.sh`/`_emailer.sh` support four modes, each intended to run on its own
schedule as a separate k8s CronJob:

```
daily    → full recipient list, 24h report   (intended: 06:00 daily)
weekly   → full recipient list, 168h report   (intended: 06:00 Mondays)
monthly  → full recipient list, monthly summary (intended: 06:00 on the 1st)
test     → iaross only, 24h report            (safe to run anytime)
```

`_emailer.sh` is a dev/test variant: recipients restricted to `iaross@wisc.edu` and
subjects prefixed `[DEV]`.

## Database files

```
gpu_state_YYYY-MM.parquet  ← GPU slot state (collector.py), one per calendar month
job_pressure_YYYY-MM.db    ← idle GPU job queue snapshots (SQLite, get_job_pressure.py), one per calendar month
```

Both scripts create a new file on the first run of each month.

## Changing email recipients

Edit the `RECIPIENTS` variable near the top of `emailer.sh`. The `TEST_RECIPIENT` line
controls where `emailer.sh test` sends.

## Re-running a report manually

```bash
uv run report.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24   # ad-hoc, see `just last-day`
bash emailer.sh test                                                      # exact production path, sends to iaross only
```

`emailer.sh test` sends only to `iaross@wisc.edu` — safe to run anytime without
spamming others. `report.py` (via `just last-day`/`last-hour`) has no automated
caller; it's for manual spot-checks.

## Common failure modes

**No email sent**
- Check the collector/emailer container's logs for a Python traceback
- Confirm SMTP is reachable: `nc -z smtp.wiscmail.wisc.edu 25`

**Empty report or wrong data**
- Confirm collector.py's CronJob is running and the latest gpu_state_YYYY-MM.parquet
  on the PVC is being updated
- Confirm the HTCondor collector is reachable from the cluster:
  `python -c "import htcondor; print(htcondor.Collector().query()[:1])"`

**Missing data file / no data for time range**
- Confirm collector.py's CronJob is running (check the cluster's CronJob/Job status)
- Check PVC free space

**`get_job_pressure.py` exits silently**
- The script uses HTCondor Python bindings (`htcondor` package), installed via
  `uv pip install htcondor` in `Dockerfile` -- not in `pyproject.toml` because it's
  not installable via plain `pip` outside that build step.

**`get_job_pressure.py` reports 0 jobs unexpectedly**
- Confirm schedd discovery works: `python -c "import htcondor; c=htcondor.Collector('cm.chtc.wisc.edu'); print(len(c.locateAll(htcondor.DaemonTypes.Schedd)), 'schedds found')"`

## Dependencies

- HTCondor Python bindings — installed via `uv pip install htcondor` in `Dockerfile`
  (not pinned in `pyproject.toml`; see above)
- SMTP access to `smtp.wiscmail.wisc.edu:25`

To build and test the collector image locally:
```bash
docker build -f Dockerfile -t gpu-reporting .
```
