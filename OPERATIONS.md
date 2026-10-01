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

HTCondor schedds (ap2001, ap2002)
    → get_job_pressure.py (intended: every 30 min; run today from a baremetal cron -- see below)
    → job_pressure_YYYY-MM.parquet (one file per calendar month)
```

`get_job_pressure.py` is in the image but, as of this writing, the k8s CronJob for it is not deployed
(the manifests live outside this repo); the baremetal cron still runs an older SQLite version until
it is replaced.

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
gpu_state_YYYY-MM.parquet     ← GPU slot state (collector.py), one per calendar month
job_pressure_YYYY-MM.parquet  ← idle + running GPU job intervals with ChtcProjects (get_job_pressure.py), one per calendar month
```

Both scripts create a new file on the first run of each month (UTC).

`job_pressure` has one row per contiguous interval a job spent in one `JobState` (`idle` or `running`),
with `first_seen`/`last_seen` in Unix seconds. Each run reads the month's file, extends or opens
intervals, and atomically replaces it (dot-prefixed `.tmp` file + `os.replace`); an flock on
`.job_pressure_YYYY-MM.parquet.lock` serializes overlapping runs. Run it with `--poll-interval <seconds>`
equal to the CronJob period (default 1800): a job unseen for 3 polls is treated as gone. Readers use
`read_data.load_job_pressure()` (time-window intervals) and `read_data.attribute_claimed_jobs()`
(joins claimed `gpu_state` jobs to their `ChtcProjects` on `GlobalJobId`).

### Converting old SQLite job_pressure files

Historical `job_pressure_YYYY-MM.db` files (from the baremetal cron) are converted, read-only, with:

```bash
uv run migrate_job_pressure.py --output-dir /data job_pressure_2026-05.db job_pressure_2026-06.db ...
```

Interval-schema files are copied as recorded; the old per-snapshot schema (e.g. 2026-05) is merged
into intervals. All migrated rows are `idle`. Do this before retiring the baremetal cron, and copy
any months written after the last sync.

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

## Testing the collectors locally

```bash
just smolvm-collectors            # scratch temp dir
just smolvm-collectors /tmp/gpu   # or a directory of your choice
```

Runs `collector.py` once and `get_job_pressure.py` twice against the live pool inside a
[smolvm](https://github.com/smol-machines/smolvm) microVM (repo mounted read-only at `/app`, output dir at
`/data`, `htcondor` pip-installed in the guest), then checks the resulting Parquet files from the host: both
non-empty, and the second job-pressure poll extended existing intervals. Takes about 45 s.

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
- The script uses HTCondor Python bindings (`htcondor2`, from the `htcondor` package), installed via
  `uv pip install htcondor` in `Dockerfile` -- not in `pyproject.toml` because it's
  not installable via plain `pip` outside that build step.

**`get_job_pressure.py` reports 0 jobs unexpectedly**
- Confirm schedd discovery works: `python -c "import htcondor2 as h; c=h.Collector('cm.chtc.wisc.edu'); print(len(c.locateAll(h.DaemonTypes.Schedd)), 'schedds found')"`
- Only `ap2001.chtc.wisc.edu` and `ap2002.chtc.wisc.edu` are queried (`DEFAULT_SCHEDDS` in the script;
  override with `--schedd <name>`, repeatable). A schedd that cannot be read is logged as
  `Warning: query failed for schedd ...` and skipped. Other schedds were not included because the
  HEP/physics ones deny anonymous queries (`SECMAN:2010 ... DENIED`) and several submit hosts are
  unreachable, so polling them would only add timeouts.

## Dependencies

- HTCondor Python bindings — installed via `uv pip install htcondor` in `Dockerfile`
  (not pinned in `pyproject.toml`; see above)
- SMTP access to `smtp.wiscmail.wisc.edu:25`

To build and test the collector image locally:
```bash
docker build -f Dockerfile -t gpu-reporting .
```
