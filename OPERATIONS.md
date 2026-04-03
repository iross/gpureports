# Operations Guide

This system runs on a CHTC baremetal host as `iaross`. It collects GPU state data from
HTCondor every 5 minutes and sends allocation reports via email on a daily/weekly/monthly
schedule.

## Data flow

```
HTCondor collector
    → get_gpu_state.py (every 5 min)
    → gpu_state_YYYY-MM.db (SQLite, one file per calendar month)
    → usage_stats.py (via emailer.sh)
    → email report
```

## Crontab entries

```
# Data collection
*/5 * * * * /home/iaross/gpureports/.venv/bin/python /home/iaross/gpureports/get_gpu_state.py &> /tmp/gpu_state.log

# Email reports
0 6 * * *    bash /home/iaross/gpureports/emailer.sh daily   &> /tmp/gpu_emailer.log
0 6 * * 1    bash /home/iaross/gpureports/emailer.sh weekly  &> /tmp/gpu_emailer_weekly.log
0 6 1 * *    bash /home/iaross/gpureports/emailer.sh monthly &> /tmp/gpu_emailer_monthly.log
41 12 * * *  bash /home/iaross/gpureports/emailer.sh test    &> /tmp/gpu_emailer.log
```

To install: `crontab -e` on the production host and paste the above.

## Log files

| Log | Written by |
|-----|-----------|
| `/tmp/gpu_state.log` | `get_gpu_state.py` — data collection |
| `/tmp/gpu_emailer.log` | `emailer.sh daily` and `emailer.sh test` |
| `/tmp/gpu_emailer_weekly.log` | `emailer.sh weekly` |
| `/tmp/gpu_emailer_monthly.log` | `emailer.sh monthly` |

## Database files

SQLite databases live in the repo directory at `/home/iaross/gpureports/`:

```
gpu_state_2025-06.db
gpu_state_2025-07.db
...
gpu_state_YYYY-MM.db   ← one per calendar month, created automatically
```

`get_gpu_state.py` creates a new file on the first run of each month.

## Changing email recipients

Edit the `RECIPIENTS` variable near the top of `emailer.sh`. The `TEST_RECIPIENT` line
controls where `emailer.sh test` sends.

## Re-running a report manually

```bash
ssh <host>
cd /home/iaross/gpureports
bash emailer.sh daily    # or weekly / monthly / test
```

`test` mode sends only to `iaross@wisc.edu` — safe to run anytime without spamming others.

## Common failure modes

**No email sent**
- Check the relevant log file for a Python traceback
- Confirm SMTP is reachable: `nc -z smtp.wiscmail.wisc.edu 25`
- Confirm the venv is intact: `ls .venv/bin/python`

**Empty report or wrong data**
- Check `/tmp/gpu_state.log` — if collection is failing, the DB won't be updated
- Confirm the HTCondor collector is reachable from the host:
  `python -c "import htcondor; print(htcondor.Collector().query()[:1])"`

**Missing DB file / no data for time range**
- Verify `get_gpu_state.py` cron is running: `crontab -l`
- Check disk space: `df -h /home/iaross/gpureports`

**`get_gpu_state.py` exits silently**
- The script uses HTCondor Python bindings (`htcondor` package) which must be installed
  in the system Python or the venv. These are not in `pyproject.toml` because they're
  provided by the HTCondor installation on the host — not installable via pip on dev machines.

## Dependencies

- Python venv at `/home/iaross/gpureports/.venv/` — activated by `emailer.sh`
- HTCondor Python bindings — provided by the HTCondor install on the host, not pip
- SMTP access to `smtp.wiscmail.wisc.edu:25`

To recreate the venv after a Python upgrade or fresh clone:
```bash
cd /home/iaross/gpureports
uv venv
uv pip install -e .
# HTCondor bindings: copy or symlink from system HTCondor install
```
