# GPU Health Monitoring System

This system monitors GPU utilization across a compute cluster and provides detailed usage statistics and reports.

## Production deployment

Runs as containers on Kubernetes. Data is collected (intended: every 5 minutes) from
the HTCondor collector and written to monthly Parquet files on a shared PVC. Email
reports go out daily, weekly, and monthly.

```
HTCondor → collector.py → gpu_state_YYYY-MM.parquet → usage_stats.py → email
```

See [OPERATIONS.md](OPERATIONS.md) for the current architecture, schedule, and
troubleshooting.

## Project Structure

```
├── read_data.py                # Canonical Parquet/SQLite loading, file discovery, host-config loading
├── classify_slots.py           # Canonical dedup/classify pipeline (prepare_frames, filter_df/filter_df_enhanced)
├── reporting.py                # HTML/text/email report generation
├── usage_stats.py             # Main analysis and reporting (full features, email)
├── report.py                  # Ad-hoc CLI report (just last-day/last-hour), no email
├── collector.py                # Data collection from HTCondor, writes Parquet
├── weekly_gpu_hours_analysis.py  # Weekly GPU hours trend analysis
├── check_unused_gpus.py       # Detect flagship/standard GPUs unused in last week
├── draining_report.py         # Report on draining GPU nodes
├── devices.py                  # GPU device name normalization
├── scripts/                   # Analysis and plotting scripts
│   ├── analyze_evictions.py
│   ├── plot_usage_stats.py
│   ├── plot_wait_times.py
│   ├── plot_gpu_availability.py
│   ├── weekly_summary.py
│   ├── plot_weekly_allocation.py
│   ├── gap_analysis.py
│   └── query.py
├── dashboard/                 # FastAPI real-time GPU state dashboard [WIP — not yet deployed]
│   ├── server.py
│   └── data.py
├── tests/                     # Unit tests
├── templates/                 # HTML report templates
├── archive/                   # Archived experiments and analysis docs
│   ├── experiments/           # One-off analysis scripts
│   ├── debug/                 # Debugging scripts
│   └── analysis_docs/         # Historical analysis and migration docs
└── methodology.md             # Slot classification methodology
```

## Core Modules

### read_data.py / classify_slots.py
The canonical gpu_state pipeline, shared by usage_stats.py, report.py, and the
dashboard: `scan_time_filtered()` lazily scans Parquet, `prepare_frames()`
dedups/classifies once per window. See `classify_slots.slot_dedup_rank()` for
the tie-breaking rule when a GPU has multiple concurrent slot rows. read_data.py
also owns per-month gpu_state file discovery (`get_required_parquet_files`,
`get_latest_timestamp_from_most_recent_parquet`, etc.), preferring Parquet with a
SQLite fallback for months predating the Parquet migration, plus loading the
host-exclusion/CHTC-owned-hosts config files. classify_slots.py also carries
pandas `filter_df`/`filter_df_enhanced` -- the small-window classification
implementation still used by a handful of scripts (`scripts/host_report.py`,
`analysis/analyze_task7_troubleshoot.py`) and its own small-window pandas
functions -- alongside the polars `prepare_frames()` path.

### usage_stats.py
Full-featured analysis engine:
- Allocation and performance usage calculations via the canonical pipeline
- HTML and text report generation
- Email notification support
- Monthly summaries and GPU model snapshots

### report.py
Ad-hoc CLI report (`just last-day`/`last-hour`/`last-day-html`) — no automated
caller, no email support. Same canonical pipeline as usage_stats.py.

## Usage

### Quick commands (justfile)

```bash
just last-day           # 24h GPU usage report with device breakdown
just week               # Weekly GPU hours + allocation summary plots
just weekly-overview    # Weekly GPU hours trend across all DBs
just weekly-allocation  # Weekly allocation percentage plot
just dashboard          # Start real-time dashboard at localhost:8051
```

### Direct CLI

```bash
# 24-hour ad-hoc report, no email
uv run report.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24

# Weekly ad-hoc report (all months)
uv run report.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 168

# Full-featured report with email
uv run usage_stats.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --group-by-device --email-to admin@example.com
```

## Testing

```bash
uv run pytest tests/ -q
```

## GPU Categories

- **Prioritized Service**: Dedicated GPU resources for priority projects
- **Open Capacity**: Shared GPU resources available to all users
- **Backfill**: Opportunistic GPU slots available when other categories are idle
