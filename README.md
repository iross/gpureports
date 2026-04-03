# GPU Health Monitoring System

This system monitors GPU utilization across a compute cluster and provides detailed usage statistics and reports.

## Production deployment

Running on a CHTC baremetal host as `iaross`. Data is collected every 5 minutes from the
HTCondor collector and written to monthly SQLite databases. Email reports go out daily,
weekly, and monthly via cron.

```
HTCondor → get_gpu_state.py → gpu_state_YYYY-MM.db → usage_stats.py → email
```

See [OPERATIONS.md](OPERATIONS.md) for crontab entries, log locations, and troubleshooting.

## Project Structure

```
├── gpu_utils.py               # Core utilities for GPU data filtering and processing
├── gpu_utils_polars.py        # Polars-based utilities for fast multi-DB loading
├── usage_stats.py             # Main analysis and reporting (full features, email)
├── usage_stats_polars.py      # Polars-accelerated reporting (fast, basic features)
├── get_gpu_state.py           # Data collection from HTCondor
├── get_gpu_state_polars.py    # Polars-based data collection
├── weekly_gpu_hours_analysis.py  # Weekly GPU hours trend analysis
├── check_unused_gpus.py       # Detect flagship/standard GPUs unused in last week
├── draining_report.py         # Report on draining GPU nodes
├── device_name_mappings.py    # GPU device name normalization
├── scripts/                   # Analysis and plotting scripts
│   ├── analyze_evictions.py
│   ├── plot_usage_stats.py
│   ├── plot_wait_times.py
│   ├── plot_gpu_availability.py
│   ├── weekly_summary.py
│   ├── weekly_allocation_plot.py
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

### gpu_utils.py / gpu_utils_polars.py
Centralized utilities for:
- GPU data filtering by utilization type (Priority, Shared, Backfill)
- Counting functions for different GPU categories
- Host exclusion management
- Database file discovery

### usage_stats.py
Full-featured analysis engine:
- Time-filtered data retrieval across multiple database files
- Allocation and performance usage calculations
- HTML and text report generation
- Email notification support
- Monthly summaries and GPU model snapshots

### usage_stats_polars.py
Polars-accelerated version for large datasets:
- 10-100x faster for multi-month queries
- Same CLI interface for basic usage
- Does not support email or monthly analysis (use usage_stats.py for those)

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
# 24-hour report
uv run usage_stats_polars.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24

# Weekly report (all months)
uv run usage_stats_polars.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 168

# Full-featured report with email
uv run usage_stats.py --email-to admin@example.com --email-config smtp_config.yaml
```

## Testing

```bash
uv run pytest tests/ -q
```

## GPU Categories

- **Prioritized Service**: Dedicated GPU resources for priority projects
- **Open Capacity**: Shared GPU resources available to all users
- **Backfill**: Opportunistic GPU slots available when other categories are idle
