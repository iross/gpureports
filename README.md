# GPU Health Monitoring System

This system monitors GPU utilization across a compute cluster and provides detailed usage statistics and reports.

## Project Structure

```
├── gpu_utils.py           # Core utilities for GPU data filtering and processing
├── usage_stats.py         # Main analysis and reporting functionality  
├── get_gpu_state.py       # Data collection from HTCondor
├── analyze.py             # Legacy analysis functions (being phased out)
├── analysis/              # Analysis scripts and notebooks
│   └── analyze_task7_troubleshoot.py
├── debug/                 # Debugging and diagnostic scripts
│   ├── debug_backfill_only.py
│   ├── debug_h200*.py
│   └── debug_priority_filter.py
├── scripts/               # Plotting and utility scripts
│   ├── plot_usage_stats.py
│   ├── plot_wait_times.py
│   ├── gap_analysis.py
│   └── query.py
├── tests/                 # Unit tests
│   ├── test_usage_stats.py
│   └── test_plot_usage_stats.py
└── templates/             # HTML report templates
    └── gpu_report.html
```

## Core Modules

### gpu_utils.py
Centralized utilities for:
- GPU data filtering by utilization type (Priority, Shared, Backfill)
- Counting functions for different GPU categories
- Host exclusion management
- Database file discovery

### usage_stats.py  
Main analysis engine providing:
- Time-filtered data retrieval across multiple database files
- Allocation and performance usage calculations
- HTML and text report generation
- Email notification support

## Usage

### Basic Analysis
```bash
python usage_stats.py --db-path gpu_state_2025-07.db --hours-back 24
```

### Generate HTML Report
```bash
python usage_stats.py --db-path gpu_state_2025-07.db --output-format html --output-file report.html
```

### Email Reports
```bash
python usage_stats.py --email-to admin@example.com --email-config smtp_config.yaml
```

## Testing

Run tests with:
```bash
python -m pytest tests/
```

Or use the project test runner:
```bash
python run_tests.py
```

## GPU Categories

- **Prioritized Service**: Dedicated GPU resources for priority projects
- **Open Capacity**: Shared GPU resources available to all users  
- **Backfill**: Opportunistic GPU slots available when other categories are idle