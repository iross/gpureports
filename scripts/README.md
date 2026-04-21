# Scripts

Analysis and plotting scripts. Some are actively maintained; others were written for
a specific investigation and may be stale.

## Maintained

These are kept working and used regularly:

| Script | Purpose |
|--------|---------|
| `plot_usage_stats.py` | Plot allocation usage over time |
| `plot_gpu_availability.py` | GPU availability by category/type |
| `weekly_allocation_plot.py` | Weekly allocation percentage trend |
| `weekly_summary.py` | Weekly GPU hours summary |
| `gap_analysis.py` | Identify unused capacity windows |
| `query.py` | Ad-hoc DB query helper |

## Exploratory

Written for a specific investigation. No guarantee they still work against current
DB schemas or code:

| Script | Original purpose |
|--------|-----------------|
| `analyze_evictions.py` | Eviction pattern analysis |
| `concurrency_checks.py` | Job concurrency investigation |
| `investigate_backfill_usage.py` | Backfill slot deep-dive |
| `benchmark_polars.py` | Polars vs pandas performance benchmarks |
| `profile_memory.py` | Memory usage profiling |
| `figures.py` | Chart utilities (used by other scripts) |
| `plot_example.py` | Example chart generation |
| `plot_wait_times.py` | Job wait time visualization |
| `healthcheck.py` | System health checks |
| `gpu_playground.py` | Scratch/experimental |
