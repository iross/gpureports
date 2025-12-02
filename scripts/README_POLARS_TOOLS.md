# Polars Migration Tools

This directory contains tools for benchmarking and profiling the Polars migration.

## Benchmarking Tool

**File:** `benchmark_polars.py`

Measures execution time for pandas vs Polars operations.

### Usage

```bash
# Use the most recent database with default settings
uv run python scripts/benchmark_polars.py

# Specify a database and number of rows
uv run python scripts/benchmark_polars.py --db-path gpu_state_2025-12.db --limit 50000

# Run more iterations for more stable results
uv run python scripts/benchmark_polars.py --iterations 5
```

### What it measures

- Data loading from SQLite
- Datetime conversion
- Boolean filtering (multi-condition)
- String operations (case-insensitive)
- Deduplication
- Sorting
- GroupBy + aggregation
- Datetime bucketing (15-minute intervals)
- Null filtering

### Output

The script outputs:
- Per-operation timing for each iteration
- Average timing across iterations
- Speedup factor (e.g., "3.2x faster")
- Speedup percentage
- Overall statistics
- Best and worst performing operations

## Memory Profiling Tool

**File:** `profile_memory.py`

Measures memory usage for pandas vs Polars operations.

### Requirements

Requires `psutil` for accurate memory measurement:
```bash
uv pip install psutil
```

### Usage

```bash
# Use the most recent database with default settings
uv run python scripts/profile_memory.py

# Specify a database and number of rows
uv run python scripts/profile_memory.py --db-path gpu_state_2025-12.db --limit 50000
```

### What it measures

- Data loading from SQLite
- Datetime conversion
- Boolean filtering
- Deduplication
- GroupBy + aggregation
- Multiple copy operations

### Output

The script outputs:
- Baseline and peak memory usage for each operation
- Memory delta (increase during operation)
- Memory saved by using Polars
- Memory savings percentage
- Overall statistics
- Best and worst memory improvements

## Interpreting Results

### Speedup

- **> 1.0x**: Polars is faster (good!)
- **1.0x**: Same speed
- **< 1.0x**: Polars is slower (investigate why)

**Target:** At least 20% improvement (1.2x speedup) on average

### Memory Savings

- **Positive**: Polars uses less memory (good!)
- **Zero**: Same memory usage
- **Negative**: Polars uses more memory (investigate why)

**Target:** At least 15% memory reduction on average

## Tips

1. **Close other applications** before running benchmarks for more accurate results
2. **Run multiple iterations** (3-5) to account for system variance
3. **Use production-sized datasets** for realistic measurements
4. **Run at different times** to see consistency
5. **Document your results** in the decision document

## Example Results

```
Data Loading:
  Pandas: 0.1234s
  Polars: 0.0567s
  Speedup: 2.18x (+54.1%)

Boolean Filtering:
  Pandas: 0.0892s
  Polars: 0.0234s
  Speedup: 3.81x (+73.8%)

Overall Speedup: 2.45x
```

## Troubleshooting

### "No database files found"
Make sure you're in the project root directory or specify `--db-path`

### "psutil not installed" (memory profiler)
Install with: `uv pip install psutil`

### Inconsistent results
- Run more iterations with `--iterations 5`
- Close other applications
- Run during low system activity

### Out of memory
Reduce dataset size with `--limit 1000`

## Next Steps

After running benchmarks:
1. Document results in `backlog/decisions/`
2. Identify operations that benefit most from Polars
3. Identify any operations where Polars is slower
4. Use results to prioritize migration efforts
