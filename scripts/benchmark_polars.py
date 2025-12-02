#!/usr/bin/env python3
"""
Fair benchmark script to compare pandas vs Polars performance.

This version ensures fair comparison by:
1. Pre-converting data to both formats before timing
2. Only measuring the actual operations, not conversion overhead
3. Using realistic dataset sizes
4. Testing with native data loading when possible
"""

import sqlite3
import time
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import polars as pl
import typer

app = typer.Typer()


class BenchmarkResult:
    """Store benchmark results for a single operation."""

    def __init__(self, name: str, pandas_time: float, polars_time: float, description: str = ""):
        self.name = name
        self.pandas_time = pandas_time
        self.polars_time = polars_time
        self.description = description

    @property
    def speedup(self) -> float:
        """Calculate speedup factor (pandas_time / polars_time)."""
        if self.polars_time == 0:
            return float("inf")
        return self.pandas_time / self.polars_time

    @property
    def speedup_pct(self) -> float:
        """Calculate speedup as percentage improvement."""
        if self.pandas_time == 0:
            return 0.0
        return ((self.pandas_time - self.polars_time) / self.pandas_time) * 100

    def __str__(self) -> str:
        winner = "POLARS" if self.speedup > 1.0 else "PANDAS"
        return (
            f"{self.name}:\n"
            f"  Pandas: {self.pandas_time:.4f}s\n"
            f"  Polars: {self.polars_time:.4f}s\n"
            f"  Speedup: {self.speedup:.2f}x ({self.speedup_pct:+.1f}%) - {winner} wins\n"
            f"  Description: {self.description}"
        )


def time_operation(func: Callable, *args, **kwargs) -> float:
    """Time a single operation and return elapsed time in seconds."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    end = time.perf_counter()
    # Keep result in scope to ensure it's actually computed
    _ = result
    return end - start


def benchmark_data_loading(db_path: str, limit: int = 10000) -> BenchmarkResult:
    """Benchmark loading data from SQLite database using native methods."""
    query = f"SELECT * FROM gpu_state ORDER BY timestamp DESC LIMIT {limit}"

    # Pandas - using sqlite3 connection
    pandas_time = time_operation(lambda: pd.read_sql_query(query, sqlite3.connect(db_path)))

    # Polars - using read_database with sqlite3 connection
    polars_time = time_operation(lambda: pl.read_database(query, sqlite3.connect(db_path)))

    return BenchmarkResult(
        "Data Loading (SQLite)", pandas_time, polars_time, f"Load {limit} rows from SQLite using native methods"
    )


def benchmark_filtering(df_pandas: pd.DataFrame, df_polars: pl.DataFrame) -> BenchmarkResult:
    """Benchmark boolean filtering operations with pre-converted data."""

    # Pandas
    def pandas_filter():
        df = df_pandas.copy()
        df = df[
            (df["State"] == "Claimed")
            & (df["PrioritizedProjects"] != "")
            & (~df["Name"].str.contains("backfill", case=False, na=False))
        ]
        return df

    pandas_time = time_operation(pandas_filter)

    # Polars
    def polars_filter():
        df = df_polars.clone()
        df = df.filter(
            (pl.col("State") == "Claimed")
            & (pl.col("PrioritizedProjects") != "")
            & (~pl.col("Name").str.contains("(?i)backfill").fill_null(False))
        )
        return df

    polars_time = time_operation(polars_filter)

    return BenchmarkResult(
        "Boolean Filtering", pandas_time, polars_time, "Filter with multiple conditions and string operations"
    )


def benchmark_string_operations(df_pandas: pd.DataFrame, df_polars: pl.DataFrame) -> BenchmarkResult:
    """Benchmark string operations."""

    # Pandas
    def pandas_strings():
        df = df_pandas.copy()
        mask = df["Machine"].str.contains("gpu", case=False, na=False)
        df = df[mask]
        return df

    pandas_time = time_operation(pandas_strings)

    # Polars
    def polars_strings():
        df = df_polars.clone()
        df = df.filter(pl.col("Machine").str.contains("(?i)gpu").fill_null(False))
        return df

    polars_time = time_operation(polars_strings)

    return BenchmarkResult("String Contains", pandas_time, polars_time, "Case-insensitive string pattern matching")


def benchmark_deduplication(df_pandas: pd.DataFrame, df_polars: pl.DataFrame) -> BenchmarkResult:
    """Benchmark duplicate detection and removal."""

    # Pandas
    def pandas_dedup():
        df = df_pandas.copy()
        df = df.drop_duplicates(subset=["timestamp", "AssignedGPUs"], keep="first")
        return df

    pandas_time = time_operation(pandas_dedup)

    # Polars
    def polars_dedup():
        df = df_polars.clone()
        df = df.unique(subset=["timestamp", "AssignedGPUs"], keep="first")
        return df

    polars_time = time_operation(polars_dedup)

    return BenchmarkResult("Deduplication", pandas_time, polars_time, "Remove duplicates based on subset of columns")


def benchmark_sorting(df_pandas: pd.DataFrame, df_polars: pl.DataFrame) -> BenchmarkResult:
    """Benchmark sorting operations."""

    # Pandas
    def pandas_sort():
        df = df_pandas.copy()
        df = df.sort_values(["AssignedGPUs", "timestamp"], ascending=[True, False])
        return df

    pandas_time = time_operation(pandas_sort)

    # Polars
    def polars_sort():
        df = df_polars.clone()
        df = df.sort(["AssignedGPUs", "timestamp"], descending=[False, True])
        return df

    polars_time = time_operation(polars_sort)

    return BenchmarkResult("Sorting", pandas_time, polars_time, "Sort by multiple columns with mixed order")


def benchmark_groupby_aggregation(df_pandas: pd.DataFrame, df_polars: pl.DataFrame) -> BenchmarkResult:
    """Benchmark groupby and aggregation operations."""

    # Pandas
    def pandas_groupby():
        df = df_pandas.copy()
        df["date"] = df["timestamp"].dt.date
        result = df.groupby("date")["AssignedGPUs"].nunique()
        return result

    pandas_time = time_operation(pandas_groupby)

    # Polars
    def polars_groupby():
        df = df_polars.clone()
        df = df.with_columns(pl.col("timestamp").dt.date().alias("date"))
        result = df.group_by("date").agg(pl.col("AssignedGPUs").n_unique())
        return result

    polars_time = time_operation(polars_groupby)

    return BenchmarkResult("GroupBy + Aggregation", pandas_time, polars_time, "Group by date and count unique GPUs")


def benchmark_datetime_bucketing(df_pandas: pd.DataFrame, df_polars: pl.DataFrame) -> BenchmarkResult:
    """Benchmark datetime bucketing operations."""

    # Pandas
    def pandas_bucket():
        df = df_pandas.copy()
        df["15min_bucket"] = df["timestamp"].dt.floor("15min")
        return df

    pandas_time = time_operation(pandas_bucket)

    # Polars
    def polars_bucket():
        df = df_polars.clone()
        df = df.with_columns(pl.col("timestamp").dt.truncate("15m").alias("15min_bucket"))
        return df

    polars_time = time_operation(polars_bucket)

    return BenchmarkResult("Datetime Bucketing", pandas_time, polars_time, "Create 15-minute time buckets")


def benchmark_null_handling(df_pandas: pd.DataFrame, df_polars: pl.DataFrame) -> BenchmarkResult:
    """Benchmark null checking and filtering."""

    # Pandas
    def pandas_nulls():
        df = df_pandas.copy()
        df = df[df["AssignedGPUs"].notna()]
        return df

    pandas_time = time_operation(pandas_nulls)

    # Polars
    def polars_nulls():
        df = df_polars.clone()
        df = df.filter(pl.col("AssignedGPUs").is_not_null())
        return df

    polars_time = time_operation(polars_nulls)

    return BenchmarkResult("Null Filtering", pandas_time, polars_time, "Filter rows where column is not null")


def benchmark_column_selection(df_pandas: pd.DataFrame, df_polars: pl.DataFrame) -> BenchmarkResult:
    """Benchmark column selection."""

    cols = ["timestamp", "Machine", "State", "AssignedGPUs"]

    # Pandas
    def pandas_select():
        df = df_pandas[cols].copy()
        return df

    pandas_time = time_operation(pandas_select)

    # Polars
    def polars_select():
        df = df_polars.select(cols)
        return df

    polars_time = time_operation(polars_select)

    return BenchmarkResult("Column Selection", pandas_time, polars_time, f"Select {len(cols)} columns from DataFrame")


@app.command()
def run_benchmarks(
    db_path: str = typer.Option(None, help="Path to SQLite database file"),
    limit: int = typer.Option(50000, help="Number of rows to load for benchmarking"),
    iterations: int = typer.Option(5, help="Number of iterations to average"),
):
    """Run all benchmarks with fair comparison methodology."""

    typer.echo("=" * 80)
    typer.echo("FAIR POLARS VS PANDAS BENCHMARK")
    typer.echo("=" * 80)
    typer.echo()
    typer.echo("Methodology:")
    typer.echo("- Data is pre-converted to both pandas and Polars formats")
    typer.echo("- Only the actual operation time is measured (no conversion overhead)")
    typer.echo("- Multiple iterations are averaged to reduce variance")
    typer.echo()

    # Find most recent database if not specified
    if db_path is None:
        import glob

        db_files = glob.glob("gpu_state_*.db")
        if not db_files:
            typer.echo("Error: No database files found. Please specify --db-path")
            raise typer.Exit(1)
        db_path = sorted(db_files)[-1]
        typer.echo(f"Using database: {db_path}\n")

    if not Path(db_path).exists():
        typer.echo(f"Error: Database file not found: {db_path}")
        raise typer.Exit(1)

    # Load sample data
    typer.echo(f"Loading {limit} rows from database...")
    query = f"SELECT * FROM gpu_state ORDER BY timestamp DESC LIMIT {limit}"
    df_pandas = pd.read_sql_query(query, sqlite3.connect(db_path))
    typer.echo(f"Loaded {len(df_pandas)} rows with {len(df_pandas.columns)} columns")

    # Pre-process datetime column for fair comparison
    typer.echo("Pre-processing datetime columns...")
    df_pandas["timestamp"] = pd.to_datetime(df_pandas["timestamp"])

    # Convert to Polars once (not counted in benchmarks)
    typer.echo("Converting to Polars format...")
    df_polars = pl.from_pandas(df_pandas)

    typer.echo(f"Pandas DataFrame size: {df_pandas.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    typer.echo(f"Polars DataFrame estimated size: {df_polars.estimated_size() / 1024 / 1024:.2f} MB")
    typer.echo()

    # List of benchmark functions - updated to take both DataFrames
    benchmarks = [
        (benchmark_data_loading, [db_path, limit], False),  # Loading test uses original method
        (benchmark_filtering, [df_pandas, df_polars], True),
        (benchmark_string_operations, [df_pandas, df_polars], True),
        (benchmark_deduplication, [df_pandas, df_polars], True),
        (benchmark_sorting, [df_pandas, df_polars], True),
        (benchmark_groupby_aggregation, [df_pandas, df_polars], True),
        (benchmark_datetime_bucketing, [df_pandas, df_polars], True),
        (benchmark_null_handling, [df_pandas, df_polars], True),
        (benchmark_column_selection, [df_pandas, df_polars], True),
    ]

    results = []

    typer.echo("=" * 80)
    typer.echo("RUNNING BENCHMARKS")
    typer.echo("=" * 80)
    typer.echo()

    for benchmark_func, args, use_iterations in benchmarks:
        typer.echo(f"Running: {benchmark_func.__name__}...")

        if use_iterations:
            # Run multiple iterations and average
            iteration_results = []
            for i in range(iterations):
                result = benchmark_func(*args)
                iteration_results.append(result)
                typer.echo(
                    f"  Iteration {i+1}/{iterations}: "
                    f"Pandas={result.pandas_time:.4f}s, Polars={result.polars_time:.4f}s, "
                    f"Speedup={result.speedup:.2f}x"
                )

            # Calculate average
            avg_pandas = sum(r.pandas_time for r in iteration_results) / iterations
            avg_polars = sum(r.polars_time for r in iteration_results) / iterations
            avg_result = BenchmarkResult(
                iteration_results[0].name, avg_pandas, avg_polars, iteration_results[0].description
            )
            results.append(avg_result)
        else:
            # Single run (for data loading which creates new connections)
            result = benchmark_func(*args)
            results.append(result)
            typer.echo(
                f"  Pandas={result.pandas_time:.4f}s, Polars={result.polars_time:.4f}s, "
                f"Speedup={result.speedup:.2f}x"
            )

        typer.echo()

    # Display summary
    typer.echo("=" * 80)
    typer.echo("BENCHMARK RESULTS SUMMARY")
    typer.echo("=" * 80)
    typer.echo()

    for result in results:
        typer.echo(str(result))
        typer.echo()

    # Calculate overall statistics
    total_pandas = sum(r.pandas_time for r in results)
    total_polars = sum(r.polars_time for r in results)
    avg_speedup = sum(r.speedup for r in results) / len(results)
    median_speedup = sorted(r.speedup for r in results)[len(results) // 2]

    # Count wins
    polars_wins = sum(1 for r in results if r.speedup > 1.0)
    pandas_wins = len(results) - polars_wins

    typer.echo("=" * 80)
    typer.echo("OVERALL STATISTICS")
    typer.echo("=" * 80)
    typer.echo(f"Total Pandas Time: {total_pandas:.4f}s")
    typer.echo(f"Total Polars Time: {total_polars:.4f}s")
    typer.echo(f"Overall Speedup: {total_pandas / total_polars:.2f}x")
    typer.echo(f"Average Speedup: {avg_speedup:.2f}x")
    typer.echo(f"Median Speedup: {median_speedup:.2f}x")
    typer.echo()
    typer.echo(f"Polars Wins: {polars_wins}/{len(results)} benchmarks")
    typer.echo(f"Pandas Wins: {pandas_wins}/{len(results)} benchmarks")
    typer.echo()

    # Identify best and worst performers
    best = max(results, key=lambda r: r.speedup)
    worst = min(results, key=lambda r: r.speedup)

    typer.echo(f"Best Polars Performance: {best.name} ({best.speedup:.2f}x faster)")
    typer.echo(f"Worst Polars Performance: {worst.name} ({worst.speedup:.2f}x)")
    typer.echo()

    # Analysis
    if avg_speedup >= 1.2:
        typer.echo("✅ RECOMMENDATION: Polars shows significant performance improvement (>20%)")
    elif avg_speedup >= 1.0:
        typer.echo("⚠️  RECOMMENDATION: Polars shows modest improvement, evaluate migration effort")
    else:
        typer.echo("❌ RECOMMENDATION: Pandas performs better, do not migrate")


if __name__ == "__main__":
    app()
