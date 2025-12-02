#!/usr/bin/env python3
"""
Memory profiling script to compare pandas vs Polars memory usage.

This script measures memory consumption for various operations on GPU state data
to evaluate memory improvements from migrating to Polars.
"""

import gc
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import polars as pl
import typer

app = typer.Typer()


def get_memory_usage_mb() -> float:
    """Get current memory usage in MB using platform-specific method."""
    try:
        import psutil

        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    except ImportError:
        typer.echo("Warning: psutil not installed, using fallback memory tracking")
        # Fallback to sys.getsizeof for basic estimation
        return 0.0


def measure_memory(func: Callable, *args, **kwargs) -> tuple[float, float, any]:
    """
    Measure memory usage for a function.

    Returns:
        Tuple of (baseline_mb, peak_mb, result)
    """
    # Force garbage collection before measurement
    gc.collect()

    baseline = get_memory_usage_mb()
    result = func(*args, **kwargs)
    peak = get_memory_usage_mb()

    return baseline, peak, result


class MemoryProfile:
    """Store memory profiling results for a single operation."""

    def __init__(
        self,
        name: str,
        pandas_baseline: float,
        pandas_peak: float,
        polars_baseline: float,
        polars_peak: float,
        description: str = "",
    ):
        self.name = name
        self.pandas_baseline = pandas_baseline
        self.pandas_peak = pandas_peak
        self.polars_baseline = polars_baseline
        self.polars_peak = polars_peak
        self.description = description

    @property
    def pandas_delta(self) -> float:
        """Memory increase for pandas operation."""
        return self.pandas_peak - self.pandas_baseline

    @property
    def polars_delta(self) -> float:
        """Memory increase for Polars operation."""
        return self.polars_peak - self.polars_baseline

    @property
    def memory_saved(self) -> float:
        """Memory saved by using Polars (MB)."""
        return self.pandas_delta - self.polars_delta

    @property
    def memory_saved_pct(self) -> float:
        """Memory saved as percentage."""
        if self.pandas_delta == 0:
            return 0.0
        return (self.memory_saved / self.pandas_delta) * 100

    def __str__(self) -> str:
        return (
            f"{self.name}:\n"
            f"  Pandas: {self.pandas_delta:.2f} MB (baseline: {self.pandas_baseline:.2f}, peak: {self.pandas_peak:.2f})\n"
            f"  Polars: {self.polars_delta:.2f} MB (baseline: {self.polars_baseline:.2f}, peak: {self.polars_peak:.2f})\n"
            f"  Memory Saved: {self.memory_saved:.2f} MB ({self.memory_saved_pct:+.1f}%)\n"
            f"  Description: {self.description}"
        )


def profile_data_loading(db_path: str, limit: int = 10000) -> MemoryProfile:
    """Profile memory usage for loading data from SQLite."""
    query = f"SELECT * FROM gpu_state ORDER BY timestamp DESC LIMIT {limit}"

    # Pandas
    pandas_baseline, pandas_peak, df_pandas = measure_memory(lambda: pd.read_sql_query(query, sqlite3.connect(db_path)))

    # Polars - convert from pandas
    polars_baseline, polars_peak, df_polars = measure_memory(
        lambda: pl.from_pandas(pd.read_sql_query(query, sqlite3.connect(db_path)))
    )

    return MemoryProfile(
        "Data Loading",
        pandas_baseline,
        pandas_peak,
        polars_baseline,
        polars_peak,
        f"Load {limit} rows from SQLite database",
    )


def profile_datetime_conversion(df_pandas: pd.DataFrame) -> MemoryProfile:
    """Profile memory usage for datetime conversion."""

    # Pandas
    def pandas_datetime():
        df = df_pandas.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df

    pandas_baseline, pandas_peak, _ = measure_memory(pandas_datetime)

    # Polars
    def polars_datetime():
        df = pl.from_pandas(df_pandas)
        # Check if timestamp is string type and needs parsing
        if df.schema["timestamp"] == pl.Utf8:
            df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f"))
        else:
            df = df.with_columns(pl.col("timestamp").cast(pl.Datetime))
        return df

    polars_baseline, polars_peak, _ = measure_memory(polars_datetime)

    return MemoryProfile(
        "Datetime Conversion",
        pandas_baseline,
        pandas_peak,
        polars_baseline,
        polars_peak,
        "Convert timestamp column to datetime type",
    )


def profile_filtering(df_pandas: pd.DataFrame) -> MemoryProfile:
    """Profile memory usage for filtering operations."""

    # Pandas
    def pandas_filter():
        df = df_pandas.copy()
        df = df[
            (df["State"] == "Claimed")
            & (df["PrioritizedProjects"] != "")
            & (~df["Name"].str.contains("backfill", case=False, na=False))
        ]
        return df

    pandas_baseline, pandas_peak, _ = measure_memory(pandas_filter)

    # Polars
    def polars_filter():
        df = pl.from_pandas(df_pandas)
        df = df.filter(
            (pl.col("State") == "Claimed")
            & (pl.col("PrioritizedProjects") != "")
            & (~pl.col("Name").str.contains("(?i)backfill").fill_null(False))
        )
        return df

    polars_baseline, polars_peak, _ = measure_memory(polars_filter)

    return MemoryProfile(
        "Boolean Filtering",
        pandas_baseline,
        pandas_peak,
        polars_baseline,
        polars_peak,
        "Filter with multiple conditions",
    )


def profile_deduplication(df_pandas: pd.DataFrame) -> MemoryProfile:
    """Profile memory usage for deduplication."""

    # Pandas
    def pandas_dedup():
        df = df_pandas.copy()
        df = df.drop_duplicates(subset=["timestamp", "AssignedGPUs"], keep="first")
        return df

    pandas_baseline, pandas_peak, _ = measure_memory(pandas_dedup)

    # Polars
    def polars_dedup():
        df = pl.from_pandas(df_pandas)
        df = df.unique(subset=["timestamp", "AssignedGPUs"], keep="first")
        return df

    polars_baseline, polars_peak, _ = measure_memory(polars_dedup)

    return MemoryProfile(
        "Deduplication", pandas_baseline, pandas_peak, polars_baseline, polars_peak, "Remove duplicates"
    )


def profile_groupby_aggregation(df_pandas: pd.DataFrame) -> MemoryProfile:
    """Profile memory usage for groupby operations."""

    # Prepare data
    df_pandas = df_pandas.copy()
    df_pandas["timestamp"] = pd.to_datetime(df_pandas["timestamp"])

    # Pandas
    def pandas_groupby():
        df = df_pandas.copy()
        df["date"] = df["timestamp"].dt.date
        result = df.groupby("date")["AssignedGPUs"].nunique()
        return result

    pandas_baseline, pandas_peak, _ = measure_memory(pandas_groupby)

    # Polars
    def polars_groupby():
        df = pl.from_pandas(df_pandas)
        df = df.with_columns(pl.col("timestamp").cast(pl.Datetime).dt.date().alias("date"))
        result = df.group_by("date").agg(pl.col("AssignedGPUs").n_unique())
        return result

    polars_baseline, polars_peak, _ = measure_memory(polars_groupby)

    return MemoryProfile(
        "GroupBy + Aggregation",
        pandas_baseline,
        pandas_peak,
        polars_baseline,
        polars_peak,
        "Group by date and count unique",
    )


def profile_copy_operations(df_pandas: pd.DataFrame) -> MemoryProfile:
    """Profile memory usage for copying DataFrames."""

    # Pandas
    def pandas_copy():
        copies = [df_pandas.copy() for _ in range(5)]
        return copies

    pandas_baseline, pandas_peak, _ = measure_memory(pandas_copy)

    # Polars
    def polars_copy():
        df = pl.from_pandas(df_pandas)
        copies = [df.clone() for _ in range(5)]
        return copies

    polars_baseline, polars_peak, _ = measure_memory(polars_copy)

    return MemoryProfile(
        "Multiple Copies", pandas_baseline, pandas_peak, polars_baseline, polars_peak, "Create 5 copies of DataFrame"
    )


@app.command()
def run_profiles(
    db_path: str = typer.Option(None, help="Path to SQLite database file"),
    limit: int = typer.Option(10000, help="Number of rows to load for profiling"),
):
    """Run all memory profiles and display results."""

    # Check if psutil is available
    try:
        import psutil  # noqa: F401

        typer.echo("Using psutil for accurate memory profiling\n")
    except ImportError:
        typer.echo("Warning: psutil not installed. Install with: uv pip install psutil")
        typer.echo("Continuing with basic memory tracking...\n")

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
    typer.echo(f"Loading {limit} rows from database...\n")
    query = f"SELECT * FROM gpu_state ORDER BY timestamp DESC LIMIT {limit}"
    df_pandas = pd.read_sql_query(query, sqlite3.connect(db_path))
    typer.echo(f"Loaded {len(df_pandas)} rows with {len(df_pandas.columns)} columns\n")

    # Estimate DataFrame size
    df_size_mb = df_pandas.memory_usage(deep=True).sum() / 1024 / 1024
    typer.echo(f"DataFrame size: {df_size_mb:.2f} MB\n")

    # List of profiling functions
    profiles = [
        (profile_data_loading, [db_path, limit]),
        (profile_datetime_conversion, [df_pandas]),
        (profile_filtering, [df_pandas]),
        (profile_deduplication, [df_pandas]),
        (profile_groupby_aggregation, [df_pandas]),
        (profile_copy_operations, [df_pandas]),
    ]

    results = []

    typer.echo("=" * 80)
    typer.echo("RUNNING MEMORY PROFILES")
    typer.echo("=" * 80)
    typer.echo()

    for profile_func, args in profiles:
        typer.echo(f"Profiling: {profile_func.__name__}...")
        result = profile_func(*args)
        results.append(result)
        typer.echo(f"  Pandas: {result.pandas_delta:.2f} MB")
        typer.echo(f"  Polars: {result.polars_delta:.2f} MB")
        typer.echo(f"  Saved: {result.memory_saved:.2f} MB")
        typer.echo()

    # Display summary
    typer.echo("=" * 80)
    typer.echo("MEMORY PROFILE RESULTS")
    typer.echo("=" * 80)
    typer.echo()

    for result in results:
        typer.echo(str(result))
        typer.echo()

    # Calculate overall statistics
    total_pandas = sum(r.pandas_delta for r in results)
    total_polars = sum(r.polars_delta for r in results)
    total_saved = total_pandas - total_polars
    total_saved_pct = (total_saved / total_pandas * 100) if total_pandas > 0 else 0

    typer.echo("=" * 80)
    typer.echo("OVERALL STATISTICS")
    typer.echo("=" * 80)
    typer.echo(f"Total Pandas Memory: {total_pandas:.2f} MB")
    typer.echo(f"Total Polars Memory: {total_polars:.2f} MB")
    typer.echo(f"Total Memory Saved: {total_saved:.2f} MB ({total_saved_pct:+.1f}%)")
    typer.echo()

    # Identify best and worst performers
    best = max(results, key=lambda r: r.memory_saved_pct)
    worst = min(results, key=lambda r: r.memory_saved_pct)

    typer.echo(f"Best Memory Reduction: {best.name} ({best.memory_saved_pct:+.1f}%)")
    typer.echo(f"Worst Memory Reduction: {worst.name} ({worst.memory_saved_pct:+.1f}%)")
    typer.echo()


if __name__ == "__main__":
    app()
