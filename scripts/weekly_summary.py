#!/usr/bin/env python3
"""
Weekly GPU Summary

Produces two side-by-side plots from the same data pipeline:
  1. Weekly GPU Hours Over Time (primary vs backfill)
  2. Weekly Allocation Percentage by Category (prioritized, open capacity, backfill)

Both plots share consistent filtering (host exclusions, old GPU type removal)
and the same week boundaries (Monday-aligned, 7-day complete weeks only).
"""

import argparse
import sqlite3
import sys
from pathlib import Path

import polars as pl

try:
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from gpu_utils_polars import HOST_EXCLUSIONS, load_chtc_owned_hosts

NEEDED_COLUMNS = [
    "Name",
    "AssignedGPUs",
    "State",
    "Machine",
    "PrioritizedProjects",
    "GPUs_DeviceName",
    "timestamp",
]
NEEDED_COLUMNS_SQL = ", ".join(NEEDED_COLUMNS)
OLD_GPU_TYPES = ["GTX 1080", "P100", "Quadro", "A30", "A40"]

# Consistent colors across both plots
COLOR_PRIMARY = "#2E86AB"
COLOR_BACKFILL = "#A23B72"
COLOR_OPEN_CAPACITY = "#F18F01"
COLOR_TOTAL = "#404040"


# ---------------------------------------------------------------------------
# Data loading & classification
# ---------------------------------------------------------------------------


def load_data_from_database(db_path: str) -> pl.DataFrame:
    """Load needed columns from a single database file."""
    if not Path(db_path).exists():
        print(f"Warning: Database {db_path} not found, skipping")
        return pl.DataFrame()

    try:
        conn = sqlite3.connect(db_path)
        df = pl.read_database(f"SELECT {NEEDED_COLUMNS_SQL} FROM gpu_state", conn)
        conn.close()

        if len(df) > 0:
            print(f"Loaded {len(df):,} rows from {db_path}")
            if df["timestamp"].dtype == pl.Utf8:
                df = df.with_columns(pl.col("timestamp").str.to_datetime())
            return df
    except Exception as e:
        print(f"Error loading {db_path}: {e}")

    return pl.DataFrame()


def _apply_duplicate_cleanup(df: pl.DataFrame) -> pl.DataFrame:
    """
    For rows where the same GPU appears multiple times in the same timestamp,
    keep the highest-priority version.
    Rank: claimed+primary(3) > claimed+backfill(2) > unclaimed+primary(1) > unclaimed+backfill(0)
    """
    is_backfill = pl.col("Name").str.contains("backfill")
    is_claimed = pl.col("State") == "Claimed"

    df = df.with_columns(
        pl.when(is_claimed & ~is_backfill)
        .then(3)
        .when(is_claimed & is_backfill)
        .then(2)
        .when(~is_claimed & ~is_backfill)
        .then(1)
        .otherwise(0)
        .alias("_rank")
    )
    df = df.sort(["AssignedGPUs", "_rank"], descending=[False, True])
    df = df.unique(subset=["timestamp", "AssignedGPUs"], keep="first")
    df = df.drop("_rank")
    return df


def classify_rows(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply host exclusions, GPU type filtering, and classify each row into a
    slot_type matching filter_df_enhanced logic.
    """
    chtc_owned_hosts = load_chtc_owned_hosts()
    chtc_list = list(chtc_owned_hosts)

    is_backfill = pl.col("Name").str.contains("backfill")
    is_chtc = pl.col("Machine").is_in(chtc_list)

    # Host exclusions
    if HOST_EXCLUSIONS:
        for excluded_host in HOST_EXCLUSIONS.keys():
            df = df.filter(~pl.col("Machine").str.contains(f"(?i){excluded_host}").fill_null(False))

    # Filter out old/uncommon GPU types and null device names
    old_pattern = "|".join(OLD_GPU_TYPES)
    df = df.filter(
        pl.col("GPUs_DeviceName").is_not_null()
        & (pl.col("GPUs_DeviceName") != "")
        & ~pl.col("GPUs_DeviceName").str.contains(old_pattern).fill_null(False)
    )

    # Priority expressions matching filter_df_enhanced
    has_priority = (pl.col("PrioritizedProjects") != "") & pl.col("PrioritizedProjects").is_not_null()
    priority_is_empty = pl.col("PrioritizedProjects") == ""
    priority_is_null_or_empty = (pl.col("PrioritizedProjects") == "") | pl.col("PrioritizedProjects").is_null()

    df = df.with_columns(
        pl.when(is_backfill & has_priority & ~is_chtc)
        .then(pl.lit("Backfill-ResearcherOwned"))
        .when(is_backfill & is_chtc)
        .then(pl.lit("Backfill-CHTCOwned"))
        .when(is_backfill & priority_is_null_or_empty & ~is_chtc)
        .then(pl.lit("Backfill-OpenCapacity"))
        .when(~is_backfill & has_priority & ~is_chtc)
        .then(pl.lit("Priority-ResearcherOwned"))
        .when(~is_backfill & has_priority & is_chtc)
        .then(pl.lit("Priority-CHTCOwned"))
        .when(~is_backfill & priority_is_empty)
        .then(pl.lit("Shared"))
        .otherwise(pl.lit("Other"))
        .alias("slot_type")
    )

    return df


# ---------------------------------------------------------------------------
# Per-DB bucket stats (keeps memory bounded)
# ---------------------------------------------------------------------------


def _compute_bucket_stats(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute per-bucket (15-min) stats for one DB.

    Returns a DataFrame with columns:
        week_start, bucket, category, total_gpus, claimed_gpus
    """
    if len(df) == 0:
        return pl.DataFrame(
            schema={
                "week_start": pl.Datetime,
                "bucket": pl.Datetime,
                "category": pl.Utf8,
                "total_gpus": pl.UInt32,
                "claimed_gpus": pl.UInt32,
            }
        )

    df = classify_rows(df)

    df = df.with_columns(
        [
            pl.col("timestamp").dt.truncate("1w").alias("week_start"),
            pl.col("timestamp").dt.truncate("15m").alias("bucket"),
        ]
    )

    category_map = {
        "Priority-ResearcherOwned": "prioritized",
        "Priority-CHTCOwned": "prioritized",
        "Shared": "open_capacity",
        "Backfill-ResearcherOwned": "backfill",
        "Backfill-CHTCOwned": "backfill",
        "Backfill-OpenCapacity": "backfill",
    }

    df = df.with_columns(pl.col("slot_type").replace_strict(category_map, default="other").alias("category"))
    df = df.filter(pl.col("category") != "other")

    # Dedup for primary (prioritized + open_capacity) slots
    full_deduped = _apply_duplicate_cleanup(df.filter(pl.col("AssignedGPUs").is_not_null()))
    primary_deduped = full_deduped.filter(pl.col("category") != "backfill")

    primary_all_gpus = (
        df.filter((pl.col("category") != "backfill") & pl.col("AssignedGPUs").is_not_null())
        .group_by(["week_start", "bucket", "category"])
        .agg(pl.col("AssignedGPUs").n_unique().alias("total_gpus"))
    )

    primary_claimed_gpus = (
        primary_deduped.filter((pl.col("State") == "Claimed") & pl.col("AssignedGPUs").is_not_null())
        .group_by(["week_start", "bucket", "category"])
        .agg(pl.col("AssignedGPUs").n_unique().alias("claimed_gpus"))
    )

    primary_stats = primary_all_gpus.join(
        primary_claimed_gpus,
        on=["week_start", "bucket", "category"],
        how="left",
    ).with_columns(pl.col("claimed_gpus").fill_null(0))

    backfill_df = df.filter(pl.col("category") == "backfill")
    backfill_stats = (
        backfill_df.filter(pl.col("AssignedGPUs").is_not_null())
        .group_by(["week_start", "bucket", "category"])
        .agg(
            [
                pl.col("AssignedGPUs").n_unique().alias("total_gpus"),
                pl.col("AssignedGPUs").filter(pl.col("State") == "Claimed").n_unique().alias("claimed_gpus"),
            ]
        )
    )

    return pl.concat([primary_stats, backfill_stats])


# ---------------------------------------------------------------------------
# Weekly aggregation
# ---------------------------------------------------------------------------


def compute_weekly_stats(db_paths: list[str]) -> pl.DataFrame:
    """
    Process databases one at a time and return a single weekly DataFrame with
    both GPU-hours and allocation-percentage columns.

    Returned columns per category (prioritized, open_capacity, backfill):
        {cat}_pct          – allocation percentage
        {cat}_avg_total    – avg total GPUs per interval
        {cat}_avg_claimed  – avg claimed GPUs per interval
        {cat}_gpu_hours    – estimated GPU-hours (claimed_gpUs * 0.25h * intervals)
        {cat}_intervals    – number of 15-min intervals
    Plus: primary_gpu_hours, backfill_gpu_hours, total_gpu_hours convenience columns.
    """
    all_bucket_stats = []

    for db_path in db_paths:
        df = load_data_from_database(db_path)
        if len(df) == 0:
            continue
        print(f"  Computing bucket stats for {db_path}...")
        stats = _compute_bucket_stats(df)
        all_bucket_stats.append(stats)
        del df

    if not all_bucket_stats:
        return pl.DataFrame()

    all_stats = pl.concat(all_bucket_stats)
    del all_bucket_stats

    # Filter to complete weeks (7 distinct days of data)
    days_per_week = (
        all_stats.with_columns(pl.col("bucket").dt.date().alias("day"))
        .group_by("week_start")
        .agg(pl.col("day").n_unique().alias("n_days"))
    )
    complete_weeks = days_per_week.filter(pl.col("n_days") >= 7)["week_start"].to_list()
    all_stats = all_stats.filter(pl.col("week_start").is_in(complete_weeks))

    if len(all_stats) == 0:
        return pl.DataFrame()

    # Aggregate per week per category
    weekly = (
        all_stats.group_by(["week_start", "category"]).agg(
            [
                pl.col("total_gpus").sum().alias("total_gpus"),
                pl.col("claimed_gpus").sum().alias("claimed_gpus"),
                pl.len().alias("intervals"),
            ]
        )
    ).with_columns(
        [
            # Allocation %
            pl.when(pl.col("total_gpus") > 0)
            .then(pl.col("claimed_gpus") / pl.col("total_gpus") * 100.0)
            .otherwise(0.0)
            .alias("pct"),
            # Avg GPUs per interval
            (pl.col("total_gpus") / pl.col("intervals")).alias("avg_total_gpus"),
            (pl.col("claimed_gpus") / pl.col("intervals")).alias("avg_claimed_gpus"),
            # GPU-hours: each interval is 15 min = 0.25 hours
            (pl.col("claimed_gpus") * 0.25).alias("gpu_hours"),
        ]
    )

    # Pivot categories into columns
    weekly_pivoted = weekly.pivot(
        on="category",
        index="week_start",
        values=["pct", "intervals", "avg_total_gpus", "avg_claimed_gpus", "gpu_hours"],
    )

    # Rename pivoted columns
    rename_map = {}
    for cat in ["prioritized", "open_capacity", "backfill"]:
        for prefix, suffix in [
            ("pct", "pct"),
            ("intervals", "intervals"),
            ("avg_total_gpus", "avg_total"),
            ("avg_claimed_gpus", "avg_claimed"),
            ("gpu_hours", "gpu_hours"),
        ]:
            col = f"{prefix}_{cat}"
            if col in weekly_pivoted.columns:
                rename_map[col] = f"{cat}_{suffix}"

    weekly_pivoted = weekly_pivoted.rename(rename_map)

    # Fill missing categories
    for cat in ["prioritized", "open_capacity", "backfill"]:
        for suffix in ["pct", "intervals", "avg_total", "avg_claimed", "gpu_hours"]:
            col = f"{cat}_{suffix}"
            if col not in weekly_pivoted.columns:
                weekly_pivoted = weekly_pivoted.with_columns(pl.lit(0.0).alias(col))

    # Add convenience totals for the GPU-hours plot
    weekly_pivoted = weekly_pivoted.with_columns(
        [
            (pl.col("prioritized_gpu_hours") + pl.col("open_capacity_gpu_hours")).alias("primary_gpu_hours"),
            pl.col("backfill_gpu_hours").alias("backfill_gpu_hours_total"),
            (pl.col("prioritized_gpu_hours") + pl.col("open_capacity_gpu_hours") + pl.col("backfill_gpu_hours")).alias(
                "total_gpu_hours"
            ),
        ]
    )

    weekly_pivoted = weekly_pivoted.sort("week_start")
    return weekly_pivoted


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def create_plots(weekly_df: pl.DataFrame, output_path: str = None, show_plot: bool = True):
    """Create side-by-side GPU hours and allocation percentage plots."""
    if not MATPLOTLIB_AVAILABLE:
        print("Error: matplotlib is required for plotting. Install with: pip install matplotlib")
        return

    if len(weekly_df) == 0:
        print("No data available for plotting")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 6))

    dates = weekly_df["week_start"].to_list()

    # --- Left plot: Weekly GPU Hours ---
    prioritized_hours = weekly_df["prioritized_gpu_hours"].to_list()
    open_cap_hours = weekly_df["open_capacity_gpu_hours"].to_list()
    backfill_hours = weekly_df["backfill_gpu_hours_total"].to_list()
    total_hours = weekly_df["total_gpu_hours"].to_list()

    ax1.plot(dates, prioritized_hours, color=COLOR_PRIMARY, linewidth=2, marker="o", markersize=3, label="Prioritized")
    ax1.plot(
        dates, open_cap_hours, color=COLOR_OPEN_CAPACITY, linewidth=2, marker="s", markersize=3, label="Open Capacity"
    )
    ax1.plot(dates, backfill_hours, color=COLOR_BACKFILL, linewidth=2, marker="^", markersize=3, label="Backfill")
    ax1.plot(dates, total_hours, color=COLOR_TOTAL, linestyle="--", linewidth=1, alpha=0.8, label="Total")

    ax1.set_title("Weekly GPU Hours Over Time")
    ax1.set_xlabel("Week Starting")
    ax1.set_ylabel("GPU Hours")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    # --- Right plot: Allocation % ---
    ax2.plot(
        dates,
        weekly_df["prioritized_pct"].to_list(),
        color=COLOR_PRIMARY,
        linewidth=2,
        marker="o",
        markersize=3,
        label="Prioritized",
    )
    ax2.plot(
        dates,
        weekly_df["open_capacity_pct"].to_list(),
        color=COLOR_OPEN_CAPACITY,
        linewidth=2,
        marker="s",
        markersize=3,
        label="Open Capacity",
    )
    ax2.plot(
        dates,
        weekly_df["backfill_pct"].to_list(),
        color=COLOR_BACKFILL,
        linewidth=2,
        marker="^",
        markersize=3,
        label="Backfill",
    )

    ax2.set_title("Weekly GPU Allocation Percentage by Category")
    ax2.set_xlabel("Week Starting")
    ax2.set_ylabel("Allocation %")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Plot saved to: {output_path}")

    if show_plot:
        try:
            plt.show()
        except Exception:
            print("Display not available - plot saved to file only")


# ---------------------------------------------------------------------------
# Summary printing
# ---------------------------------------------------------------------------


def print_summary(weekly_df: pl.DataFrame):
    """Print summary statistics for both GPU hours and allocation."""
    if len(weekly_df) == 0:
        print("No data to summarize")
        return

    week_starts = weekly_df["week_start"]
    print(f"\nDate Range: {str(week_starts.min())[:10]} to {str(week_starts.max())[:10]}")
    print(f"Complete Weeks: {len(weekly_df)}")

    # GPU Hours summary
    print("\n" + "=" * 70)
    print("GPU HOURS SUMMARY (weekly)")
    print("=" * 70)
    for label, col in [
        ("Prioritized", "prioritized_gpu_hours"),
        ("Open Capacity", "open_capacity_gpu_hours"),
        ("Backfill", "backfill_gpu_hours_total"),
        ("Total", "total_gpu_hours"),
    ]:
        print(
            f"  {label:<14}  Avg: {weekly_df[col].mean():>8,.0f}   "
            f"Min: {weekly_df[col].min():>8,.0f}   Max: {weekly_df[col].max():>8,.0f}"
        )

    # Allocation summary
    print("\n" + "=" * 70)
    print("ALLOCATION PERCENTAGE SUMMARY")
    print("=" * 70)
    for label, col in [
        ("Prioritized", "prioritized_pct"),
        ("Open Capacity", "open_capacity_pct"),
        ("Backfill", "backfill_pct"),
    ]:
        print(
            f"  {label:<14}  Avg: {weekly_df[col].mean():>5.1f}%   "
            f"Min: {weekly_df[col].min():>5.1f}%   Max: {weekly_df[col].max():>5.1f}%"
        )

    # Weekly breakdown table
    print("\n" + "=" * 120)
    print("WEEKLY BREAKDOWN")
    print("-" * 120)
    print(
        f"{'Week':<12} {'Prio Hrs':>9} {'OpenCap Hrs':>12} {'Backfill Hrs':>13} {'Total Hrs':>10}"
        f"  {'Prio %':>7} {'OC %':>7} {'BF %':>7}  {'BF Pool':>12}"
    )
    print("-" * 120)
    for row in weekly_df.iter_rows(named=True):
        week_str = str(row["week_start"])[:10]
        print(
            f"{week_str:<12}"
            f" {row['prioritized_gpu_hours']:>8,.0f}"
            f" {row['open_capacity_gpu_hours']:>11,.0f}"
            f" {row['backfill_gpu_hours_total']:>12,.0f}"
            f" {row['total_gpu_hours']:>10,.0f}"
            f"  {row['prioritized_pct']:>6.1f}%"
            f" {row['open_capacity_pct']:>6.1f}%"
            f" {row['backfill_pct']:>6.1f}%"
            f"  {row['backfill_avg_claimed']:>4.0f}/{row['backfill_avg_total']:>4.0f}"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Weekly GPU summary: hours and allocation percentage side by side")
    parser.add_argument(
        "--databases",
        "-d",
        nargs="+",
        default=[
            "gpu_state_2025-10.db",
            "gpu_state_2025-11.db",
            "gpu_state_2025-12.db",
            "gpu_state_2026-01.db",
        ],
        help="Database files to analyze",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="weekly_summary.png",
        help="Output plot file path (default: weekly_summary.png)",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Skip generating the plot, only print summary",
    )
    parser.add_argument(
        "--csv",
        help="Output CSV file path for weekly data",
    )

    args = parser.parse_args()

    print("Weekly GPU Summary")
    print("=" * 50)
    print(f"Databases: {', '.join(args.databases)}")

    weekly_df = compute_weekly_stats(args.databases)

    if len(weekly_df) == 0:
        print("Error: No complete weeks of data found")
        sys.exit(1)

    print_summary(weekly_df)

    if args.csv:
        weekly_df.write_csv(args.csv)
        print(f"\nCSV saved to: {args.csv}")

    if not args.no_plot:
        create_plots(weekly_df, args.output)


if __name__ == "__main__":
    main()
