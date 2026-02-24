#!/usr/bin/env python3
"""
Weekly Allocation Percentage Plot

Plots allocation percentages over time grouped by week for:
- Prioritized total (Priority-ResearcherOwned + Priority-CHTCOwned)
- Open Capacity total (Shared slots)
- Backfill total (all Backfill-* types)
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
from gpu_utils_polars import HOST_EXCLUSIONS, load_chtc_owned_hosts  # noqa: I001


NEEDED_COLUMNS = ["Name", "AssignedGPUs", "State", "Machine", "PrioritizedProjects", "GPUs_DeviceName", "timestamp"]
OLD_GPU_TYPES = ["GTX 1080", "P100", "Quadro", "A30", "A40"]
NEEDED_COLUMNS_SQL = ", ".join(NEEDED_COLUMNS)


def load_data_from_database(db_path: str) -> pl.DataFrame:
    """Load data from a single database file, selecting only needed columns."""
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
    Vectorized duplicate cleanup: for rows where the same GPU appears multiple times
    in the same timestamp, keep the highest-priority version.
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
    Classify each row into a slot_type category and an effective state,
    replicating the logic from filter_df_enhanced but vectorized.

    Returns the dataframe with added columns: slot_type, effective_state
    """
    chtc_owned_hosts = load_chtc_owned_hosts()
    chtc_list = list(chtc_owned_hosts)

    is_backfill = pl.col("Name").str.contains("backfill")
    is_chtc = pl.col("Machine").is_in(chtc_list)

    # Apply host exclusions
    if HOST_EXCLUSIONS:
        for excluded_host in HOST_EXCLUSIONS.keys():
            df = df.filter(~pl.col("Machine").str.contains(f"(?i){excluded_host}").fill_null(False))

    # Filter out old/uncommon GPU types and null device names (same as usage_stats)
    old_pattern = "|".join(OLD_GPU_TYPES)
    df = df.filter(
        pl.col("GPUs_DeviceName").is_not_null()
        & (pl.col("GPUs_DeviceName") != "")
        & ~pl.col("GPUs_DeviceName").str.contains(old_pattern).fill_null(False)
    )

    # Shared requires PrioritizedProjects == "" (not null) to match filter_df_enhanced
    has_priority_not_null = (pl.col("PrioritizedProjects") != "") & pl.col("PrioritizedProjects").is_not_null()
    priority_is_empty = pl.col("PrioritizedProjects") == ""
    priority_is_null_or_empty = (pl.col("PrioritizedProjects") == "") | pl.col("PrioritizedProjects").is_null()

    # Step 1: Classify each row into its slot_type
    # Match filter_df_enhanced logic exactly:
    #   Backfill-ResearcherOwned: backfill & has_priority & !chtc
    #   Backfill-CHTCOwned: backfill & chtc (no priority check)
    #   Backfill-OpenCapacity: backfill & (priority=="" or null) & !chtc
    #   Priority-ResearcherOwned: !backfill & has_priority & !chtc
    #   Priority-CHTCOwned: !backfill & has_priority & chtc
    #   Shared: !backfill & priority=="" (NOT null)
    df = df.with_columns(
        pl.when(is_backfill & has_priority_not_null & ~is_chtc)
        .then(pl.lit("Backfill-ResearcherOwned"))
        .when(is_backfill & is_chtc)
        .then(pl.lit("Backfill-CHTCOwned"))
        .when(is_backfill & priority_is_null_or_empty & ~is_chtc)
        .then(pl.lit("Backfill-OpenCapacity"))
        .when(~is_backfill & has_priority_not_null & ~is_chtc)
        .then(pl.lit("Priority-ResearcherOwned"))
        .when(~is_backfill & has_priority_not_null & is_chtc)
        .then(pl.lit("Priority-CHTCOwned"))
        .when(~is_backfill & priority_is_empty)
        .then(pl.lit("Shared"))
        .otherwise(pl.lit("Other"))
        .alias("slot_type")
    )

    # Step 2: Apply duplicate cleanup for priority/shared primary slots.
    # The original logic deduplicates across ALL rows (primary+backfill) within a timestamp,
    # then filters to just primary slots for Claimed, or primary-unclaimed + backfill-claimed for Unclaimed.
    #
    # For the weekly allocation calc, what matters is:
    #   - "Claimed" count = unique GPUs in claimed primary slots (after dedup)
    #   - "Total" count = claimed + unique GPUs that are unclaimed in primary
    #     (where "unclaimed" means: primary slot is unclaimed AND not claimed in backfill)
    #
    # The dedup logic keeps the highest-ranked row per (timestamp, GPU), so if a GPU is
    # claimed in backfill but unclaimed in primary, the claimed-backfill row wins.
    # For Priority/Shared "Unclaimed" counting, the original code also counts
    # GPUs that are claimed in backfill (condition2 in filter_df_enhanced).
    # This means: "Unclaimed from prioritized/shared perspective" = not being used for priority/shared.
    #
    # Simplification: after dedup, for priority/shared primary slots:
    #   - If State=Claimed and not backfill -> truly claimed for priority/shared
    #   - If the GPU only appears in backfill (claimed) -> it's "unclaimed" from priority/shared perspective
    #   - If State=Unclaimed and not backfill -> truly unclaimed
    #
    # For the percentage calc (claimed / total), we need:
    #   Claimed = unique GPUs where primary slot is Claimed
    #   Total = all unique GPUs that have a primary slot (claimed or unclaimed)

    # We apply dedup only for counting purposes. The slot_type classification is already done.
    # The dedup affects which GPUs count as "claimed" vs "unclaimed" for Priority/Shared.
    # For backfill, no dedup is needed.

    return df


def _compute_bucket_stats(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute per-bucket (15-min) stats for a single database's worth of data.
    Returns a small DataFrame with columns: week_start, bucket, category, total_gpus, claimed_gpus.
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

    # Classify all rows
    df = classify_rows(df)

    # Add time buckets
    df = df.with_columns(
        [
            pl.col("timestamp").dt.truncate("1w").alias("week_start"),
            pl.col("timestamp").dt.truncate("15m").alias("bucket"),
        ]
    )

    # Map slot_types to categories
    category_map = {
        "Priority-ResearcherOwned": "prioritized",
        "Priority-CHTCOwned": "prioritized",
        "Shared": "open_capacity",
        "Backfill-ResearcherOwned": "backfill",
        "Backfill-CHTCOwned": "backfill",
        "Backfill-OpenCapacity": "backfill",
    }

    df = df.with_columns(pl.col("slot_type").replace_strict(category_map, default="other").alias("category"))

    # Filter to only the categories we care about
    df = df.filter(pl.col("category") != "other")

    # Apply dedup to determine GPU states
    full_deduped = _apply_duplicate_cleanup(df.filter(pl.col("AssignedGPUs").is_not_null()))
    primary_deduped = full_deduped.filter(pl.col("category") != "backfill")

    # Primary categories: total GPUs from pre-dedup, claimed from post-dedup
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

    # Backfill: simpler
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


def calculate_weekly_allocation_incremental(db_paths: list[str]) -> pl.DataFrame:
    """
    Calculate weekly allocation percentages by processing one database at a time.
    Each DB is loaded, aggregated to bucket-level stats, then freed before the next.
    """
    all_bucket_stats = []

    for db_path in db_paths:
        df = load_data_from_database(db_path)
        if len(df) == 0:
            continue
        print(f"  Computing bucket stats for {db_path}...")
        stats = _compute_bucket_stats(df)
        all_bucket_stats.append(stats)
        del df  # free memory before loading next DB

    if not all_bucket_stats:
        return pl.DataFrame()

    # Combine the small bucket-level stats from all DBs
    all_stats = pl.concat(all_bucket_stats)
    del all_bucket_stats

    # Count distinct days per week to filter partial weeks (must have all 7 days, like gpu_hours)
    days_per_week = (
        all_stats.with_columns(pl.col("bucket").dt.date().alias("day"))
        .group_by("week_start")
        .agg(pl.col("day").n_unique().alias("n_days"))
    )
    complete_weeks = days_per_week.filter(pl.col("n_days") >= 7)["week_start"].to_list()
    all_stats = all_stats.filter(pl.col("week_start").is_in(complete_weeks))

    # Sum counts across buckets per week (matching usage_stats: sum totals, then divide)
    weekly = (
        all_stats.group_by(["week_start", "category"]).agg(
            [
                pl.col("total_gpus").sum().alias("total_gpus"),
                pl.col("claimed_gpus").sum().alias("claimed_gpus"),
                pl.len().alias("intervals"),
            ]
        )
    ).with_columns(
        pl.when(pl.col("total_gpus") > 0)
        .then(pl.col("claimed_gpus") / pl.col("total_gpus") * 100.0)
        .otherwise(0.0)
        .alias("avg_pct")
    )

    # Also compute average GPUs per interval for context
    weekly = weekly.with_columns(
        [
            (pl.col("total_gpus") / pl.col("intervals")).alias("avg_total_gpus"),
            (pl.col("claimed_gpus") / pl.col("intervals")).alias("avg_claimed_gpus"),
        ]
    )

    # Pivot to get one row per week with columns for each category
    weekly_pivoted = weekly.pivot(
        on="category",
        index="week_start",
        values=["avg_pct", "intervals", "avg_total_gpus", "avg_claimed_gpus"],
    )

    # Rename columns to match expected output
    rename_map = {}
    for cat in ["prioritized", "open_capacity", "backfill"]:
        for prefix, suffix in [
            ("avg_pct", "pct"),
            ("intervals", "intervals"),
            ("avg_total_gpus", "avg_total"),
            ("avg_claimed_gpus", "avg_claimed"),
        ]:
            col = f"{prefix}_{cat}"
            if col in weekly_pivoted.columns:
                rename_map[col] = f"{cat}_{suffix}"

    weekly_pivoted = weekly_pivoted.rename(rename_map)

    # Fill missing categories with 0
    for cat in ["prioritized", "open_capacity", "backfill"]:
        for suffix in ["pct", "intervals", "avg_total", "avg_claimed"]:
            col = f"{cat}_{suffix}"
            if col not in weekly_pivoted.columns:
                weekly_pivoted = weekly_pivoted.with_columns(pl.lit(0.0).alias(col))

    weekly_pivoted = weekly_pivoted.sort("week_start")

    return weekly_pivoted


def create_plot(weekly_df: pl.DataFrame, output_path: str = None, show_plot: bool = True):
    """Create the weekly allocation percentage plot."""
    if not MATPLOTLIB_AVAILABLE:
        print("Error: matplotlib is required for plotting. Install with: pip install matplotlib")
        return

    if len(weekly_df) == 0:
        print("No data available for plotting")
        return

    fig, ax = plt.subplots(1, 1, figsize=(12, 6))

    # Convert to Python datetimes for matplotlib
    dates = weekly_df["week_start"].to_list()

    ax.plot(
        dates,
        weekly_df["prioritized_pct"].to_list(),
        color="#2E86AB",
        linewidth=2,
        marker="o",
        markersize=3,
        label="Prioritized",
    )
    ax.plot(
        dates,
        weekly_df["open_capacity_pct"].to_list(),
        color="#F18F01",
        linewidth=2,
        marker="s",
        markersize=3,
        label="Open Capacity",
    )
    ax.plot(
        dates,
        weekly_df["backfill_pct"].to_list(),
        color="#A23B72",
        linewidth=2,
        marker="^",
        markersize=3,
        label="Backfill",
    )

    ax.set_title("Weekly GPU Allocation Percentage by Category")
    ax.set_xlabel("Week Starting")
    ax.set_ylabel("Allocation %")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Plot saved to: {output_path}")

    if show_plot:
        try:
            plt.show()
        except Exception:
            print("Display not available - plot saved to file only")


def print_summary(weekly_df: pl.DataFrame):
    """Print summary statistics."""
    if len(weekly_df) == 0:
        print("No data to summarize")
        return

    print("\n" + "=" * 70)
    print("WEEKLY ALLOCATION PERCENTAGE SUMMARY")
    print("=" * 70)

    week_starts = weekly_df["week_start"]
    print(f"Date Range: {week_starts.min()} to {week_starts.max()}")
    print(f"Total Weeks: {len(weekly_df)}")
    print()

    for category in ["prioritized", "open_capacity", "backfill"]:
        col = f"{category}_pct"
        print(f"{category.replace('_', ' ').title()}:")
        print(f"  Average: {weekly_df[col].mean():.1f}%")
        print(f"  Min:     {weekly_df[col].min():.1f}%")
        print(f"  Max:     {weekly_df[col].max():.1f}%")
        print()

    # Print weekly breakdown
    print("WEEKLY BREAKDOWN (allocation % and avg claimed/total GPUs per interval):")
    print("-" * 100)
    print(f"{'Week Starting':<14} {'Prioritized':>12} {'Open Capacity':>14} {'Backfill':>10}   {'Backfill Pool':>14}")
    print("-" * 100)
    for row in weekly_df.iter_rows(named=True):
        week_str = str(row["week_start"])[:10]
        print(
            f"{week_str:<14} "
            f"{row['prioritized_pct']:>11.1f}% "
            f"{row['open_capacity_pct']:>13.1f}% "
            f"{row['backfill_pct']:>9.1f}%"
            f"   {row['backfill_avg_claimed']:>5.0f}/{row['backfill_avg_total']:>5.0f} GPUs"
        )


def main():
    parser = argparse.ArgumentParser(description="Plot weekly GPU allocation percentages by category")
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
        default="weekly_allocation.png",
        help="Output plot file path (default: weekly_allocation.png)",
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

    print("Weekly Allocation Percentage Analysis")
    print("=" * 50)
    print(f"Analyzing databases: {', '.join(args.databases)}")
    print()

    # Calculate weekly stats (processes one DB at a time to limit memory)
    print("Calculating weekly allocation percentages...")
    weekly_df = calculate_weekly_allocation_incremental(args.databases)

    if len(weekly_df) == 0:
        print("Error: Could not calculate weekly statistics")
        sys.exit(1)

    print(f"Complete weeks found: {len(weekly_df)}")

    # Print summary
    print_summary(weekly_df)

    # Save to CSV if requested
    if args.csv:
        weekly_df.write_csv(args.csv)
        print(f"\nCSV saved to: {args.csv}")

    # Create plot
    if not args.no_plot:
        create_plot(weekly_df, args.output)


if __name__ == "__main__":
    main()
