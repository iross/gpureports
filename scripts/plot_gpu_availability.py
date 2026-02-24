#!/usr/bin/env python3
"""
GPU Availability by Category Plot

Plots claimed and unclaimed GPU counts over time for each slot category:
  - Prioritized (Priority-ResearcherOwned + Priority-CHTCOwned)
  - Open Capacity (Shared slots)
  - Backfill (all Backfill-* types)

Supports filtering by host and a configurable time window (default: 1 week).
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

try:
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

sys.path.insert(0, str(Path(__file__).parent.parent))
from gpu_utils_polars import (
    HOST_EXCLUSIONS,
    get_latest_timestamp_from_most_recent_db,
    get_required_databases,
    load_chtc_owned_hosts,
    load_host_exclusions,
)

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

CATEGORY_MAP = {
    "Priority-ResearcherOwned": "prioritized",
    "Priority-CHTCOwned": "prioritized",
    "Shared": "open_capacity",
    "Backfill-ResearcherOwned": "backfill",
    "Backfill-CHTCOwned": "backfill",
    "Backfill-OpenCapacity": "backfill",
}

CATEGORY_COLORS = {
    "prioritized": "#2E86AB",
    "open_capacity": "#F18F01",
    "backfill": "#A23B72",
}

CATEGORY_LABELS = {
    "prioritized": "Prioritized",
    "open_capacity": "Open Capacity",
    "backfill": "Backfill",
}


def load_data(db_path: str, start_time: datetime, end_time: datetime) -> pl.DataFrame:
    """Load needed columns from a single database file within the time range."""
    if not Path(db_path).exists():
        print(f"Warning: {db_path} not found, skipping")
        return pl.DataFrame()

    try:
        conn = sqlite3.connect(db_path)
        query = (
            f"SELECT {NEEDED_COLUMNS_SQL} FROM gpu_state "
            f"WHERE timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' "
            f"AND timestamp <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
        )
        df = pl.read_database(query, conn)
        conn.close()

        if len(df) > 0:
            print(f"  Loaded {len(df):,} rows from {db_path}")
            if df["timestamp"].dtype == pl.Utf8:
                df = df.with_columns(pl.col("timestamp").str.to_datetime())

        return df
    except Exception as e:
        print(f"Error loading {db_path}: {e}")
        return pl.DataFrame()


def _apply_dedup(df: pl.DataFrame) -> pl.DataFrame:
    """
    Keep the highest-priority row per (timestamp, GPU).
    Rank: claimed+primary(3) > claimed+backfill(2) > unclaimed+primary(1) > unclaimed+backfill(0).
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
    return df.drop("_rank")


def compute_bucket_stats(
    df: pl.DataFrame,
    host: str = "",
    bucket_minutes: int = 15,
) -> pl.DataFrame:
    """
    Compute claimed and total GPU counts per category per time bucket.

    Returns a DataFrame with columns:
        bucket, category, total_gpus, claimed_gpus
    where unclaimed = total_gpus - claimed_gpus.
    """
    if len(df) == 0:
        return pl.DataFrame(
            schema={
                "bucket": pl.Datetime,
                "category": pl.Utf8,
                "total_gpus": pl.UInt32,
                "claimed_gpus": pl.UInt32,
            }
        )

    # Apply global host exclusions
    for excluded_host in HOST_EXCLUSIONS:
        df = df.filter(~pl.col("Machine").str.contains(f"(?i){excluded_host}").fill_null(False))

    # Apply user host filter (comma-separated list of substrings, OR logic)
    if host:
        hosts = [h.strip() for h in host.split(",") if h.strip()]
        pattern = "|".join(hosts)
        df = df.filter(pl.col("Machine").str.contains(pattern))

    # Filter out old/uncommon GPU types
    old_pattern = "|".join(OLD_GPU_TYPES)
    df = df.filter(
        pl.col("GPUs_DeviceName").is_not_null()
        & (pl.col("GPUs_DeviceName") != "")
        & ~pl.col("GPUs_DeviceName").str.contains(old_pattern).fill_null(False)
    )

    chtc_list = list(load_chtc_owned_hosts())
    is_backfill = pl.col("Name").str.contains("backfill")
    is_chtc = pl.col("Machine").is_in(chtc_list)
    has_priority = (pl.col("PrioritizedProjects") != "") & pl.col("PrioritizedProjects").is_not_null()
    priority_empty = pl.col("PrioritizedProjects") == ""
    priority_null_or_empty = (pl.col("PrioritizedProjects") == "") | pl.col("PrioritizedProjects").is_null()

    df = df.with_columns(
        pl.when(is_backfill & has_priority & ~is_chtc)
        .then(pl.lit("Backfill-ResearcherOwned"))
        .when(is_backfill & is_chtc)
        .then(pl.lit("Backfill-CHTCOwned"))
        .when(is_backfill & priority_null_or_empty & ~is_chtc)
        .then(pl.lit("Backfill-OpenCapacity"))
        .when(~is_backfill & has_priority & ~is_chtc)
        .then(pl.lit("Priority-ResearcherOwned"))
        .when(~is_backfill & has_priority & is_chtc)
        .then(pl.lit("Priority-CHTCOwned"))
        .when(~is_backfill & priority_empty)
        .then(pl.lit("Shared"))
        .otherwise(pl.lit("Other"))
        .alias("slot_type")
    )

    df = df.with_columns(pl.col("slot_type").replace_strict(CATEGORY_MAP, default="other").alias("category")).filter(
        pl.col("category") != "other"
    )

    bucket_expr = f"{bucket_minutes}m"
    df = df.with_columns(pl.col("timestamp").dt.truncate(bucket_expr).alias("bucket"))

    # Primary categories: dedup across all slots, then count from primary slots only
    primary_df = df.filter(pl.col("category") != "backfill")
    full_deduped = _apply_dedup(df.filter(pl.col("AssignedGPUs").is_not_null()))
    primary_deduped = full_deduped.filter(pl.col("category") != "backfill")

    primary_totals = (
        primary_df.filter(pl.col("AssignedGPUs").is_not_null())
        .group_by(["bucket", "category"])
        .agg(pl.col("AssignedGPUs").n_unique().alias("total_gpus"))
    )
    primary_claimed = (
        primary_deduped.filter((pl.col("State") == "Claimed") & pl.col("AssignedGPUs").is_not_null())
        .group_by(["bucket", "category"])
        .agg(pl.col("AssignedGPUs").n_unique().alias("claimed_gpus"))
    )
    primary_stats = primary_totals.join(primary_claimed, on=["bucket", "category"], how="left").with_columns(
        pl.col("claimed_gpus").fill_null(0)
    )

    # Backfill: straightforward unique-GPU count
    backfill_stats = (
        df.filter((pl.col("category") == "backfill") & pl.col("AssignedGPUs").is_not_null())
        .group_by(["bucket", "category"])
        .agg(
            pl.col("AssignedGPUs").n_unique().alias("total_gpus"),
            pl.col("AssignedGPUs").filter(pl.col("State") == "Claimed").n_unique().alias("claimed_gpus"),
        )
    )

    return pl.concat([primary_stats, backfill_stats]).sort("bucket")


def create_plot(
    stats_df: pl.DataFrame,
    start_time: datetime,
    end_time: datetime,
    host: str = "",
    output_path: str | None = None,
    show_plot: bool = True,
) -> None:
    """
    Create a 3-subplot stacked area plot (one per category).
    Each subplot shows claimed GPUs (solid) stacked under unclaimed (lighter).
    """
    if not MATPLOTLIB_AVAILABLE:
        print("Error: matplotlib is required. Install with: pip install matplotlib")
        return

    if len(stats_df) == 0:
        print("No data available for plotting.")
        return

    categories = ["prioritized", "backfill"]
    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

    # Precompute full bucket timeline so gaps show as zero
    all_buckets = stats_df["bucket"].unique().sort()

    for ax, category in zip(axes, categories, strict=True):
        color = CATEGORY_COLORS[category]
        label = CATEGORY_LABELS[category]

        cat_df = stats_df.filter(pl.col("category") == category)

        if len(cat_df) == 0:
            ax.set_title(f"{label} — no data", fontsize=11)
            ax.set_ylabel("GPUs", fontsize=10)
            continue

        # Join against full timeline to fill gaps with zero
        cat_full = (
            all_buckets.to_frame("bucket")
            .join(cat_df.select(["bucket", "total_gpus", "claimed_gpus"]), on="bucket", how="left")
            .fill_null(0)
            .sort("bucket")
        )

        dates = cat_full["bucket"].to_list()
        claimed = cat_full["claimed_gpus"].to_list()
        unclaimed = [t - c for t, c in zip(cat_full["total_gpus"].to_list(), claimed, strict=True)]

        ax.fill_between(dates, 0, claimed, color=color, alpha=0.8, label="Claimed")
        ax.fill_between(
            dates,
            claimed,
            [c + u for c, u in zip(claimed, unclaimed, strict=True)],
            color=color,
            alpha=0.3,
            label="Unclaimed",
        )
        ax.plot(
            dates,
            [c + u for c, u in zip(claimed, unclaimed, strict=True)],
            color=color,
            linewidth=1,
            alpha=0.6,
        )

        ax.set_ylabel("GPUs", fontsize=10)
        ax.set_title(label, fontsize=11, fontweight="bold")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right", fontsize=9)
        ax.set_ylim(bottom=0)

    # X-axis formatting on the bottom subplot only (shared)
    span_days = (end_time - start_time).days
    if span_days <= 2:
        axes[-1].xaxis.set_major_locator(mdates.HourLocator(interval=3))
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    elif span_days <= 14:
        axes[-1].xaxis.set_major_locator(mdates.DayLocator())
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    else:
        axes[-1].xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=45)
    axes[-1].set_xlabel("Time", fontsize=11)

    title = f"GPU Availability by Category\n{start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}"
    if host:
        title += f"  (hosts: {host})"
    fig.suptitle(title, fontsize=13, fontweight="bold")

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Plot saved to: {output_path}")

    if show_plot:
        try:
            plt.show()
        except Exception:
            print("Display not available — plot saved to file only.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot claimed/unclaimed GPU counts by category (Prioritized, Open Capacity, Backfill)"
    )
    parser.add_argument(
        "--databases",
        "-d",
        nargs="+",
        help="Database files to analyze (auto-detected from date range if not provided)",
    )
    parser.add_argument(
        "--days-back",
        "-n",
        type=int,
        default=7,
        help="Days to look back from end time (default: 7)",
    )
    parser.add_argument(
        "--end-time",
        help="End time: YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS' (default: latest timestamp in DB)",
    )
    parser.add_argument(
        "--host",
        default="",
        help="Filter by host name substring(s), comma-separated (e.g. 'gpu2003,gpu4000')",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="gpu_availability_by_category.png",
        help="Output file path (default: gpu_availability_by_category.png)",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Save to file without displaying interactively",
    )
    parser.add_argument(
        "--bucket-minutes",
        type=int,
        default=15,
        help="Time bucket size in minutes (default: 15)",
    )
    parser.add_argument(
        "--base-dir",
        default=".",
        help="Directory containing database files (default: current directory)",
    )
    parser.add_argument(
        "--exclusions-yaml",
        default="masked_hosts.yaml",
        help="YAML file with host exclusions (default: masked_hosts.yaml)",
    )

    args = parser.parse_args()

    import gpu_utils_polars

    gpu_utils_polars.HOST_EXCLUSIONS = load_host_exclusions(yaml_file=args.exclusions_yaml)

    # Resolve end time
    if args.end_time:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                end_time = datetime.strptime(args.end_time, fmt)
                break
            except ValueError:
                pass
        else:
            print(f"Error: cannot parse --end-time '{args.end_time}'. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS.")
            sys.exit(1)
    else:
        end_time = get_latest_timestamp_from_most_recent_db(args.base_dir)
        if end_time is None:
            print("Error: no database files found and no --end-time specified.")
            sys.exit(1)
        print(f"Using latest DB timestamp as end time: {end_time}")

    start_time = end_time - timedelta(days=args.days_back)
    print(f"Time range: {start_time} to {end_time}")

    # Resolve database files
    if args.databases:
        db_paths = args.databases
    else:
        db_paths = get_required_databases(start_time, end_time, args.base_dir)
        if not db_paths:
            print(f"Error: no database files found in '{args.base_dir}' for this date range.")
            sys.exit(1)
        print(f"Auto-detected databases: {db_paths}")

    # Load data
    frames = [load_data(db, start_time, end_time) for db in db_paths]
    frames = [f for f in frames if len(f) > 0]

    if not frames:
        print("Error: no data loaded.")
        sys.exit(1)

    combined = pl.concat(frames)
    print(f"Total rows loaded: {len(combined):,}")

    print("Computing bucket stats...")
    stats_df = compute_bucket_stats(combined, args.host, args.bucket_minutes)

    if len(stats_df) == 0:
        print("No data after filtering.")
        sys.exit(1)

    for cat in ["prioritized", "open_capacity", "backfill"]:
        cat_data = stats_df.filter(pl.col("category") == cat)
        if len(cat_data) > 0:
            avg_total = cat_data["total_gpus"].mean()
            avg_claimed = cat_data["claimed_gpus"].mean()
            print(f"  {CATEGORY_LABELS[cat]}: avg {avg_claimed:.0f} claimed / {avg_total:.0f} total")

    create_plot(
        stats_df,
        start_time,
        end_time,
        host=args.host,
        output_path=args.output,
        show_plot=not args.no_plot,
    )


if __name__ == "__main__":
    main()
