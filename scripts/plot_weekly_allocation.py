#!/usr/bin/env python3
"""
Weekly Allocation Percentage Plot

Plots weekly GPU allocation percentages from the gpu_state Parquet files for
the three report headline categories:
- Prioritized (Primary) (Priority-ResearcherOwned + Priority-CHTCOwned real slots)
- Open Capacity (Shared real slots)
- Secondary/Backfill (Backfill-ResearcherOwned + Backfill-CHTCOwned slots)

A week's percentage is the mean over its 15-minute buckets of
claimed / total unique GPUs, matching the report methodology. Partial weeks
(fewer than 7 distinct days of data) are dropped.
"""

import argparse
import datetime
import sys
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

import gpu_utils  # noqa: E402
from gpu_utils import load_host_exclusions  # noqa: E402
from stats_calculations import (  # noqa: E402
    _REAL_CLASS_EXPRS,
    OLD_GPU_TYPES,
    PreparedFrames,
    _researcher_scope,
    prepare_frames,
)
from stats_data import get_latest_timestamp, scan_time_filtered  # noqa: E402

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e5e4e0"

# Categorical slots 1-3 (validated: scripts/validate_palette.js, light mode)
CATEGORIES = [
    ("Prioritized (Primary)", "#2a78d6"),
    ("Open Capacity", "#1baf7a"),
    ("Secondary (Backfill)", "#eda100"),
]


def _category_frames(frames: PreparedFrames) -> dict[str, pl.LazyFrame]:
    """Class frames for the three headline categories, old devices excluded."""
    keep_device = (
        pl.col("GPUs_DeviceName").is_not_null()
        & (pl.col("GPUs_DeviceName") != "")
        & ~pl.col("GPUs_DeviceName").str.contains("|".join(OLD_GPU_TYPES)).fill_null(False)
    )
    prioritized = frames.dedup.filter(
        (_REAL_CLASS_EXPRS["Priority-ResearcherOwned"] | _REAL_CLASS_EXPRS["Priority-CHTCOwned"]) & keep_device
    )
    open_capacity = frames.dedup.filter(_REAL_CLASS_EXPRS["Shared"] & keep_device)
    researcher = _researcher_scope(frames, ["Machine"])
    backfill = frames.raw_bf.filter(keep_device).filter(
        pl.col("_is_chtc") | pl.col("Machine").is_in(researcher.collect()["Machine"].implode())
    )
    return {
        "Prioritized (Primary)": prioritized,
        "Open Capacity": open_capacity,
        "Secondary (Backfill)": backfill,
    }


def weekly_allocation(frames: PreparedFrames) -> pl.DataFrame:
    """Weekly mean of per-bucket allocation percentages per category."""
    weeklies = []
    for name, frame in _category_frames(frames).items():
        per_bucket = (
            frame.filter(pl.col("AssignedGPUs").is_not_null())
            .group_by("bucket", "AssignedGPUs")
            .agg((pl.col("State") == "Claimed").any().alias("claimed"))
            .group_by("bucket")
            .agg(pl.len().alias("total"), pl.col("claimed").sum().alias("claimed"))
            .with_columns(pl.col("bucket").dt.truncate("1w").alias("week_start"))
        )
        weekly = (
            per_bucket.group_by("week_start")
            .agg(
                (pl.col("claimed") / pl.col("total") * 100).mean().alias("pct"),
                (pl.col("claimed").sum() / pl.len()).alias("avg_claimed"),
                (pl.col("total").sum() / pl.len()).alias("avg_total"),
                pl.len().alias("intervals"),
                pl.col("bucket").dt.date().n_unique().alias("n_days"),
            )
            .filter(pl.col("n_days") >= 7)
            .with_columns(pl.lit(name).alias("category"))
        )
        weeklies.append(weekly.collect(engine="streaming"))
    return pl.concat(weeklies).sort(["category", "week_start"])


def create_plot(weekly: pl.DataFrame, output_path: str, title_period: str):
    fig, ax = plt.subplots(figsize=(12, 6.5), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    # Break lines at weeks dropped for incomplete data instead of bridging the gap
    first_week = weekly["week_start"].min()
    last_week = weekly["week_start"].max()
    n_weeks = (last_week - first_week).days // 7 + 1
    all_weeks = [first_week + datetime.timedelta(weeks=i) for i in range(n_weeks)]

    for name, color in CATEGORIES:
        cat = weekly.filter(pl.col("category") == name)
        by_week = dict(zip(cat["week_start"].to_list(), cat["pct"].to_list(), strict=True))
        pcts = [by_week.get(week, float("nan")) for week in all_weeks]
        ax.plot(all_weeks, pcts, color=color, linewidth=2, solid_capstyle="round", label=name)
        last_present = max(w for w in by_week if by_week[w] == by_week[w])
        # Direct label at the line end (required relief for the low-contrast slots)
        ax.annotate(
            name,
            xy=(last_present, by_week[last_present]),
            xytext=(8, 0),
            textcoords="offset points",
            va="center",
            fontsize=9.5,
            color=TEXT_PRIMARY,
        )

    ax.set_ylim(0, 100)
    ax.set_yticks(range(0, 101, 20))
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=9)

    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    # Room on the right for the direct labels
    last_week = weekly["week_start"].max()
    ax.set_xlim(right=last_week + datetime.timedelta(days=42))

    ax.set_title(
        f"Weekly GPU allocation, {title_period}",
        loc="left",
        fontsize=14,
        color=TEXT_PRIMARY,
        pad=18,
        fontweight="bold",
    )
    ax.text(
        0,
        1.015,
        "Share of available GPUs claimed, averaged over each week",
        transform=ax.transAxes,
        fontsize=10,
        color=TEXT_SECONDARY,
    )
    ax.legend(loc="lower right", frameon=False, fontsize=9.5, labelcolor=TEXT_SECONDARY)

    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches="tight", facecolor=SURFACE)
    print(f"Plot saved to: {output_path}")


def print_summary(weekly: pl.DataFrame):
    print(f"\nWeeks: {weekly['week_start'].n_unique()}  ({weekly['week_start'].min()} to {weekly['week_start'].max()})")
    for name, _ in CATEGORIES:
        cat = weekly.filter(pl.col("category") == name)
        print(f"{name}: avg {cat['pct'].mean():.1f}%  min {cat['pct'].min():.1f}%  max {cat['pct'].max():.1f}%")


def _parse_date(value: str) -> datetime.datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")


def main():
    parser = argparse.ArgumentParser(description="Plot weekly GPU allocation percentages by category")
    parser.add_argument("--data-dir", default=".", help="Directory containing gpu_state_*.parquet files")
    parser.add_argument("--start", type=_parse_date, default=None, help="Window start, default: one year before end")
    parser.add_argument("--end", type=_parse_date, default=None, help="Window end, default: latest data")
    parser.add_argument("--exclude-hosts-yaml", default="masked_hosts.yaml", help="Host exclusions YAML")
    parser.add_argument("--output", "-o", default="weekly_allocation.png", help="Output plot file path")
    parser.add_argument("--csv", help="Optional CSV output path for the weekly data")
    parser.add_argument("--no-plot", action="store_true", help="Skip the plot, only print the summary")
    args = parser.parse_args()

    end = args.end or get_latest_timestamp(args.data_dir)
    if end is None:
        print(f"Error: no gpu_state Parquet files found in {args.data_dir}")
        sys.exit(1)
    start = args.start or end - datetime.timedelta(days=365)
    if start >= end:
        print(f"Error: start {start} is not before end {end}")
        sys.exit(1)
    gpu_utils.HOST_EXCLUSIONS = load_host_exclusions(None, args.exclude_hosts_yaml)

    print(f"Preparing window {start} – {end} from {args.data_dir}...")
    hours_back = (end - start).total_seconds() / 3600
    frames = prepare_frames(scan_time_filtered(args.data_dir, hours_back, end))
    if frames.original_count == 0:
        print("Error: no data found in the specified time range")
        sys.exit(1)

    weekly = weekly_allocation(frames)
    print_summary(weekly)

    if args.csv:
        weekly.write_csv(args.csv)
        print(f"CSV saved to: {args.csv}")

    if not args.no_plot:
        period = f"{frames.start_time:%b %Y} – {frames.end_time:%b %Y}"
        create_plot(weekly, args.output, period)


if __name__ == "__main__":
    main()
