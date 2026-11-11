#!/usr/bin/env python3
"""
Backfill Fill Plot

Poster figure: how a GPU fleet spends its time, as a weekly stacked area of
GPU counts. --fleet picks the fleet:

prioritized (default): researcher-owned and CHTC-owned priority GPUs
- Owner jobs: a primary (non-backfill) slot holding the GPU is Claimed
- Backfill: not owner-claimed, but a backfill slot holding the GPU is Claimed
- Idle: neither

open: open-capacity GPUs (primary, non-interactive slots with no
PrioritizedProjects)
- Allocated: any slot holding the GPU is Claimed
- Idle: none is

A GPU's state is decided per 15-minute bucket from all of its slot rows, not
from the dedup frame: slot_dedup_rank prefers an unclaimed primary over a
claimed backfill row, which would hide the backfill usage this figure shows.
Weeks with fewer than MIN_DAYS distinct days of data are dropped; other weeks
average over the buckets that have data, so short collector outages do not
blank out a whole week.

The window is aggregated to per-bucket counts in CHUNK-sized slices, so peak
memory scales with the chunk, not the window.
"""

import argparse
import datetime
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

import classify_slots  # noqa: E402
from classify_slots import OLD_GPU_TYPES, derive_columns  # noqa: E402
from read_data import get_latest_timestamp, load_host_exclusions, scan_time_filtered  # noqa: E402

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e5e4e0"

MIN_DAYS = 3
CHUNK = datetime.timedelta(days=7)
IDLE = ("idle", "Idle", "#dcd8cf")

_primary = ~pl.col("_is_bf")
# Rows on a known, current GPU model; OLD_GPU_TYPES are left out of every fleet
KEEP_DEVICE = (
    pl.col("GPUs_DeviceName").is_not_null()
    & (pl.col("GPUs_DeviceName") != "")
    & ~pl.col("GPUs_DeviceName").str.contains("|".join(OLD_GPU_TYPES)).fill_null(False)
)
_owner = pl.col("owner")
_backfill = pl.col("backfill")


def _pct(num: int, den: int) -> float | None:
    return 100 * num / den if den else None


def _totals(counts: pl.DataFrame, keys: list[str]) -> dict[str, int]:
    return {key: int(counts[key].sum()) for key in keys}


def headline_stats(counts: pl.DataFrame) -> dict[str, float | None]:
    """Prioritized-fleet shares, in percent, from per-bucket GPU counts.

    backfill_fill_pct: backfill / (backfill + idle), the share of owner-idle
        GPU time that backfill used.
    allocated_pct: (owner + backfill) / all, the share of prioritized GPU time
        allocated to a job.
    owner_pct: owner / all.

    A share is None when its denominator is zero.
    """
    totals = _totals(counts, ["owner", "backfill", "idle"])
    owner_idle = totals["backfill"] + totals["idle"]
    all_time = totals["owner"] + owner_idle
    return {
        "backfill_fill_pct": _pct(totals["backfill"], owner_idle),
        "allocated_pct": _pct(totals["owner"] + totals["backfill"], all_time),
        "owner_pct": _pct(totals["owner"], all_time),
    }


def open_stats(counts: pl.DataFrame) -> dict[str, float | None]:
    """Open-capacity share allocated, in percent; None when there is no GPU time."""
    totals = _totals(counts, ["allocated", "idle"])
    return {"allocated_pct": _pct(totals["allocated"], totals["allocated"] + totals["idle"])}


def _prioritized_headings(stats: dict[str, float | None], period: str) -> tuple[str, str]:
    title = "CHTC backfill puts idle GPUs to work"
    allocated, owner = stats["allocated_pct"], stats["owner_pct"]
    if allocated is None or owner is None:
        return title, f"Researcher-owned and reserved GPUs, {period}"
    return title, (
        f"Researcher-owned and reserved GPUs, {period}: {owner:.0f}% allocated to owner jobs, "
        f"{allocated:.0f}% including backfill"
    )


def _open_headings(stats: dict[str, float | None], period: str) -> tuple[str, str]:
    title = "CHTC Open Capacity GPUs stay busy"
    allocated = stats["allocated_pct"]
    if allocated is None:
        return title, f"Open Capacity GPUs, {period}"
    return title, f"Open Capacity GPUs, {period}: {allocated:.0f}% allocated"


@dataclass(frozen=True)
class Fleet:
    """A set of GPUs to plot and how to classify, summarize and title it.

    member: row-level expression; a GPU is in the fleet for a bucket when any
        of its rows matches.
    state: expression over the per-GPU booleans owner and backfill (any
        primary / backfill slot Claimed) giving the state key.
    states: (key, legend label, color), bottom to top of the stack.
    headings: (stats, period label) -> (title, subtitle).
    """

    name: str
    member: pl.Expr
    state: pl.Expr
    states: list[tuple[str, str, str]]
    stats: Callable[[pl.DataFrame], dict[str, float | None]]
    headings: Callable[[dict[str, float | None], str], tuple[str, str]]
    default_output: str

    @property
    def keys(self) -> list[str]:
        return [key for key, _, _ in self.states]


FLEETS = {
    "prioritized": Fleet(
        name="prioritized",
        member=_primary & pl.col("_pp_prio"),
        state=pl.when(_owner).then(pl.lit("owner")).when(_backfill).then(pl.lit("backfill")).otherwise(pl.lit("idle")),
        states=[
            ("owner", "Owner jobs", "#3a6ea5"),
            ("backfill", "Backfill on owner-idle GPUs", "#e0702f"),
            IDLE,
        ],
        stats=headline_stats,
        headings=_prioritized_headings,
        default_output="backfill_fill.png",
    ),
    "open": Fleet(
        name="open-capacity",
        member=_primary & pl.col("_pp_shared") & ~pl.col("_is_inter"),
        state=pl.when(_owner | _backfill).then(pl.lit("allocated")).otherwise(pl.lit("idle")),
        states=[("allocated", "Allocated", "#2f9a5a"), IDLE],
        stats=open_stats,
        headings=_open_headings,
        default_output="open_capacity_fill.png",
    ),
}


def gpu_states(raw: pl.LazyFrame, fleet: Fleet) -> pl.LazyFrame:
    """One row per (bucket, fleet GPU) with its state for that bucket.

    Args:
        raw: Output of classify_slots.derive_columns.
        fleet: Which GPUs to keep and how to name their states.

    Returns:
        LazyFrame with columns bucket, AssignedGPUs, state.
    """
    claimed = pl.col("State") == "Claimed"
    return (
        raw.filter(~pl.col("_excluded") & pl.col("AssignedGPUs").is_not_null() & KEEP_DEVICE)
        .group_by("bucket", "AssignedGPUs")
        .agg(
            fleet.member.any().alias("member"),
            (_primary & claimed).any().alias("owner"),
            (pl.col("_is_bf") & claimed).any().alias("backfill"),
        )
        .filter(pl.col("member"))
        .select("bucket", "AssignedGPUs", fleet.state.alias("state"))
    )


def bucket_counts(raw: pl.LazyFrame, start: datetime.datetime, end: datetime.datetime, fleet: Fleet) -> pl.DataFrame:
    """Fleet GPU count per state per bucket over [start, end], one row per bucket.

    Collected one CHUNK at a time. Chunks start at midnight of start's day so
    their edges fall on bucket boundaries and no bucket is split across chunks;
    rows before start are left to the caller's scan filter.
    """
    ts = pl.col("timestamp")
    parts = []
    lo = datetime.datetime.combine(start.date(), datetime.time())
    while lo <= end:
        hi = lo + CHUNK
        in_chunk = (ts >= lo) & ((ts < hi) if hi <= end else (ts <= end))
        counts = (
            gpu_states(raw.filter(in_chunk), fleet)
            .group_by("bucket")
            .agg((pl.col("state") == key).sum().cast(pl.Int64).alias(key) for key in fleet.keys)
            .collect(engine="streaming")
        )
        parts.append(counts)
        lo = hi
    return pl.concat(parts).sort("bucket")


def weekly_states(counts: pl.DataFrame) -> pl.DataFrame:
    """Weekly mean GPU count per state; weeks with under MIN_DAYS days of data dropped."""
    return (
        counts.with_columns(pl.col("bucket").dt.truncate("1w").alias("week_start"))
        .group_by("week_start")
        .agg(
            pl.exclude("bucket", "week_start").mean(),
            pl.col("bucket").dt.date().n_unique().alias("n_days"),
        )
        .filter(pl.col("n_days") >= MIN_DAYS)
        .sort("week_start")
    )


def data_gaps(counts: pl.DataFrame, min_gap: datetime.timedelta = datetime.timedelta(days=1)) -> list[str]:
    """Collection outages longer than min_gap, as 'Mar 25 – Mar 30 2026' spans."""
    buckets: list[datetime.datetime] = counts["bucket"].to_list()
    return [
        f"{prev:%b %-d} – {cur:%b %-d %Y}"
        for prev, cur in zip(buckets, buckets[1:], strict=False)
        if cur - prev > min_gap
    ]


def create_plot(
    weekly: pl.DataFrame, fleet: Fleet, stats: dict[str, float | None], gaps: list[str], output_path: str, period: str
):
    fig, ax = plt.subplots(figsize=(19.2, 6.5))
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    # Reindex onto every week so missing weeks break the areas instead of bridging them
    present: list[datetime.datetime] = weekly["week_start"].to_list()
    first, last = present[0], present[-1]
    all_weeks = pl.DataFrame({"week_start": pl.datetime_range(first, last, interval="1w", eager=True)})
    full = all_weeks.join(weekly, on="week_start", how="left")
    weeks = full["week_start"].to_list()

    series = [full[key].to_numpy() for key in fleet.keys]
    ax.stackplot(
        weeks,
        *series,
        colors=[color for _, _, color in fleet.states],
        edgecolor=SURFACE,
        linewidth=0.6,
    )

    ax.legend(
        [label for _, label, _ in fleet.states],
        loc="lower left",
        bbox_to_anchor=(0, 1.0),
        ncol=len(fleet.states),
        frameon=False,
        fontsize=12,
        labelcolor=TEXT_SECONDARY,
        handlelength=1.2,
        handleheight=1.2,
        borderaxespad=0.3,
        borderpad=0,
        handletextpad=0.5,
        columnspacing=1.6,
    )

    ax.set_ylim(0, None)
    ax.set_ylabel("GPUs (weekly average)", fontsize=13, color=TEXT_SECONDARY)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=12)
    month_starts = pl.date_range(first.date(), last.date(), interval="1mo", eager=True)
    ticks = [d for d in month_starts.dt.month_start().unique().sort().to_list() if d >= first.date()][::2]
    ax.xaxis.set_major_locator(mticker.FixedLocator([float(mdates.date2num(d)) for d in ticks]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.set_xlim(float(mdates.date2num(first)), float(mdates.date2num(last)))

    title, subtitle = fleet.headings(stats, period)
    ax.set_title(title, loc="left", fontsize=20, color=TEXT_PRIMARY, pad=56, fontweight="bold")
    ax.text(0, 1.085, subtitle, transform=ax.transAxes, fontsize=13, color=TEXT_SECONDARY, in_layout=False)

    if gaps:
        note = "No data collected " + "; ".join(gaps)
        ax.text(0, -0.15, note, transform=ax.transAxes, fontsize=10, color=TEXT_SECONDARY, va="top")

    plt.tight_layout()
    stem = Path(output_path)
    for path in (stem.with_suffix(".png"), stem.with_suffix(".svg")):
        plt.savefig(path, dpi=300, bbox_inches="tight", facecolor=SURFACE)
        print(f"Plot saved to: {path}")
    plt.close(fig)


def print_summary(weekly: pl.DataFrame, fleet: Fleet, stats: dict[str, float | None]):
    print(f"\nWeeks: {weekly.height}  ({weekly['week_start'].min()} to {weekly['week_start'].max()})")
    for key, label, _ in fleet.states:
        print(f"{label}: avg {weekly[key].mean():.1f} GPUs")
    for name, value in stats.items():
        print(f"{name}: {'n/a' if value is None else f'{value:.1f}%'}")


def _parse_date(value: str) -> datetime.datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")


def _load(args: argparse.Namespace, fleet: Fleet) -> pl.DataFrame:
    """Per-bucket state counts for the requested window."""
    end = args.end or get_latest_timestamp(args.data_dir)
    if end is None:
        sys.exit(f"Error: no gpu_state Parquet files found in {args.data_dir}")
    start = args.start or end - datetime.timedelta(days=365)
    if start >= end:
        sys.exit(f"Error: start {start} is not before end {end}")
    classify_slots.HOST_EXCLUSIONS = load_host_exclusions(None, args.exclude_hosts_yaml)

    print(f"Aggregating window {start} – {end} from {args.data_dir}...")
    hours_back = (end - start).total_seconds() / 3600
    raw = derive_columns(scan_time_filtered(args.data_dir, hours_back, end))
    counts = bucket_counts(raw, start, end, fleet)
    if counts.is_empty():
        sys.exit(f"Error: no {fleet.name} GPU data found in the specified time range")
    return counts


def main():
    parser = argparse.ArgumentParser(description="Plot how a GPU fleet spends its time, week by week")
    parser.add_argument("--fleet", choices=sorted(FLEETS), default="prioritized", help="GPU fleet to plot")
    parser.add_argument("--data-dir", default=".", help="Directory containing gpu_state_*.parquet files")
    parser.add_argument("--start", type=_parse_date, default=None, help="Window start, default: one year before end")
    parser.add_argument("--end", type=_parse_date, default=None, help="Window end, default: latest data")
    parser.add_argument("--exclude-hosts-yaml", default="masked_hosts.yaml", help="Host exclusions YAML")
    parser.add_argument(
        "--output", "-o", help="Output path; a .svg is written alongside. Default: per fleet, e.g. backfill_fill.png"
    )
    parser.add_argument("--csv", help="Optional CSV output path for the weekly data")
    args = parser.parse_args()

    fleet = FLEETS[args.fleet]
    counts = _load(args, fleet)
    weekly = weekly_states(counts)
    if weekly.is_empty():
        sys.exit(f"Error: no complete weeks of {fleet.name} GPU data in the window")
    stats = fleet.stats(counts)
    print_summary(weekly, fleet, stats)

    if args.csv:
        weekly.write_csv(args.csv)
        print(f"CSV saved to: {args.csv}")

    period = f"{counts['bucket'].min():%b %Y} – {counts['bucket'].max():%b %Y}"
    create_plot(weekly, fleet, stats, data_gaps(counts), args.output or fleet.default_output, period)


if __name__ == "__main__":
    main()
