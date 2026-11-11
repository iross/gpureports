#!/usr/bin/env python3
"""
Poster Stats

Headline numbers for a window of gpu_state data (default: the year before the
latest data): GPU-hours allocated by fleet, jobs run, distinct users, fleet
size and peak concurrency.

Fleets and the device filter come from plot_backfill_fill, so these numbers
describe the same GPUs as the backfill and open-capacity plots. Each snapshot
row is weighted by the time to the next snapshot, capped at MAX_GAP so
collector outages do not count as allocated time. Jobs and users are seen
only through 5-minute snapshots, so jobs shorter than that can be missed.

Monthly files are processed one at a time to bound memory.
"""

import argparse
import datetime
import glob
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

import classify_slots  # noqa: E402
from classify_slots import derive_columns  # noqa: E402
from read_data import (  # noqa: E402
    GPU_STATE_SCHEMA,
    SCAN_CAST_OPTIONS,
    get_latest_timestamp,
    load_host_exclusions,
    parquet_glob,
)
from scripts.plot_backfill_fill import FLEETS, KEEP_DEVICE, _parse_date  # noqa: E402

MAX_GAP = 600  # seconds
DEFAULT_GAP = 300  # weight for a window's last snapshot, which has no successor
HOURS_PER_YEAR = 8766
ALLOCATED_CATS = ["prio_owner", "prio_backfill", "open_alloc"]

_claimed = pl.col("State") == "Claimed"
_primary = ~pl.col("_is_bf")
_prio_row = FLEETS["prioritized"].member
_open_row = FLEETS["open"].member


def snapshot_weights(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Seconds each snapshot timestamp stands for: gap to the next one, capped at MAX_GAP."""
    gap = (pl.col("timestamp").shift(-1) - pl.col("timestamp")).dt.total_seconds()
    return lf.select(pl.col("timestamp").unique().sort()).with_columns(
        gap.clip(upper_bound=MAX_GAP).fill_null(DEFAULT_GAP).alias("seconds")
    )


def gpu_snapshots(raw: pl.LazyFrame) -> pl.LazyFrame:
    """One row per (timestamp, GPU) with its category, model, machine and weight.

    Categories: prio_owner, prio_backfill, prio_idle (prioritized fleet, same
    precedence as the backfill plot); open_alloc, open_idle (open-capacity
    fleet); other (neither fleet).

    Args:
        raw: Output of classify_slots.derive_columns, already limited to the window.
    """
    owner = pl.col("owner")
    backfill = pl.col("backfill")
    prio = pl.col("prio")
    cat = (
        pl.when(prio & owner)
        .then(pl.lit("prio_owner"))
        .when(prio & backfill)
        .then(pl.lit("prio_backfill"))
        .when(prio)
        .then(pl.lit("prio_idle"))
        .when(pl.col("open") & (owner | backfill))
        .then(pl.lit("open_alloc"))
        .when(pl.col("open"))
        .then(pl.lit("open_idle"))
        .otherwise(pl.lit("other"))
    )
    return (
        raw.filter(~pl.col("_excluded") & pl.col("AssignedGPUs").is_not_null() & KEEP_DEVICE)
        .group_by("timestamp", "AssignedGPUs")
        .agg(
            _prio_row.any().alias("prio"),
            _open_row.any().alias("open"),
            (_primary & _claimed).any().alias("owner"),
            (pl.col("_is_bf") & _claimed).any().alias("backfill"),
            pl.col("GPUs_DeviceName").first().alias("model"),
            pl.col("Machine").first().alias("machine"),
        )
        .select("timestamp", "AssignedGPUs", "model", "machine", cat.alias("cat"))
        .join(snapshot_weights(raw), on="timestamp")
    )


def claimed_jobs(raw: pl.LazyFrame) -> pl.LazyFrame:
    """Distinct (GlobalJobId, RemoteOwner, kind) for claimed slots; kind is backfill, prio_owner, open or other."""
    kind = (
        pl.when(pl.col("_is_bf"))
        .then(pl.lit("backfill"))
        .when(_prio_row)
        .then(pl.lit("prio_owner"))
        .when(_open_row)
        .then(pl.lit("open"))
        .otherwise(pl.lit("other"))
    )
    return (
        raw.filter(
            ~pl.col("_excluded")
            & KEEP_DEVICE
            & pl.col("AssignedGPUs").is_not_null()
            & _claimed
            & pl.col("GlobalJobId").is_not_null()
        )
        .select("GlobalJobId", "RemoteOwner", kind.alias("kind"))
        .unique()
    )


def collect_parts(raw: pl.LazyFrame) -> dict[str, pl.DataFrame]:
    """Small per-window aggregates that can be concatenated across files.

    Returns:
        hours: cat, gpu_hours. peak: timestamp, allocated (GPUs in ALLOCATED_CATS).
        gpus: distinct AssignedGPUs, model, machine, cat. jobs: claimed_jobs rows.
    """
    snaps = gpu_snapshots(raw)
    hours, peak, gpus, jobs = pl.collect_all(
        [
            snaps.group_by("cat").agg((pl.col("seconds").sum() / 3600).alias("gpu_hours")),
            snaps.filter(pl.col("cat").is_in(ALLOCATED_CATS)).group_by("timestamp").agg(pl.len().alias("allocated")),
            snaps.select("AssignedGPUs", "model", "machine", "cat").unique(),
            claimed_jobs(raw),
        ],
        engine="streaming",
    )
    return {"hours": hours, "peak": peak, "gpus": gpus, "jobs": jobs}


def summarize(parts: list[dict[str, pl.DataFrame]]) -> dict:
    """Combine per-file parts into the headline numbers."""
    hours_df = pl.concat([p["hours"] for p in parts]).group_by("cat").agg(pl.col("gpu_hours").sum())
    hours = dict(zip(hours_df["cat"], hours_df["gpu_hours"], strict=True))
    peak = pl.concat([p["peak"] for p in parts]).group_by("timestamp").agg(pl.col("allocated").sum())
    gpus = pl.concat([p["gpus"] for p in parts])
    jobs = pl.concat([p["jobs"] for p in parts]).unique()
    fleet_jobs = jobs.filter(pl.col("kind") != "other")

    def h(cat: str) -> float:
        return hours.get(cat, 0.0)

    prio_total = h("prio_owner") + h("prio_backfill") + h("prio_idle")
    open_total = h("open_alloc") + h("open_idle")
    return {
        "gpu_hours": hours,
        "allocated_gpu_hours": sum(h(cat) for cat in ALLOCATED_CATS),
        "prio_owner_pct": 100 * h("prio_owner") / prio_total if prio_total else None,
        "prio_allocated_pct": 100 * (h("prio_owner") + h("prio_backfill")) / prio_total if prio_total else None,
        "open_allocated_pct": 100 * h("open_alloc") / open_total if open_total else None,
        "peak_allocated": peak["allocated"].max() if peak.height else 0,
        "median_allocated": peak["allocated"].median() if peak.height else 0,
        "jobs": fleet_jobs["GlobalJobId"].n_unique(),
        "users": fleet_jobs["RemoteOwner"].n_unique(),
        "jobs_by_kind": jobs.group_by("kind")
        .agg(pl.col("GlobalJobId").n_unique().alias("jobs"), pl.col("RemoteOwner").n_unique().alias("users"))
        .sort("kind"),
        "gpus": gpus["AssignedGPUs"].n_unique(),
        "machines": gpus["machine"].n_unique(),
        "models": gpus.unique("AssignedGPUs").group_by("model").len().sort("len", descending=True),
    }


def _fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.0f}%"


def round_big(n: float) -> str:
    """A count as it reads on a slide: '2.26 million', '867,000', '541'."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.3g} million"
    if n >= 10_000:
        return f"{round(n, -3):,.0f}"
    return f"{n:,.0f}"


def poster_lines(stats: dict, start: datetime.datetime, end: datetime.datetime) -> list[str]:
    """One plain-text line per stat, with no alignment padding, for pasting into slides."""
    hours = stats["gpu_hours"]
    allocated = stats["allocated_gpu_hours"]
    backfill = hours.get("prio_backfill", 0.0)
    jobs_by_kind = {row["kind"]: row for row in stats["jobs_by_kind"].iter_rows(named=True)}
    bf_jobs = jobs_by_kind.get("backfill", {"jobs": 0, "users": 0})
    return [
        f"{start:%b %Y} – {end:%b %Y}",
        f"{round_big(allocated)} GPU-hours allocated to jobs ({allocated / HOURS_PER_YEAR:,.0f} GPU-years)",
        f"{round_big(stats['jobs'])} jobs run",
        f"{stats['users']:,} users",
        f"{stats['gpus']} GPUs across {stats['machines']} machines and {stats['models'].height} GPU models",
        f"Up to {stats['peak_allocated']} GPUs allocated at once",
        f"{round_big(backfill)} GPU-hours from backfill on owners' idle GPUs, "
        f"like {backfill / HOURS_PER_YEAR:,.0f} GPUs running all year",
        f"{round_big(bf_jobs['jobs'])} backfill jobs from {bf_jobs['users']:,} users",
        f"Researcher-owned and reserved GPUs allocated {_fmt_pct(stats['prio_owner_pct'])} of the time "
        f"to owner jobs, {_fmt_pct(stats['prio_allocated_pct'])} including backfill",
        f"{round_big(hours.get('open_alloc', 0.0))} Open Capacity GPU-hours, "
        f"allocated {_fmt_pct(stats['open_allocated_pct'])} of the time",
    ]


def print_details(stats: dict) -> None:
    hours = stats["gpu_hours"]
    print("\nGPU-hours by category:")
    for cat, value in sorted(hours.items()):
        print(f"  {cat}: {value:,.0f}")
    print(f"Median concurrent allocated: {stats['median_allocated']:.0f} GPUs")
    print("\nJobs and users by kind:")
    print(stats["jobs_by_kind"])
    print("\nGPUs by model:")
    with pl.Config(tbl_rows=-1):
        print(stats["models"])


def _scan(path: str) -> pl.LazyFrame:
    return pl.scan_parquet(path, schema=GPU_STATE_SCHEMA, missing_columns="insert", cast_options=SCAN_CAST_OPTIONS)


def main() -> None:
    parser = argparse.ArgumentParser(description="Headline GPU stats for a poster")
    parser.add_argument("--data-dir", default=".", help="Directory containing gpu_state_*.parquet files")
    parser.add_argument("--start", type=_parse_date, default=None, help="Window start, default: one year before end")
    parser.add_argument("--end", type=_parse_date, default=None, help="Window end, default: latest data")
    parser.add_argument("--exclude-hosts-yaml", default="masked_hosts.yaml", help="Host exclusions YAML")
    parser.add_argument("--details", action="store_true", help="Also print per-category and per-model breakdowns")
    args = parser.parse_args()

    files = sorted(glob.glob(parquet_glob(args.data_dir)))
    end = args.end or get_latest_timestamp(args.data_dir)
    if not files or end is None:
        sys.exit(f"Error: no gpu_state Parquet files found in {args.data_dir}")
    start = args.start or end - datetime.timedelta(days=365)
    if start >= end:
        sys.exit(f"Error: start {start} is not before end {end}")
    classify_slots.HOST_EXCLUSIONS = load_host_exclusions(None, args.exclude_hosts_yaml)

    parts = []
    for path in files:
        raw = derive_columns(_scan(path).filter(pl.col("timestamp").is_between(start, end)))
        part = collect_parts(raw)
        if part["hours"].height:
            parts.append(part)
            print(f"Read {Path(path).name}", file=sys.stderr)
    if not parts:
        sys.exit(f"Error: no GPU data between {start} and {end}")
    stats = summarize(parts)
    print("\n".join(poster_lines(stats, start, end)))
    if args.details:
        print_details(stats)


if __name__ == "__main__":
    main()
