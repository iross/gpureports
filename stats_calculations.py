#!/usr/bin/env python3
"""
GPU Usage Statistics - Calculation Functions

The report-scale aggregations (device/memory/user breakdowns, draining,
prevent-jobs) run on polars LazyFrames prepared by prepare_frames(), so a
year-scale window is never materialized as a pandas DataFrame. A handful of
small-window helpers (timeseries, non-device allocation, GPU model snapshots)
remain pandas-based and are consumed by standalone scripts.
"""

import datetime
import re
from dataclasses import dataclass, field

import duckdb
import pandas as pd
import polars as pl

import gpu_utils
from device_name_mappings import get_memory_category_from_mb
from gpu_utils import (
    BACKFILL_SLOT_TYPES,
    CLASS_ORDER,
    UTILIZATION_TYPES,
    filter_df,
    filter_df_enhanced,
    load_chtc_owned_hosts,
)
from stats_data import (
    get_latest_timestamp,
    parquet_glob,
    scan_time_filtered,
)

# GPU families hidden from reports unless --all-devices is passed
OLD_GPU_TYPES = ["GTX 1080", "P100", "Quadro", "A30", "A40"]

_PJ_COLUMN = "PreventJobsReason"


# ---------------------------------------------------------------------------
# Prepared lazy frames shared by the polars calculations
# ---------------------------------------------------------------------------


@dataclass
class PreparedFrames:
    """Window data prepared for the polars aggregations.

    raw is a LazyFrame over the Parquet files with derived boolean columns and
    NO host exclusions applied (denominators like bucket counts match the
    pre-exclusion window). dedup and raw_bf are collected once and wrapped
    back in LazyFrames: dedup holds one representative row per
    (timestamp, GPU) after host exclusion and duplicate-slot ranking; raw_bf
    holds all backfill-slot rows after host exclusion.
    """

    raw: pl.LazyFrame
    dedup: pl.LazyFrame
    raw_bf: pl.LazyFrame
    total_buckets: int
    start_time: datetime.datetime | None
    end_time: datetime.datetime | None
    original_count: int
    excluded_count: int
    has_prevent_jobs: bool
    excluded_hosts: dict = field(default_factory=dict)


def _host_exclusion_expr(exclusions: dict) -> pl.Expr:
    if not exclusions:
        return pl.lit(False)
    pattern = "|".join(re.escape(host) for host in exclusions)
    return pl.col("Machine").str.contains(f"(?i)({pattern})").fill_null(False)


def prepare_frames(lf: pl.LazyFrame) -> PreparedFrames:
    """Derive classification columns and collect the shared dedup/backfill frames.

    The duplicate-slot ranking (see gpu_utils._apply_duplicate_cleanup) is
    computed once for the whole window instead of once per class/device
    filter: each GPU maps to a single machine and device, so a global dedup
    is equivalent to the per-subset dedup the pandas filters performed.
    """
    exclusions = gpu_utils.HOST_EXCLUSIONS
    chtc_hosts = load_chtc_owned_hosts()
    has_pj = _PJ_COLUMN in lf.collect_schema().names()

    state = pl.col("State")
    if has_pj:
        pj_set = pl.col(_PJ_COLUMN).is_not_null() & (pl.col(_PJ_COLUMN).str.strip_chars() != "")
        prev_idle = pj_set & (state != "Claimed")
    else:
        pj_set = pl.lit(False)
        prev_idle = pl.lit(False)

    raw = lf.with_columns(
        pl.col("timestamp").dt.truncate("15m").alias("bucket"),
        pl.col("Name").str.contains("backfill").fill_null(False).alias("_is_bf"),
        pl.col("Name").str.contains("interactive").fill_null(False).alias("_is_inter"),
        # pandas object-dtype `!= ""` treats null as True and `== ""` as False;
        # the fill_null values preserve that classification for null projects
        (pl.col("PrioritizedProjects") != "").fill_null(True).alias("_pp_prio"),
        (pl.col("PrioritizedProjects") == "").fill_null(False).alias("_pp_shared"),
        pl.col("Machine").is_in(sorted(chtc_hosts)).fill_null(False).alias("_is_chtc"),
        _host_exclusion_expr(exclusions).alias("_excluded"),
        pj_set.alias("_pj_set"),
        prev_idle.alias("_prev_idle"),
    )

    meta_q = raw.select(
        pl.len().alias("total"),
        pl.col("timestamp").min().alias("start"),
        pl.col("timestamp").max().alias("end"),
        pl.col("bucket").n_unique().alias("buckets"),
        pl.col("_excluded").sum().alias("excluded"),
    )

    is_bf = pl.col("_is_bf")
    claimed = state == "Claimed"
    unclaimed = state == "Unclaimed"
    rank = (
        pl.when(~is_bf & claimed)
        .then(6)
        .when(~is_bf & pl.col("_prev_idle"))
        .then(2)
        .when(~is_bf & unclaimed)
        .then(5)
        .when(~is_bf)
        .then(4)
        .when(is_bf & claimed)
        .then(3)
        .when(is_bf & unclaimed)
        .then(1)
        .otherwise(0)
        .alias("_rank")
    )

    dedup_cols = [
        "State",
        "Name",
        "Machine",
        "GPUs_DeviceName",
        "GPUs_GlobalMemoryMb",
        "RemoteOwner",
        "_is_bf",
        "_is_inter",
        "_pp_prio",
        "_pp_shared",
        "_is_chtc",
        "_pj_set",
    ]
    dedup_q = (
        raw.filter(~pl.col("_excluded") & pl.col("AssignedGPUs").is_not_null())
        .with_columns(rank)
        .group_by("timestamp", "AssignedGPUs")
        .agg(pl.struct(dedup_cols).sort_by("_rank").last().alias("_top"))
        .unnest("_top")
        .with_columns(pl.col("timestamp").dt.truncate("15m").alias("bucket"))
        .drop("timestamp")
    )

    raw_bf_q = raw.filter(~pl.col("_excluded") & is_bf).select(
        "bucket",
        "AssignedGPUs",
        "State",
        "Name",
        "Machine",
        "GPUs_DeviceName",
        "RemoteOwner",
        "_is_chtc",
        "_pj_set",
    )

    meta, dedup, raw_bf = pl.collect_all([meta_q, dedup_q, raw_bf_q], engine="streaming")
    row = meta.row(0, named=True)

    return PreparedFrames(
        raw=raw,
        dedup=dedup.lazy(),
        raw_bf=raw_bf.lazy(),
        total_buckets=row["buckets"],
        start_time=row["start"],
        end_time=row["end"],
        original_count=row["total"],
        excluded_count=row["excluded"] or 0,
        has_prevent_jobs=has_pj,
        excluded_hosts=dict(exclusions) if exclusions else {},
    )


# Class-membership expressions evaluated against the dedup frame's
# representative row per (timestamp, GPU)
_REAL_CLASS_EXPRS = {
    "Priority-ResearcherOwned": pl.col("_pp_prio") & ~pl.col("_is_chtc") & ~pl.col("_is_bf"),
    "Priority-CHTCOwned": pl.col("_pp_prio") & pl.col("_is_chtc") & ~pl.col("_is_bf"),
    "Shared": pl.col("_pp_shared") & ~pl.col("_is_bf") & ~pl.col("_is_inter"),
}


def _researcher_scope(frames: PreparedFrames, keys: list[str], device: str | None = None) -> pl.LazyFrame:
    """Machines (or machine/device pairs) whose primary slots carry PrioritizedProjects.

    Mirrors the researcher-machine discovery inside filter_df_enhanced: primary
    slots with a non-empty, non-null PrioritizedProjects on non-CHTC machines,
    scoped to the same subset (per device, or globally) the pandas code used.
    """
    f = frames.raw.filter(
        ~pl.col("_excluded")
        & ~pl.col("_is_bf")
        & (pl.col("PrioritizedProjects") != "").fill_null(False)
        & ~pl.col("_is_chtc")
    )
    if device is not None:
        f = f.filter(pl.col("GPUs_DeviceName") == device)
    return f.select(keys).unique()


def _class_frame(
    frames: PreparedFrames,
    class_name: str,
    host: str = "",
    researcher: pl.LazyFrame | None = None,
    researcher_keys: list[str] | None = None,
) -> pl.LazyFrame:
    """Rows belonging to a slot class: deduped rows for Real-slot classes, raw
    backfill rows for Backfill classes (matching filter_df_enhanced)."""
    if class_name in _REAL_CLASS_EXPRS:
        f = frames.dedup.filter(_REAL_CLASS_EXPRS[class_name])
        if host:
            f = f.filter(pl.col("Name").str.contains(host).fill_null(False))
        return f

    f = frames.raw_bf
    if host:
        f = f.filter(pl.col("Name").str.contains(host).fill_null(False))
    if class_name == "Backfill-CHTCOwned":
        return f.filter(pl.col("_is_chtc"))
    if class_name == "Backfill-ResearcherOwned":
        if researcher is None:
            raise ValueError("Backfill-ResearcherOwned requires a researcher scope frame")
        return f.join(researcher, on=researcher_keys or ["Machine"], how="semi")
    raise ValueError(f"Unknown class name: {class_name}")


def _pair_bucket_stats(frame: pl.LazyFrame, group_cols: list[str]) -> pl.DataFrame:
    """Aggregate unique-GPU claim/drain counts per 15-minute bucket.

    A GPU counts as claimed (or drained) in a bucket if any of its
    representative rows in that bucket has that state; a GPU claimed in a
    bucket is not also counted as drained (Claimed wins).
    """
    pairs = (
        frame.filter(pl.col("AssignedGPUs").is_not_null())
        .group_by([*group_cols, "bucket", "AssignedGPUs"])
        .agg(
            (pl.col("State") == "Claimed").any().alias("_claimed"),
            (pl.col("State") == "Drained").any().alias("_drained"),
        )
    )
    per_bucket = pairs.group_by([*group_cols, "bucket"]).agg(
        pl.len().alias("total"),
        pl.col("_claimed").sum().alias("claimed"),
        (pl.col("_drained") & ~pl.col("_claimed")).sum().alias("drained"),
    )
    return (
        per_bucket.group_by(group_cols)
        .agg(
            (pl.col("claimed") / pl.col("total") * 100).mean().alias("pct_claimed"),
            (pl.col("drained") / pl.col("total") * 100).mean().alias("pct_drained"),
            pl.col("claimed").sum().alias("claimed_sum"),
            pl.col("drained").sum().alias("drained_sum"),
            pl.col("total").sum().alias("total_sum"),
            pl.len().alias("num_buckets"),
        )
        .sort(group_cols)
        .collect(engine="streaming")
    )


def _is_old_device(device: str) -> bool:
    return any(old in device for old in OLD_GPU_TYPES)


def calculate_allocation_usage_by_device_enhanced(
    frames: PreparedFrames, host: str = "", include_all_devices: bool = True
) -> dict:
    """
    Calculate allocation-based usage grouped by device type with enhanced backfill categories.

    Args:
        frames: Prepared window frames from prepare_frames()
        host: Optional host filter (matched against slot Name)
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with usage statistics for each enhanced class and device type
    """
    utilization_types = [
        "Priority-ResearcherOwned",
        "Priority-CHTCOwned",
        "Shared",
        "Backfill-CHTCOwned",
        "Backfill-ResearcherOwned",
    ]
    # Backfill ownership is resolved per (machine, device) to match the pandas
    # path, which discovered researcher machines within each device subset
    researcher = _researcher_scope(frames, ["Machine", "GPUs_DeviceName"])
    total_intervals = frames.total_buckets

    stats = {}
    for utilization_type in utilization_types:
        frame = _class_frame(
            frames, utilization_type, host, researcher, researcher_keys=["Machine", "GPUs_DeviceName"]
        ).filter(pl.col("GPUs_DeviceName").is_not_null())
        agg = _pair_bucket_stats(frame, ["GPUs_DeviceName"])

        stats[utilization_type] = {}
        for row in agg.iter_rows(named=True):
            device_type = row["GPUs_DeviceName"]
            if not include_all_devices and _is_old_device(device_type):
                continue
            stats[utilization_type][device_type] = {
                "avg_claimed": row["claimed_sum"] / total_intervals if total_intervals > 0 else 0,
                "avg_drained": row["drained_sum"] / total_intervals if total_intervals > 0 else 0,
                "avg_total_available": row["total_sum"] / total_intervals if total_intervals > 0 else 0,
                "allocation_usage_percent": float(row["pct_claimed"]),
                "drained_percent": float(row["pct_drained"]),
                "num_intervals": total_intervals,
            }

    return stats


def calculate_allocation_usage_by_memory(
    frames: PreparedFrames, host: str = "", include_all_devices: bool = True
) -> dict:
    """
    Calculate allocation-based usage grouped by memory category for Real slots only.
    Uses GPUs_GlobalMemoryMb field for dynamic memory categorization.

    Args:
        frames: Prepared window frames from prepare_frames()
        host: Optional host filter (matched against slot Name)
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with usage statistics for each memory category (for Real slots only)
    """
    real_slot_expr = (
        _REAL_CLASS_EXPRS["Priority-ResearcherOwned"]
        | _REAL_CLASS_EXPRS["Priority-CHTCOwned"]
        | _REAL_CLASS_EXPRS["Shared"]
    )
    frame = frames.dedup.filter(real_slot_expr)
    if host:
        frame = frame.filter(pl.col("Name").str.contains(host).fill_null(False))
    if not include_all_devices:
        old_mask = pl.col("GPUs_DeviceName").str.contains("|".join(OLD_GPU_TYPES)).fill_null(False)
        frame = frame.filter(~old_mask)

    # Map the handful of distinct memory sizes through the categorization
    # function instead of applying it per row
    mem_values = frame.select(pl.col("GPUs_GlobalMemoryMb").unique()).collect()["GPUs_GlobalMemoryMb"].to_list()
    mem_map = {mb: get_memory_category_from_mb(mb) for mb in mem_values if mb is not None}
    frame = frame.with_columns(
        pl.col("GPUs_GlobalMemoryMb")
        .replace_strict(mem_map, default=get_memory_category_from_mb(None), return_dtype=pl.Utf8)
        .alias("memory_category")
    )

    agg = _pair_bucket_stats(frame, ["memory_category"])

    stats = {}
    for row in agg.iter_rows(named=True):
        num_intervals = row["num_buckets"]
        stats[row["memory_category"]] = {
            "avg_claimed": row["claimed_sum"] / num_intervals,
            "avg_drained": row["drained_sum"] / num_intervals,
            "avg_total_available": row["total_sum"] / num_intervals,
            "allocation_usage_percent": float(row["pct_claimed"]),
            "drained_percent": float(row["pct_drained"]),
            "num_intervals": num_intervals,
        }
    return stats


def _user_gpu_totals(frame: pl.LazyFrame) -> dict[str, int]:
    """Sum per-bucket unique claimed GPU counts per user (empty owner → Unknown)."""
    per_owner = (
        frame.filter((pl.col("State") == "Claimed") & pl.col("RemoteOwner").is_not_null())
        .group_by("bucket", "RemoteOwner")
        .agg(pl.col("AssignedGPUs").drop_nulls().n_unique().alias("n"))
        .group_by("RemoteOwner")
        .agg(pl.col("n").sum().alias("total"))
        .collect(engine="streaming")
    )
    totals: dict[str, int] = {}
    for owner, total in per_owner.iter_rows():
        user = "Unknown" if owner == "" else owner
        totals[user] = totals.get(user, 0) + int(total)
    return totals


def _finalize_user_stats(user_stats: dict) -> dict:
    """Drop zero-usage users/classes and add per-class percentages."""
    final_stats = {}
    for user, slot_data in user_stats.items():
        total_gpu_hours = sum(slot_data.values())
        if total_gpu_hours > 0:
            final_stats[user] = {"total_gpu_hours": total_gpu_hours, "slot_breakdown": {}}
            for slot_type, gpu_hours in slot_data.items():
                if gpu_hours > 0:
                    final_stats[user]["slot_breakdown"][slot_type] = {
                        "gpu_hours": gpu_hours,
                        "percentage": (gpu_hours / total_gpu_hours) * 100,
                    }
    return final_stats


def calculate_h200_user_breakdown(frames: PreparedFrames, host: str = "", hours_back: int = 1) -> dict:
    """
    Calculate H200 usage breakdown by user and slot type.

    Args:
        frames: Prepared window frames from prepare_frames()
        host: Optional host filter (matched against Machine, then slot Name)
        hours_back: Lookback period in hours

    Returns:
        Dictionary with H200 usage statistics by user and slot type
    """
    h200 = pl.col("GPUs_DeviceName") == "NVIDIA H200"
    host_machine = pl.col("Machine").str.contains(f"(?i){re.escape(host)}").fill_null(False) if host else pl.lit(True)

    # Denominator counts all intervals in the H200 dataset (pre-exclusion),
    # including those where a user has 0 GPUs
    num_buckets = frames.raw.filter(h200 & host_machine).select(pl.col("bucket").n_unique()).collect().item()
    if not num_buckets:
        return {}

    researcher = _researcher_scope(frames, ["Machine"], device="NVIDIA H200")

    user_stats: dict[str, dict[str, float]] = {}
    for slot_type in CLASS_ORDER:
        frame = _class_frame(frames, slot_type, host, researcher).filter(h200 & host_machine)
        for user, total_gpus in _user_gpu_totals(frame).items():
            gpu_hours = (total_gpus / num_buckets) * hours_back
            if user not in user_stats:
                user_stats[user] = dict.fromkeys(CLASS_ORDER, 0)
            user_stats[user][slot_type] = gpu_hours

    return _finalize_user_stats(user_stats)


def calculate_backfill_usage_by_user(
    frames: PreparedFrames, host: str = "", hours_back: int = 1, include_all_devices: bool = False
) -> dict:
    """
    Calculate backfill slot usage breakdown by user and slot type.

    Args:
        frames: Prepared window frames from prepare_frames()
        host: Optional host filter (matched against Machine, then slot Name)
        hours_back: Lookback period in hours
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with backfill usage statistics by user and slot type
    """
    keep = pl.lit(True)
    if not include_all_devices:
        pattern = "|".join(OLD_GPU_TYPES)
        keep = keep & ~pl.col("GPUs_DeviceName").str.contains(f"(?i)({pattern})").fill_null(False)
    if host:
        keep = keep & pl.col("Machine").str.contains(f"(?i){re.escape(host)}").fill_null(False)

    num_buckets = frames.raw.filter(keep).select(pl.col("bucket").n_unique()).collect().item()
    if not num_buckets:
        return {}

    researcher = (
        frames.raw.filter(
            keep
            & ~pl.col("_excluded")
            & ~pl.col("_is_bf")
            & (pl.col("PrioritizedProjects") != "").fill_null(False)
            & ~pl.col("_is_chtc")
        )
        .select("Machine")
        .unique()
    )

    user_stats: dict[str, dict[str, float]] = {}
    for slot_type in BACKFILL_SLOT_TYPES:
        frame = _class_frame(frames, slot_type, host, researcher).filter(keep)
        for user, total_gpus in _user_gpu_totals(frame).items():
            gpu_hours = (total_gpus / num_buckets) * hours_back
            if user not in user_stats:
                user_stats[user] = dict.fromkeys(BACKFILL_SLOT_TYPES, 0)
            user_stats[user][slot_type] = gpu_hours

    return _finalize_user_stats(user_stats)


def calculate_machines_with_zero_active_gpus(
    frames: PreparedFrames, host: str = "", include_all_devices: bool = True
) -> dict:
    """
    Calculate machines that had ZERO active (claimed) GPUs across the entire time span.

    This identifies machines that had GPUs available but never had any claimed during
    the analysis period, which may indicate underutilized or problematic hosts.

    Args:
        frames: Prepared window frames from prepare_frames()
        host: Optional exact machine name filter
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with per-machine info for hosts with zero claimed GPUs and a summary
    """
    f = frames.raw.filter(~pl.col("_excluded"))
    if host:
        f = f.filter(pl.col("Machine") == host)

    is_bf_ci = pl.col("Name").str.contains("(?i)backfill").fill_null(False)
    claimed = pl.col("State") == "Claimed"
    gpus = pl.col("AssignedGPUs")

    per_machine = (
        f.group_by("Machine")
        .agg(
            pl.len().alias("total_observations"),
            gpus.filter(~is_bf_ci).drop_nulls().n_unique().alias("all_gpus"),
            gpus.filter(~is_bf_ci & claimed).drop_nulls().n_unique().alias("claimed_gpus"),
            pl.col("bucket").filter(is_bf_ci).n_unique().alias("bf_buckets"),
            pl.struct("bucket", "AssignedGPUs")
            .filter(is_bf_ci & claimed & gpus.is_not_null())
            .n_unique()
            .alias("bf_claimed_pairs"),
            pl.col("GPUs_DeviceName").drop_nulls().mode().sort().first().alias("gpu_model"),
            pl.col("PrioritizedProjects").drop_nulls().unique().alias("prioritized_projects"),
        )
        .collect(engine="streaming")
    )

    machines_with_zero_active = []
    total_gpus_idle = 0
    for row in per_machine.iter_rows(named=True):
        if row["claimed_gpus"] != 0 or row["all_gpus"] == 0:
            continue
        gpu_model = row["gpu_model"] or "Unknown"
        if not include_all_devices and _is_old_device(gpu_model):
            continue
        machines_with_zero_active.append(
            {
                "machine": row["Machine"],
                "gpu_model": gpu_model,
                "total_gpus": row["all_gpus"],
                "total_observations": row["total_observations"],
                "prioritized_projects": {p.strip() for p in row["prioritized_projects"] if p.strip()},
                "avg_backfill_claimed": (row["bf_claimed_pairs"] / row["bf_buckets"]) if row["bf_buckets"] else 0,
            }
        )
        total_gpus_idle += row["all_gpus"]

    machines_with_zero_active.sort(key=lambda x: x["machine"])

    return {
        "machines": machines_with_zero_active,
        "summary": {
            "total_machines": len(machines_with_zero_active),
            "total_gpus_idle": total_gpus_idle,
        },
    }


def calculate_prevent_jobs_stats(frames: PreparedFrames) -> dict:
    """
    Calculate summary statistics for GPUs with PreventJobsReason set.

    Args:
        frames: Prepared window frames from prepare_frames()

    Returns:
        Dictionary with per-host breakdown (including last_seen and whether the reason
        is still active), reason groupings, and per-class counts: per_class_avg feeds
        the Real Slots table Prevented (avg.) column. A GPU counts as Prevented only
        when its representative slot (after duplicate cleanup) is idle with
        PreventJobsReason set.
    """
    empty = {
        "has_prevent_jobs": False,
        "num_hosts": 0,
        "num_unique_gpus": 0,
        "per_host": {},
        "by_reason": {},
        "per_class_avg": dict.fromkeys(CLASS_ORDER, 0.0),
        "per_class_device_avg": {c: {} for c in CLASS_ORDER},
        "pj_buckets": 0,
        "total_buckets": 0,
    }

    if not frames.has_prevent_jobs:
        return empty

    pj_rows = frames.raw.filter(pl.col("_pj_set"))
    per_host_agg = (
        pj_rows.group_by("Machine")
        .agg(
            pl.col("AssignedGPUs").drop_nulls().n_unique().alias("num_gpus"),
            pl.col(_PJ_COLUMN).drop_nulls().unique().alias("reasons"),
            pl.col("timestamp").max().alias("last_seen"),
        )
        .collect(engine="streaming")
    )
    if per_host_agg.height == 0:
        return empty

    by_reason_agg = (
        pj_rows.group_by(_PJ_COLUMN)
        .agg(
            pl.col("Machine").drop_nulls().n_unique().alias("num_hosts"),
            pl.col("AssignedGPUs").drop_nulls().n_unique().alias("num_gpus"),
        )
        .sort(_PJ_COLUMN)
        .collect()
    )
    by_reason = {
        str(row[_PJ_COLUMN]): {"num_hosts": row["num_hosts"], "num_gpus": row["num_gpus"]}
        for row in by_reason_agg.iter_rows(named=True)
    }

    # A host is "active" if its reason is still set in the most recent bucket with PJ
    # data; otherwise the reason was lifted partway through the window.
    pj_buckets = sorted(pj_rows.select(pl.col("bucket").unique()).collect()["bucket"].to_list())
    last_pj_bucket = pj_buckets[-1]
    active_machines = set(
        pj_rows.filter(pl.col("bucket") == last_pj_bucket).select(pl.col("Machine").unique()).collect()["Machine"]
    )

    per_host = {}
    for row in sorted(per_host_agg.iter_rows(named=True), key=lambda r: r["Machine"]):
        per_host[row["Machine"]] = {
            "num_gpus": row["num_gpus"],
            "reasons": sorted(row["reasons"]),
            "last_seen": row["last_seen"].strftime("%Y-%m-%d %H:%M"),
            "active": row["Machine"] in active_machines,
        }

    totals = pj_rows.select(
        pl.col("Machine").drop_nulls().n_unique().alias("num_hosts"),
        pl.col("AssignedGPUs").drop_nulls().n_unique().alias("num_gpus"),
    ).collect()

    # PreventJobsReason does not evict running jobs — it only stops new ones, so only
    # idle GPUs count as Prevented. The duplicate-cleanup ranking in prepare_frames
    # resolves multi-slot GPUs: a Claimed slot outranks an idle prevented one, so a
    # GPU still finishing a job surfaces here as Claimed (or, for a backfill-slot
    # job, drops out of the primary class) and counts as Allocated.
    researcher = _researcher_scope(frames, ["Machine"])
    num_buckets = frames.total_buckets
    per_class_avg: dict[str, float] = {}
    per_class_device_avg: dict[str, dict[str, float]] = {}
    for class_name in CLASS_ORDER:
        prevented = _class_frame(frames, class_name, "", researcher).filter(
            pl.col("_pj_set") & (pl.col("State") != "Claimed")
        )
        per_device = (
            prevented.filter(pl.col("GPUs_DeviceName").is_not_null())
            .group_by("GPUs_DeviceName", "bucket")
            .agg(pl.col("AssignedGPUs").drop_nulls().n_unique().alias("n"))
            .group_by("GPUs_DeviceName")
            .agg(pl.col("n").sum().alias("total"))
            .sort("GPUs_DeviceName")
            .collect(engine="streaming")
        )
        total = (
            prevented.group_by("bucket")
            .agg(pl.col("AssignedGPUs").drop_nulls().n_unique().alias("n"))
            .select(pl.col("n").sum())
            .collect()
            .item()
            or 0
        )
        per_class_avg[class_name] = total / num_buckets if num_buckets else 0.0
        per_class_device_avg[class_name] = {
            str(row["GPUs_DeviceName"]): row["total"] / num_buckets
            for row in per_device.iter_rows(named=True)
            if row["total"] > 0
        }

    return {
        "has_prevent_jobs": True,
        "num_hosts": totals["num_hosts"][0],
        "num_unique_gpus": totals["num_gpus"][0],
        "per_host": per_host,
        "by_reason": by_reason,
        "per_class_avg": per_class_avg,
        "per_class_device_avg": per_class_device_avg,
        "pj_buckets": len(pj_buckets),
        "total_buckets": num_buckets,
    }


def calculate_draining_stats(df: pl.DataFrame) -> dict:
    """
    Calculate summary statistics for drained GPUs.

    Args:
        df: DataFrame with draining data (Machine, AssignedGPUs, timestamp)

    Returns:
        Dictionary with draining summary and per-host breakdown
    """
    no_draining = {
        "has_draining": False,
        "num_hosts": 0,
        "num_unique_gpus": 0,
        "num_intervals": 0,
        "total_hours": 0.0,
        "per_host": {},
    }
    if df.height == 0:
        return no_draining

    keys = ["Machine", "AssignedGPUs"]
    intervals = (
        df.sort([*keys, "timestamp"])
        .with_columns(
            (pl.col("timestamp").diff().over(keys) > pl.duration(minutes=20)).fill_null(False).alias("_new_interval")
        )
        .with_columns(pl.col("_new_interval").cum_sum().over(keys).alias("interval_id"))
        .group_by([*keys, "interval_id"])
        .agg(pl.col("timestamp").min().alias("start"), pl.col("timestamp").max().alias("end"))
        # Single data point: assume 15 min duration
        .with_columns(
            pl.when(pl.col("end") == pl.col("start"))
            .then(pl.col("start") + pl.duration(minutes=15))
            .otherwise(pl.col("end"))
            .alias("end")
        )
        .with_columns(((pl.col("end") - pl.col("start")).dt.total_seconds() / 3600).alias("duration_hours"))
        .sort([*keys, "start"])
    )

    per_host = {}
    for (machine,), machine_data in intervals.group_by("Machine", maintain_order=True):
        gpu_details = {}
        for (gpu_id,), gpu_data in machine_data.group_by("AssignedGPUs", maintain_order=True):
            gpu_details[str(gpu_id)] = {
                "num_intervals": gpu_data.height,
                "total_hours": gpu_data["duration_hours"].sum(),
            }
        per_host[machine] = {
            "num_gpus": machine_data["AssignedGPUs"].n_unique(),
            "num_intervals": machine_data.height,
            "total_hours": machine_data["duration_hours"].sum(),
            "gpu_details": gpu_details,
        }

    return {
        "has_draining": True,
        "num_hosts": intervals["Machine"].n_unique(),
        "num_unique_gpus": intervals["AssignedGPUs"].n_unique(),
        "num_intervals": intervals.height,
        "total_hours": intervals["duration_hours"].sum(),
        "per_host": per_host,
    }


def calculate_monthly_summary(data_dir: str, end_time: datetime.datetime | None = None) -> dict:
    """
    Calculate complete monthly GPU usage summary for the previous month.

    Args:
        data_dir: Directory containing gpu_state Parquet files
        end_time: Optional end time (defaults to latest data)

    Returns:
        Dictionary containing monthly usage statistics
    """
    import calendar

    if end_time is None:
        end_time = get_latest_timestamp(data_dir)
        if end_time is None:
            end_time = datetime.datetime.now()

    # Calculate previous month range
    current_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end = current_month - datetime.timedelta(seconds=1)
    prev_month_start = prev_month_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    days_in_month = calendar.monthrange(prev_month_start.year, prev_month_start.month)[1]
    total_hours = days_in_month * 24

    print(f"Calculating monthly summary for {prev_month_start.strftime('%B %Y')}")
    print(f"Period: {prev_month_start} to {prev_month_end}")
    print(f"Total hours in month: {total_hours}")

    lf = scan_time_filtered(data_dir, total_hours, prev_month_end + datetime.timedelta(seconds=1))
    frames = prepare_frames(lf)

    if frames.original_count == 0:
        return {
            "error": f"No data found for {prev_month_start.strftime('%B %Y')}",
            "month": prev_month_start.strftime("%B %Y"),
            "start_date": prev_month_start,
            "end_date": prev_month_end,
            "total_hours": total_hours,
        }

    return {
        "month": prev_month_start.strftime("%B %Y"),
        "start_date": prev_month_start,
        "end_date": prev_month_end,
        "total_hours": total_hours,
        "device_stats": calculate_allocation_usage_by_device_enhanced(frames, "", False),
        "memory_stats": calculate_allocation_usage_by_memory(frames, "", False),
        "h200_user_stats": calculate_h200_user_breakdown(frames, "", total_hours),
        "data_coverage": {
            "start_time": frames.start_time,
            "end_time": frames.end_time,
            "total_records": frames.original_count,
            "unique_intervals": frames.total_buckets,
        },
    }


# ---------------------------------------------------------------------------
# pandas-based calculations retained for small-window paths and scripts
# ---------------------------------------------------------------------------


def calculate_allocation_usage(df: pd.DataFrame, host: str = "") -> dict:
    """
    Calculate allocation-based usage: percentage of available GPUs that are claimed,
    averaged across 15-minute intervals.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter

    Returns:
        Dictionary with usage statistics for each class
    """
    # Create 15-minute time buckets
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["15min_bucket"] = df["timestamp"].dt.floor("15min")

    stats = {}

    for utilization_type in UTILIZATION_TYPES:
        interval_usage_percentages = []
        total_claimed_gpus = 0
        total_available_gpus = 0

        # For each 15-minute interval, count unique GPUs
        for bucket in sorted(df["15min_bucket"].unique()):
            bucket_df = df[df["15min_bucket"] == bucket]

            claimed_gpus = len(
                filter_df(bucket_df, utilization_type, "Claimed", host)["AssignedGPUs"].dropna().unique()
            )
            unclaimed_gpus = len(
                filter_df(bucket_df, utilization_type, "Unclaimed", host)["AssignedGPUs"].dropna().unique()
            )

            total_gpus_this_interval = claimed_gpus + unclaimed_gpus

            if total_gpus_this_interval > 0:
                interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                interval_usage_percentages.append(interval_usage)
                total_claimed_gpus += claimed_gpus
                total_available_gpus += total_gpus_this_interval

        # Calculate average usage percentage across all intervals
        avg_usage_percentage = (
            sum(interval_usage_percentages) / len(interval_usage_percentages) if interval_usage_percentages else 0
        )

        # Calculate average GPU counts across intervals
        num_intervals = len(df["15min_bucket"].unique())
        avg_claimed = total_claimed_gpus / num_intervals if num_intervals > 0 else 0
        avg_total = total_available_gpus / num_intervals if num_intervals > 0 else 0

        stats[utilization_type] = {
            "avg_claimed": avg_claimed,
            "avg_total_available": avg_total,
            "allocation_usage_percent": avg_usage_percentage,
            "num_intervals": num_intervals,
        }

    return stats


def calculate_allocation_usage_enhanced(df: pd.DataFrame, host: str = "") -> dict:
    """
    Calculate allocation-based usage with enhanced backfill categories.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter

    Returns:
        Dictionary with usage statistics for each enhanced class
    """
    # Create 15-minute time buckets
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["15min_bucket"] = df["timestamp"].dt.floor("15min")

    stats = {}

    # Utilization types with emphasis on hosted capacity
    utilization_types = [
        "Priority-ResearcherOwned",
        "Priority-CHTCOwned",
        "Shared",
        "Backfill-CHTCOwned",
        "Backfill-ResearcherOwned",
    ]

    for utilization_type in utilization_types:
        interval_usage_percentages = []
        total_claimed_gpus = 0
        total_available_gpus = 0

        # For each 15-minute interval, count unique GPUs
        for bucket in sorted(df["15min_bucket"].unique()):
            bucket_df = df[df["15min_bucket"] == bucket]

            claimed_gpus = len(
                filter_df_enhanced(bucket_df, utilization_type, "Claimed", host)["AssignedGPUs"].dropna().unique()
            )
            unclaimed_gpus = len(
                filter_df_enhanced(bucket_df, utilization_type, "Unclaimed", host)["AssignedGPUs"].dropna().unique()
            )

            total_gpus_this_interval = claimed_gpus + unclaimed_gpus

            if total_gpus_this_interval > 0:
                interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                interval_usage_percentages.append(interval_usage)
                total_claimed_gpus += claimed_gpus
                total_available_gpus += total_gpus_this_interval

        # Calculate average usage percentage across all intervals
        avg_usage_percentage = (
            sum(interval_usage_percentages) / len(interval_usage_percentages) if interval_usage_percentages else 0
        )

        # Calculate average GPU counts across intervals
        num_intervals = len(df["15min_bucket"].unique())
        avg_claimed = total_claimed_gpus / num_intervals if num_intervals > 0 else 0
        avg_total = total_available_gpus / num_intervals if num_intervals > 0 else 0

        stats[utilization_type] = {
            "avg_claimed": avg_claimed,
            "avg_total_available": avg_total,
            "allocation_usage_percent": avg_usage_percentage,
            "num_intervals": num_intervals,
        }

    return stats


def calculate_time_series_usage(df: pd.DataFrame, bucket_minutes: int = 15, host: str = "") -> pd.DataFrame:
    """
    Calculate usage over time in buckets, counting unique GPUs per interval.

    Args:
        df: DataFrame with GPU state data
        bucket_minutes: Size of time buckets in minutes
        host: Optional host filter

    Returns:
        DataFrame with time series usage statistics
    """
    # Create time buckets
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df[f"{bucket_minutes}min_bucket"] = df["timestamp"].dt.floor(f"{bucket_minutes}min")

    time_series_data = []

    for bucket in sorted(df[f"{bucket_minutes}min_bucket"].unique()):
        bucket_df = df[df[f"{bucket_minutes}min_bucket"] == bucket]
        bucket_stats = {"timestamp": bucket}

        for utilization_type in UTILIZATION_TYPES:
            claimed_gpus = len(
                filter_df(bucket_df, utilization_type, "Claimed", host)["AssignedGPUs"].dropna().unique()
            )
            unclaimed_gpus = len(
                filter_df(bucket_df, utilization_type, "Unclaimed", host)["AssignedGPUs"].dropna().unique()
            )

            total_gpus = claimed_gpus + unclaimed_gpus
            usage_percent = (claimed_gpus / total_gpus * 100) if total_gpus > 0 else 0

            bucket_stats[f"{utilization_type.lower()}_claimed"] = claimed_gpus
            bucket_stats[f"{utilization_type.lower()}_total"] = total_gpus
            bucket_stats[f"{utilization_type.lower()}_usage_percent"] = usage_percent

        time_series_data.append(bucket_stats)

    return pd.DataFrame(time_series_data)


def calculate_allocation_usage_by_device(df: pd.DataFrame, host: str = "", include_all_devices: bool = True) -> dict:
    """
    Calculate allocation-based usage grouped by device type, averaged across 15-minute intervals.

    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones

    Returns:
        Dictionary with usage statistics for each class and device type
    """
    # Create 15-minute time buckets
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["15min_bucket"] = df["timestamp"].dt.floor("15min")

    # Get unique device types
    device_types = df["GPUs_DeviceName"].dropna().unique()

    stats = {}

    for utilization_type in UTILIZATION_TYPES:
        stats[utilization_type] = {}

        for device_type in device_types:
            # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
            if not include_all_devices and _is_old_device(device_type):
                continue

            interval_usage_percentages = []
            interval_drained_percentages = []
            total_claimed_gpus = 0
            total_drained_gpus = 0
            total_available_gpus = 0

            # For each 15-minute interval, count unique GPUs of this device type
            for bucket in sorted(df["15min_bucket"].unique()):
                bucket_df = df[df["15min_bucket"] == bucket]

                # Filter by device type
                device_df = bucket_df[bucket_df["GPUs_DeviceName"] == device_type]

                if device_df.empty:
                    continue

                # Get all GPUs for this utilization type, then count claimed vs total
                # to avoid double-counting
                all_gpus_df = filter_df(device_df, utilization_type, "", host)

                # Count unique GPUs (total available for this utilization type)
                unique_gpu_ids = set(all_gpus_df["AssignedGPUs"].dropna().unique())
                total_gpus_this_interval = len(unique_gpu_ids)

                # Count how many of these unique GPUs are currently claimed
                claimed_gpus_df = all_gpus_df[all_gpus_df["State"] == "Claimed"]
                claimed_gpus = len(set(claimed_gpus_df["AssignedGPUs"].dropna().unique()))

                # Count how many of these unique GPUs are currently drained
                drained_gpus_df = all_gpus_df[all_gpus_df["State"] == "Drained"]
                drained_gpus = len(set(drained_gpus_df["AssignedGPUs"].dropna().unique()))

                if total_gpus_this_interval > 0:
                    interval_usage_percentages.append((claimed_gpus / total_gpus_this_interval) * 100)
                    interval_drained_percentages.append((drained_gpus / total_gpus_this_interval) * 100)

                    total_claimed_gpus += claimed_gpus
                    total_drained_gpus += drained_gpus
                    total_available_gpus += total_gpus_this_interval

            if interval_usage_percentages:
                # Average percentages across intervals (correct approach when GPUs can change state)
                # Averaging counts then calculating % is wrong because the same GPU counted as
                # Claimed in one interval and Drained in another adds to both totals
                avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages)
                avg_drained_percentage = (
                    sum(interval_drained_percentages) / len(interval_drained_percentages)
                    if interval_drained_percentages
                    else 0.0
                )

                # Calculate average GPU counts across ALL intervals (including those with 0 usage)
                total_intervals = len(df["15min_bucket"].unique())
                avg_claimed = total_claimed_gpus / total_intervals if total_intervals > 0 else 0
                avg_drained = total_drained_gpus / total_intervals if total_intervals > 0 else 0
                avg_total = total_available_gpus / total_intervals if total_intervals > 0 else 0

                stats[utilization_type][device_type] = {
                    "avg_claimed": avg_claimed,
                    "avg_drained": avg_drained,
                    "avg_total_available": avg_total,
                    "allocation_usage_percent": avg_usage_percentage,
                    "drained_percent": avg_drained_percentage,
                    "num_intervals": total_intervals,
                }

    return stats


def get_gpu_models_at_time(data_dir: str, target_time: datetime.datetime, window_minutes: int = 5) -> list:
    """
    Get all GPU models available at a specific time.

    Args:
        data_dir: Directory containing gpu_state_*.parquet files
        target_time: Time to query for GPU models
        window_minutes: Time window around target_time to search (default: 5 minutes)

    Returns:
        List of GPU model names available at the specified time
    """
    # Define time window
    start_time = target_time - datetime.timedelta(minutes=window_minutes)
    end_time = target_time + datetime.timedelta(minutes=window_minutes)

    glob = parquet_glob(data_dir)
    start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    # Note: datetime strings are derived from internal datetime objects, not user input.
    query = (
        f"SELECT DISTINCT GPUs_DeviceName FROM parquet_scan('{glob}', hive_partitioning=false, union_by_name=true) "
        f"WHERE GPUs_DeviceName IS NOT NULL AND timestamp BETWEEN '{start_str}' AND '{end_str}' "
        "ORDER BY GPUs_DeviceName"
    )

    con = duckdb.connect()
    df = con.execute(query).df()
    con.close()

    return df["GPUs_DeviceName"].tolist()


def get_gpu_model_activity_at_time(
    data_dir: str, gpu_model: str, target_time: datetime.datetime, window_minutes: int = 5
) -> pd.DataFrame:
    """
    Get detailed activity for a specific GPU model at a specific time.

    Args:
        data_dir: Directory containing gpu_state_*.parquet files
        gpu_model: GPU model name (e.g., 'NVIDIA A100-SXM4-80GB')
        target_time: Time to query for activity
        window_minutes: Time window around target_time to search (default: 5 minutes)

    Returns:
        DataFrame with detailed GPU activity information
    """
    # Define time window
    start_time = target_time - datetime.timedelta(minutes=window_minutes)
    end_time = target_time + datetime.timedelta(minutes=window_minutes)

    glob = parquet_glob(data_dir)
    start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    # Note: timestamps are derived from internal datetime objects; gpu_model is user input
    # and is bound as a query parameter below.
    query = (
        "SELECT timestamp, Name, AssignedGPUs, State, GPUs_DeviceName, "
        "GPUsAverageUsage, Machine, RemoteOwner, GlobalJobId, PrioritizedProjects "
        f"FROM parquet_scan('{glob}', hive_partitioning=false, union_by_name=true) "
        f"WHERE GPUs_DeviceName = ? AND timestamp BETWEEN '{start_str}' AND '{end_str}' "
        "ORDER BY timestamp DESC, Machine, AssignedGPUs"
    )

    con = duckdb.connect()
    df = con.execute(query, [gpu_model]).df()
    con.close()

    if len(df) > 0:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df


def analyze_gpu_model_at_time(
    data_dir: str, gpu_model: str, target_time: datetime.datetime, window_minutes: int = 5
) -> dict:
    """
    Analyze what's happening with a specific GPU model at a specific time.

    Args:
        data_dir: Directory containing gpu_state_*.parquet files
        gpu_model: GPU model name
        target_time: Time to analyze
        window_minutes: Time window to search

    Returns:
        Dictionary with analysis results
    """
    df = get_gpu_model_activity_at_time(data_dir, gpu_model, target_time, window_minutes)

    if len(df) == 0:
        return {"error": f"No data found for {gpu_model} around {target_time.strftime('%Y-%m-%d %H:%M:%S')}"}

    # Get the closest timestamp to target
    df["time_diff"] = abs(df["timestamp"] - target_time)
    closest_time = df.loc[df["time_diff"].idxmin(), "timestamp"]

    # Filter to records from the closest timestamp
    snapshot_df = df[df["timestamp"] == closest_time]

    # Analyze the snapshot - count unique GPUs only
    unique_gpus = snapshot_df["AssignedGPUs"].dropna().nunique()

    # Count active GPUs (those actually running jobs with RemoteOwner)
    active_gpus_count = (
        snapshot_df[(snapshot_df["State"] == "Claimed") & (snapshot_df["RemoteOwner"].notna())]["AssignedGPUs"]
        .dropna()
        .nunique()
    )

    # Count idle GPUs (those not running jobs)
    idle_gpus_count = unique_gpus - active_gpus_count

    total_gpus = unique_gpus
    claimed_gpus = active_gpus_count  # Rename for compatibility with existing code
    unclaimed_gpus = idle_gpus_count  # Rename for compatibility with existing code

    # Categorize by utilization class
    priority_gpus = filter_df(snapshot_df, "Priority", "", "")
    shared_gpus = filter_df(snapshot_df, "Shared", "", "")
    backfill_gpus = filter_df(snapshot_df, "Backfill", "", "")

    # Get unique machines
    machines = snapshot_df["Machine"].unique()

    # Calculate utilization stats
    claimed_with_usage = snapshot_df[(snapshot_df["State"] == "Claimed") & (snapshot_df["GPUsAverageUsage"].notna())]

    avg_utilization = claimed_with_usage["GPUsAverageUsage"].mean() if len(claimed_with_usage) > 0 else 0

    # Get job information - ensure unique GPU IDs
    active_jobs_df = snapshot_df[(snapshot_df["State"] == "Claimed") & (snapshot_df["RemoteOwner"].notna())][
        ["RemoteOwner", "GlobalJobId", "AssignedGPUs", "Machine"]
    ].copy()

    # Remove duplicates based on AssignedGPUs, keeping first occurrence
    active_jobs = active_jobs_df.drop_duplicates(subset=["AssignedGPUs"], keep="first")

    # Get inactive GPUs - ensure unique GPU IDs and exclude ones that appear in active jobs
    inactive_gpus_df = snapshot_df[snapshot_df["State"] == "Unclaimed"][
        ["AssignedGPUs", "Machine", "PrioritizedProjects"]
    ].copy()

    # Remove duplicates based on AssignedGPUs, keeping first occurrence
    inactive_gpus_unique = inactive_gpus_df.drop_duplicates(subset=["AssignedGPUs"], keep="first")

    # Get list of GPU IDs that are active (have jobs running)
    active_gpu_ids = set(active_jobs["AssignedGPUs"].dropna().tolist())

    # Filter out GPUs that appear in active jobs list
    inactive_gpus = inactive_gpus_unique[~inactive_gpus_unique["AssignedGPUs"].isin(active_gpu_ids)]

    return {
        "gpu_model": gpu_model,
        "snapshot_time": closest_time,
        "target_time": target_time,
        "window_minutes": window_minutes,
        "summary": {
            "total_gpus": total_gpus,
            "claimed_gpus": claimed_gpus,  # This is now active_gpus_count
            "unclaimed_gpus": unclaimed_gpus,  # This is now idle_gpus_count
            "utilization_percent": (claimed_gpus / total_gpus * 100) if total_gpus > 0 else 0,
            "avg_gpu_usage_percent": avg_utilization * 100 if avg_utilization else 0,
            "num_machines": len(machines),
        },
        "by_class": {
            "Priority": {
                "total": priority_gpus["AssignedGPUs"].dropna().nunique(),
                "claimed": priority_gpus[priority_gpus["State"] == "Claimed"]["AssignedGPUs"].dropna().nunique(),
            },
            "Shared": {
                "total": shared_gpus["AssignedGPUs"].dropna().nunique(),
                "claimed": shared_gpus[shared_gpus["State"] == "Claimed"]["AssignedGPUs"].dropna().nunique(),
            },
            "Backfill": {
                "total": backfill_gpus["AssignedGPUs"].dropna().nunique(),
                "claimed": backfill_gpus[backfill_gpus["State"] == "Claimed"]["AssignedGPUs"].dropna().nunique(),
            },
        },
        "machines": list(machines),
        "active_jobs": active_jobs.to_dict("records") if len(active_jobs) > 0 else [],
        "inactive_gpus": inactive_gpus.to_dict("records") if len(inactive_gpus) > 0 else [],
        "raw_data": snapshot_df,
    }
