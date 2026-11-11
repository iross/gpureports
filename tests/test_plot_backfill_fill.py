"""Tests for scripts/plot_backfill_fill.py."""

import datetime
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import classify_slots
from classify_slots import derive_columns
from scripts.plot_backfill_fill import (
    CHUNK,
    FLEETS,
    MIN_DAYS,
    bucket_counts,
    data_gaps,
    gpu_states,
    headline_stats,
    open_stats,
    weekly_states,
)

PRIO = FLEETS["prioritized"]
OPEN = FLEETS["open"]

T0 = datetime.datetime(2026, 3, 2, 12, 0)  # a Monday


def _row(gpu, name, state, ts=T0, projects="Lab_A", device="NVIDIA A100", prevent=None):
    return {
        "Name": f"{name}@host1.chtc.wisc.edu",
        "Machine": "host1.chtc.wisc.edu",
        "State": state,
        "AssignedGPUs": gpu,
        "PrioritizedProjects": "" if "backfill" in name else projects,
        "GPUs_DeviceName": device,
        "GPUs_GlobalMemoryMb": 40960,
        "RemoteOwner": None,
        "PreventJobsReason": prevent,
        "timestamp": ts,
    }


@pytest.fixture(autouse=True)
def _no_exclusions(monkeypatch):
    monkeypatch.setattr(classify_slots, "HOST_EXCLUSIONS", {})


def _raw(rows) -> pl.LazyFrame:
    schema_overrides = {"RemoteOwner": pl.String, "PreventJobsReason": pl.String}
    return derive_columns(pl.LazyFrame(rows, schema_overrides=schema_overrides))


def _states(rows, fleet=PRIO) -> dict[str, str]:
    per_gpu = gpu_states(_raw(rows), fleet).collect()
    return dict(zip(per_gpu["AssignedGPUs"], per_gpu["state"], strict=True))


def test_owner_claim_wins_over_concurrent_backfill_claim():
    states = _states([_row("GPU-a", "slot1_1", "Claimed"), _row("GPU-a", "backfill1_1", "Claimed")])
    assert states == {"GPU-a": "owner"}


def test_backfill_claim_on_unclaimed_primary_counts_as_backfill():
    states = _states([_row("GPU-a", "slot1", "Unclaimed"), _row("GPU-a", "backfill1_1", "Claimed")])
    assert states == {"GPU-a": "backfill"}


@pytest.mark.parametrize(
    ("state", "prevent"),
    [("Drained", None), ("Owner", None), ("Unclaimed", "Node is being serviced"), ("Unclaimed", None)],
)
def test_any_unclaimed_gpu_counts_as_idle(state, prevent):
    assert _states([_row("GPU-a", "slot1", state, prevent=prevent)]) == {"GPU-a": "idle"}


def test_claim_anywhere_in_bucket_counts():
    later = T0 + datetime.timedelta(minutes=5)
    rows = [_row("GPU-a", "slot1", "Unclaimed"), _row("GPU-a", "slot1_1", "Claimed", ts=later)]
    assert _states(rows) == {"GPU-a": "owner"}


def test_open_capacity_and_old_devices_excluded():
    rows = [
        _row("GPU-shared", "slot1", "Unclaimed", projects=""),
        _row("GPU-old", "slot1", "Unclaimed", device="Tesla P100"),
        _row("GPU-prio", "slot1", "Unclaimed"),
    ]
    assert _states(rows) == {"GPU-prio": "idle"}


def _counts(days: int, owner: int = 0, backfill: int = 0, idle: int = 1) -> pl.DataFrame:
    buckets = [T0 + datetime.timedelta(days=d) for d in range(days)]
    return pl.DataFrame(
        {"bucket": buckets, "owner": [owner] * days, "backfill": [backfill] * days, "idle": [idle] * days}
    )


def test_bucket_counts_counts_each_gpu_once_per_bucket():
    rows = [
        _row("GPU-a", "slot1_1", "Claimed"),
        _row("GPU-b", "slot1", "Unclaimed"),
        _row("GPU-b", "backfill1_1", "Claimed"),
        _row("GPU-c", "slot1", "Unclaimed"),
        _row("GPU-c", "slot1", "Unclaimed", ts=T0 + datetime.timedelta(minutes=5)),
    ]
    counts = bucket_counts(_raw(rows), T0, T0 + datetime.timedelta(hours=1), PRIO)
    assert counts.to_dicts() == [{"bucket": T0, "owner": 1, "backfill": 1, "idle": 1}]


def test_bucket_counts_chunking_matches_one_pass():
    start = T0 + datetime.timedelta(minutes=7)  # not bucket-aligned
    edge = datetime.datetime.combine(start.date(), datetime.time()) + CHUNK
    times = [
        start,
        edge - datetime.timedelta(minutes=1),
        edge,
        edge + datetime.timedelta(minutes=1),
        start + CHUNK - datetime.timedelta(minutes=1),  # same bucket as an unaligned edge
        start + CHUNK + datetime.timedelta(minutes=1),
        edge + CHUNK,
    ]
    rows = [_row("GPU-a", "slot1", "Unclaimed", ts=t) for t in times]
    rows += [_row("GPU-b", "slot1_1", "Claimed", ts=t) for t in times]
    end = times[-1]
    counts = bucket_counts(_raw(rows), start, end, PRIO)
    one_pass = (
        gpu_states(_raw(rows), PRIO)
        .group_by("bucket")
        .agg((pl.col("state") == k).sum().cast(pl.Int64).alias(k) for k in ("owner", "backfill", "idle"))
        .sort("bucket")
        .collect()
    )
    assert counts.equals(one_pass)
    assert counts["bucket"].is_unique().all()
    assert counts["idle"].to_list() == [1] * 5


def test_bucket_counts_empty_window():
    rows = [_row("GPU-a", "slot1", "Unclaimed")]
    later = T0 + datetime.timedelta(days=30)
    assert bucket_counts(_raw(rows), later, later + datetime.timedelta(days=1), PRIO).is_empty()


def test_weekly_states_drops_weeks_with_too_few_days():
    weekly = weekly_states(_counts(7 + MIN_DAYS - 1))
    assert weekly["week_start"].to_list() == [datetime.datetime(2026, 3, 2)]
    assert weekly_states(_counts(7 + MIN_DAYS)).height == 2
    assert weekly.row(0, named=True)["idle"] == 1.0
    assert weekly.row(0, named=True)["owner"] == 0.0


def test_weekly_states_averages_gpu_counts_per_bucket():
    weekly = weekly_states(_counts(7, backfill=1, idle=1))
    row = weekly.row(0, named=True)
    assert (row["idle"], row["backfill"], row["owner"]) == (1.0, 1.0, 0.0)


def _totals(owner: int, backfill: int, idle: int) -> pl.DataFrame:
    return pl.DataFrame({"owner": [owner], "backfill": [backfill], "idle": [idle]})


def test_headline_stats():
    stats = headline_stats(_totals(owner=2, backfill=3, idle=1))
    assert stats["backfill_fill_pct"] == pytest.approx(75.0)
    assert stats["allocated_pct"] == pytest.approx(500 / 6)
    assert stats["owner_pct"] == pytest.approx(200 / 6)


def test_headline_stats_zero_denominators():
    assert headline_stats(_totals(1, 0, 0))["backfill_fill_pct"] is None
    empty = pl.DataFrame(schema={"owner": pl.Int64, "backfill": pl.Int64, "idle": pl.Int64})
    assert headline_stats(empty) == {
        "backfill_fill_pct": None,
        "allocated_pct": None,
        "owner_pct": None,
    }
    assert headline_stats(_totals(0, 1, 0))["backfill_fill_pct"] == 100.0


def test_open_fleet_keeps_only_shared_non_interactive_gpus():
    rows = [
        _row("GPU-shared", "slot1", "Unclaimed", projects=""),
        _row("GPU-prio", "slot1", "Unclaimed"),
        _row("GPU-inter", "slot1_interactive", "Unclaimed", projects=""),
        _row("GPU-old", "slot1", "Unclaimed", projects="", device="Tesla P100"),
    ]
    assert _states(rows, OPEN) == {"GPU-shared": "idle"}


@pytest.mark.parametrize("name", ["slot1_1", "backfill1_1"])
def test_open_fleet_any_claimed_slot_counts_as_allocated(name):
    rows = [_row("GPU-a", "slot1", "Unclaimed", projects=""), _row("GPU-a", name, "Claimed", projects="")]
    assert _states(rows, OPEN) == {"GPU-a": "allocated"}


def test_open_bucket_counts_and_weekly_columns():
    rows = [
        _row("GPU-a", "slot1_1", "Claimed", projects=""),
        _row("GPU-b", "slot1", "Unclaimed", projects=""),
        _row("GPU-prio", "slot1_1", "Claimed"),
    ]
    counts = bucket_counts(_raw(rows), T0, T0 + datetime.timedelta(hours=1), OPEN)
    assert counts.to_dicts() == [{"bucket": T0, "allocated": 1, "idle": 1}]
    assert weekly_states(counts).columns == ["week_start", "allocated", "idle", "n_days"]


def test_open_stats():
    counts = pl.DataFrame({"allocated": [3, 0], "idle": [1, 0]})
    assert open_stats(counts) == {"allocated_pct": 75.0}
    assert open_stats(counts.head(0)) == {"allocated_pct": None}


def test_data_gaps_reports_only_outages_over_a_day():
    buckets = [T0, T0 + datetime.timedelta(hours=20), T0 + datetime.timedelta(days=4)]
    counts = pl.DataFrame({"bucket": buckets})
    assert data_gaps(counts) == ["Mar 3 – Mar 6 2026"]
    assert data_gaps(counts.head(2)) == []
