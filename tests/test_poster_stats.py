"""Tests for scripts/poster_stats.py."""

import datetime
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import classify_slots
from classify_slots import derive_columns
from scripts.poster_stats import (
    MAX_GAP,
    collect_parts,
    gpu_snapshots,
    poster_lines,
    round_big,
    snapshot_weights,
    summarize,
)

T0 = datetime.datetime(2026, 3, 2, 12, 0)
STEP = datetime.timedelta(minutes=5)


def _row(gpu, name, state, ts=T0, projects="Lab_A", job=None, user=None, device="NVIDIA A100", machine="host1"):
    return {
        "Name": f"{name}@{machine}.chtc.wisc.edu",
        "Machine": f"{machine}.chtc.wisc.edu",
        "State": state,
        "AssignedGPUs": gpu,
        "PrioritizedProjects": "" if "backfill" in name else projects,
        "GPUs_DeviceName": device,
        "GPUs_GlobalMemoryMb": 40960,
        "RemoteOwner": user,
        "GlobalJobId": job,
        "PreventJobsReason": None,
        "timestamp": ts,
    }


@pytest.fixture(autouse=True)
def _no_exclusions(monkeypatch):
    monkeypatch.setattr(classify_slots, "HOST_EXCLUSIONS", {})


def _raw(rows) -> pl.LazyFrame:
    overrides = {"RemoteOwner": pl.String, "GlobalJobId": pl.String, "PreventJobsReason": pl.String}
    return derive_columns(pl.LazyFrame(rows, schema_overrides=overrides))


def test_snapshot_weights_cap_outages_and_fill_last():
    times = [T0, T0 + STEP, T0 + STEP + datetime.timedelta(hours=3)]
    weights = snapshot_weights(pl.LazyFrame({"timestamp": times + [T0]})).collect()
    assert weights["seconds"].to_list() == [300, MAX_GAP, 300]


def test_gpu_snapshots_categories():
    rows = [
        _row("GPU-own", "slot1_1", "Claimed", job="j1", user="a@x"),
        _row("GPU-bf", "slot1", "Unclaimed"),
        _row("GPU-bf", "backfill1_1", "Claimed", job="j2", user="b@x"),
        _row("GPU-idle", "slot1", "Unclaimed"),
        _row("GPU-open", "slot1_1", "Claimed", projects="", job="j3", user="c@x"),
        _row("GPU-openidle", "slot1", "Unclaimed", projects=""),
        _row("GPU-old", "slot1", "Unclaimed", device="Tesla P100"),
    ]
    snaps = gpu_snapshots(_raw(rows)).collect()
    assert dict(zip(snaps["AssignedGPUs"], snaps["cat"], strict=True)) == {
        "GPU-own": "prio_owner",
        "GPU-bf": "prio_backfill",
        "GPU-idle": "prio_idle",
        "GPU-open": "open_alloc",
        "GPU-openidle": "open_idle",
    }


def test_summarize_hours_jobs_users_and_peak():
    rows = []
    for i in range(12):  # one hour of 5-minute snapshots
        ts = T0 + i * STEP
        rows += [
            _row("GPU-a", "slot1_1", "Claimed", ts=ts, job="ap#1.0", user="a@x"),
            _row("GPU-b", "slot1", "Unclaimed", ts=ts),
            _row("GPU-b", "backfill1_1", "Claimed", ts=ts, job="ap#2.0", user="b@x"),
            _row("GPU-c", "slot1", "Unclaimed", ts=ts, projects="", machine="host2"),
        ]
    rows.append(_row("GPU-c", "slot1_1", "Claimed", ts=T0, projects="", job="ap#3.0", user="a@x", machine="host2"))
    stats = summarize([collect_parts(_raw(rows))])

    assert stats["gpu_hours"]["prio_owner"] == pytest.approx(1.0)
    assert stats["gpu_hours"]["prio_backfill"] == pytest.approx(1.0)
    assert stats["gpu_hours"]["open_alloc"] == pytest.approx(5 / 60)
    assert stats["allocated_gpu_hours"] == pytest.approx(2 + 5 / 60)
    assert stats["prio_owner_pct"] == pytest.approx(50.0)
    assert stats["prio_allocated_pct"] == pytest.approx(100.0)
    assert stats["open_allocated_pct"] == pytest.approx(100 / 12)
    assert (stats["jobs"], stats["users"]) == (3, 2)
    assert (stats["peak_allocated"], stats["median_allocated"]) == (3, 2)
    assert (stats["gpus"], stats["machines"]) == (3, 2)


def test_summarize_merges_files_without_double_counting():
    first = [_row("GPU-a", "slot1_1", "Claimed", ts=T0, job="ap#1.0", user="a@x")]
    second = [_row("GPU-a", "slot1_1", "Claimed", ts=T0 + STEP, job="ap#1.0", user="a@x")]
    stats = summarize([collect_parts(_raw(first)), collect_parts(_raw(second))])
    assert (stats["jobs"], stats["users"], stats["gpus"]) == (1, 1, 1)
    assert stats["gpu_hours"]["prio_owner"] == pytest.approx(10 / 60)


@pytest.mark.parametrize(
    ("n", "text"),
    [(2_262_671, "2.26 million"), (1_106_616, "1.11 million"), (867_257, "867,000"), (12_499, "12,000"), (541, "541")],
)
def test_round_big(n, text):
    assert round_big(n) == text


def test_poster_lines_are_plain_and_unpadded():
    rows = [
        _row("GPU-a", "slot1", "Unclaimed"),
        _row("GPU-a", "backfill1_1", "Claimed", job="ap#1.0", user="a@x"),
    ]
    end = T0 + datetime.timedelta(days=1)
    lines = poster_lines(summarize([collect_parts(_raw(rows))]), T0, end)
    assert lines[0] == "Mar 2026 – Mar 2026"
    assert "1 backfill jobs from 1 users" in lines
    assert all("  " not in line and "\t" not in line for line in lines)
