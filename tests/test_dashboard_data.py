"""Regression tests for dashboard/data.py's parquet scans and dedup ranking.

_query_dbs used to call pl.scan_parquet() with no explicit schema. That's fine as long
as every file scanned individually happens to have the exact same column order/set as
the code expects, but breaks the moment a file has columns reordered or is missing a
newer column (e.g. PreventJobsReason, added after some files were already written) --
exactly the scenario read_data.py's GPU_STATE_SCHEMA/missing_columns="insert"/
SCAN_CAST_OPTIONS combination exists to tolerate.

Dashboard's dedup/classify used to be its own hand-rolled implementation (removed in
TASK-49.1 in favor of classify_slots.prepare_frames()); _collapse_to_bucket_winner
is the one piece of dashboard-specific logic that remains -- picking the single slot row
a heatmap cell shows. It deliberately differs from the canonical rank: a claimed backfill
slot beats an unclaimed primary, since the primary stays Unclaimed while backfill runs.
"""

import datetime
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import classify_slots  # noqa: E402
from dashboard.data import (  # noqa: E402
    STATE_CODES,
    _collapse_to_bucket_winner,
    _map_state_codes,
    _query_dbs,
    get_heatmap_data,
)


def test_query_dbs_tolerates_reordered_and_missing_columns(tmp_path):
    ts = datetime.datetime(2026, 5, 15, 10, 0, 0)

    # Older-style file: missing PrioritizedProjects (one of the columns dashboard/data.py
    # selects) as well as PreventJobsReason, and columns in a different order than
    # GPU_STATE_SCHEMA declares. Without schema=GPU_STATE_SCHEMA/missing_columns="insert",
    # .select(COLUMNS) raises ColumnNotFoundError for the missing PrioritizedProjects.
    old_df = pl.DataFrame(
        {
            "timestamp": [ts],
            "State": ["Claimed"],
            "Name": ["slot1@gpu1.chtc.wisc.edu"],
            "AssignedGPUs": ["GPU-1A"],
            "Machine": ["gpu1.chtc.wisc.edu"],
            "GPUs_DeviceName": ["NVIDIA A100-SXM4-40GB"],
        }
    )
    old_path = tmp_path / "gpu_state_2025-01.parquet"
    old_df.write_parquet(str(old_path))

    # Current-style file: full column set, standard order.
    new_df = pl.DataFrame(
        {
            "Name": ["slot1@gpu2.chtc.wisc.edu"],
            "AssignedGPUs": ["GPU-2A"],
            "AvailableGPUs": ["GPU-2A"],
            "State": ["Claimed"],
            "GPUs_DeviceName": ["NVIDIA H200"],
            "GPUs_GlobalMemoryMb": [143771],
            "PrioritizedProjects": ["ProjB"],
            "GPUsAverageUsage": [0.5],
            "Machine": ["gpu2.chtc.wisc.edu"],
            "RemoteOwner": ["bob"],
            "GlobalJobId": ["200.0"],
            "PreventJobsReason": [None],
            "timestamp": [ts + datetime.timedelta(minutes=15)],
        }
    )
    new_path = tmp_path / "gpu_state_2026-05.parquet"
    new_df.write_parquet(str(new_path))

    start = ts - datetime.timedelta(hours=1)
    end = ts + datetime.timedelta(hours=1)

    combined, warnings = _query_dbs([(str(old_path), "parquet"), (str(new_path), "parquet")], start, end)

    assert warnings == []
    assert combined.height == 2
    assert set(combined["Name"]) == {"slot1@gpu1.chtc.wisc.edu", "slot1@gpu2.chtc.wisc.edu"}


def _slot_rows(names: list[str], states: list[str], pj_set: list[bool] | None = None) -> pl.DataFrame:
    bucket = datetime.datetime(2026, 5, 15, 10, 0, 0)
    n = len(names)
    return pl.DataFrame(
        {
            "bucket": [bucket] * n,
            "AssignedGPUs": ["GPU-1A"] * n,
            "Name": names,
            "State": states,
            "_is_bf": ["backfill" in name for name in names],
            "_pj_set": pj_set or [False] * n,
        }
    )


PRIMARY = "slot1@gpu1.chtc.wisc.edu"
BACKFILL = "slot1_backfill@gpu1.chtc.wisc.edu"


@pytest.mark.parametrize(
    ("primary_state", "primary_pj", "backfill_state", "winner"),
    [
        # A running backfill job leaves the GPU's primary slot Unclaimed; the
        # heatmap must show the backfill job, not an idle GPU.
        ("Unclaimed", False, "Claimed", BACKFILL),
        ("Unclaimed", True, "Claimed", BACKFILL),
        ("Drained", False, "Claimed", BACKFILL),
        ("Claimed", False, "Claimed", PRIMARY),
        ("Claimed", False, "Unclaimed", PRIMARY),
        ("Unclaimed", False, "Unclaimed", PRIMARY),
        ("Drained", False, "Unclaimed", PRIMARY),
    ],
)
def test_collapse_to_bucket_winner_picks_displayed_slot(primary_state, primary_pj, backfill_state, winner):
    rows = _slot_rows([PRIMARY, BACKFILL], [primary_state, backfill_state], [primary_pj, False])

    result = _collapse_to_bucket_winner(rows)

    assert result.height == 1
    assert result["Name"][0] == winner


def test_heatmap_shows_busy_backfill_when_primary_slot_is_unclaimed(tmp_path, monkeypatch):
    monkeypatch.setattr(classify_slots, "load_chtc_owned_hosts", lambda: set())
    ts = datetime.datetime(2026, 5, 15, 10, 0, 0)
    pl.DataFrame(
        {
            "Name": [PRIMARY, BACKFILL],
            "AssignedGPUs": ["GPU-1A", "GPU-1A"],
            "State": ["Unclaimed", "Claimed"],
            "PrioritizedProjects": ["ProjA", "ProjA"],
            "Machine": ["gpu1.chtc.wisc.edu"] * 2,
            "GPUs_DeviceName": ["NVIDIA H200"] * 2,
            "RemoteOwner": [None, "alice"],
            "PreventJobsReason": [None, None],
            "timestamp": [ts, ts],
        }
    ).write_parquet(str(tmp_path / "gpu_state_2026-05.parquet"))

    data = get_heatmap_data(start=ts - datetime.timedelta(minutes=5), end=ts, bucket_minutes=15, base_dir=str(tmp_path))

    assert data["machines"][0]["gpus"][0]["states"] == [STATE_CODES["busy_backfill"]]


def test_map_state_codes_covers_all_six_states_and_na_fallback():
    """_map_state_codes derives the dashboard's 6 STATE_CODES from
    prepare_frames()'s _is_bf/_pp_prio/State columns (TASK-49.1); this pins the
    mapping against every combination the old hand-rolled _classify_states covered,
    plus the "na" fallback for a state that's neither Claimed nor Unclaimed."""
    rows = [
        # (State, _is_bf, _pp_prio) -> expected state code
        ("Claimed", True, False, "busy_backfill"),
        ("Claimed", True, True, "busy_backfill"),  # backfill wins over priority when claimed
        ("Claimed", False, True, "busy_prioritized"),
        ("Claimed", False, False, "busy_shared"),
        ("Unclaimed", True, False, "idle_backfill"),
        ("Unclaimed", True, True, "idle_backfill"),
        ("Unclaimed", False, True, "idle_prioritized"),
        ("Unclaimed", False, False, "idle_shared"),
        ("Drained", False, False, "na"),
    ]
    df = pl.DataFrame(
        {
            "State": [r[0] for r in rows],
            "_is_bf": [r[1] for r in rows],
            "_pp_prio": [r[2] for r in rows],
        }
    )
    result = _map_state_codes(df)
    expected = [STATE_CODES[r[3]] for r in rows]
    assert result["state_code"].to_list() == expected
