"""Regression tests for dashboard/data.py's parquet scans and dedup ranking.

_query_dbs used to call pl.scan_parquet() with no explicit schema. That's fine as long
as every file scanned individually happens to have the exact same column order/set as
the code expects, but breaks the moment a file has columns reordered or is missing a
newer column (e.g. PreventJobsReason, added after some files were already written) --
exactly the scenario read_data.py's GPU_STATE_SCHEMA/missing_columns="insert"/
SCAN_CAST_OPTIONS combination exists to tolerate.

Dashboard's dedup/classify used to be its own hand-rolled implementation (removed in
TASK-49.1 in favor of classify_slots.prepare_frames()); _collapse_to_bucket_winner
is the one piece of dashboard-specific logic that remains -- picking a single row per
display bucket when prepare_frames()'s per-raw-timestamp dedup leaves more than one
winner in the same bucket -- and it must agree with the canonical rank tiers.
"""

import datetime
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from classify_slots import slot_dedup_rank  # noqa: E402
from dashboard.data import STATE_CODES, _collapse_to_bucket_winner, _map_state_codes, _query_dbs  # noqa: E402


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


def test_collapse_to_bucket_winner_matches_canonical_rank_on_backfill_vs_primary():
    """A pre-task-47 4-tier rank picked claimed-backfill over unclaimed-primary
    (backwards from classify_slots.slot_dedup_rank, which ranks unclaimed-primary(5)
    above claimed-backfill(3)). Same GPU, same display bucket, one primary slot
    Unclaimed and one backfill slot Claimed (as prepare_frames()'s per-timestamp dedup
    would emit if both were the sole survivor of their own raw timestamp): the collapse
    step must pick the primary row.
    """
    bucket = datetime.datetime(2026, 5, 15, 10, 0, 0)
    dedup = pl.DataFrame(
        {
            "bucket": [bucket, bucket],
            "AssignedGPUs": ["GPU-1A", "GPU-1A"],
            "Name": ["slot1@gpu1.chtc.wisc.edu", "slot1_backfill@gpu1.chtc.wisc.edu"],
            "State": ["Unclaimed", "Claimed"],
            "_is_bf": [False, True],
            "_pj_set": [False, False],
        }
    )

    result = _collapse_to_bucket_winner(dedup)

    assert result.height == 1
    assert result["Name"][0] == "slot1@gpu1.chtc.wisc.edu"

    # Cross-check directly against the canonical rank tiers.
    ranked = dedup.with_columns(slot_dedup_rank(pl.col("_is_bf"), pl.col("State"), pl.lit(False)).alias("_rank"))
    assert ranked.filter(pl.col("Name") == "slot1@gpu1.chtc.wisc.edu")["_rank"][0] == 5
    assert ranked.filter(pl.col("Name") == "slot1_backfill@gpu1.chtc.wisc.edu")["_rank"][0] == 3


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
