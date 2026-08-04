"""Regression test guarding dashboard/data.py's parquet scans against schema drift.

_query_dbs and get_opencap_users_data used to call pl.scan_parquet() with no explicit
schema. That's fine as long as every file scanned individually happens to have the
exact same column order/set as the code expects, but breaks the moment a file has
columns reordered or is missing a newer column (e.g. PreventJobsReason, added after
some files were already written) -- exactly the scenario stats_data.py's
GPU_STATE_SCHEMA/missing_columns="insert"/SCAN_CAST_OPTIONS combination exists to
tolerate.
"""

import datetime
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.data import _query_dbs  # noqa: E402


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
