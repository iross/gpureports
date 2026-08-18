"""Regression tests comparing the pre-migration pandas pipeline against the
current polars pipeline, on identical input.

The polars migration (task-40) replaced stats_calculations.py's (now
classify_slots.py's) calculation functions in place (same names, pandas
DataFrame -> PreparedFrames), so the old implementation no longer exists in
the live tree to compare against. The task's implementation notes describe a
one-time manual comparison on 24h/168h windows, with no automated test
locking in the result and no coverage of the monthly summary path.
legacy_stats_data.py and legacy_stats_calculations.py are a frozen, verbatim
copy of stats_data.py/stats_calculations.py as of commit c972c6c (the last
commit before the migration) kept only so this test can run both
implementations side by side and catch future drift.

Two differences are intentional (documented in the task-40 notes) and are
special-cased below instead of asserted equal:
  - monthly data_coverage.unique_intervals: pandas never populated the
    15min_bucket column before this check, so it always reported 0; polars
    reports the real bucket count.
  - metadata.filtered_hosts_info shape (not exercised here since this test
    doesn't configure host exclusions).
"""

import datetime
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import legacy_stats_calculations as legacy_calc  # noqa: E402
import legacy_stats_data as legacy_data  # noqa: E402

import classify_slots as calc  # noqa: E402
import read_data as data  # noqa: E402

_SCHEMA = {
    "Name": pl.Utf8,
    "AssignedGPUs": pl.Utf8,
    "AvailableGPUs": pl.Utf8,
    "State": pl.Utf8,
    "GPUs_DeviceName": pl.Utf8,
    "GPUs_GlobalMemoryMb": pl.Int64,
    "PrioritizedProjects": pl.Utf8,
    "GPUsAverageUsage": pl.Float64,
    "Machine": pl.Utf8,
    "RemoteOwner": pl.Utf8,
    "GlobalJobId": pl.Utf8,
    "PreventJobsReason": pl.Utf8,
    "timestamp": pl.Datetime("us"),
}


def _row(ts, machine, slot, gpu, state, device, mem_mb, prioritized="", owner=None, job_id=None, pj=None):
    return {
        "Name": f"{slot}@{machine}",
        "AssignedGPUs": gpu,
        "AvailableGPUs": gpu,
        "State": state,
        "GPUs_DeviceName": device,
        "GPUs_GlobalMemoryMb": mem_mb,
        "PrioritizedProjects": prioritized,
        "GPUsAverageUsage": 0.5,
        "Machine": machine,
        "RemoteOwner": owner,
        "GlobalJobId": job_id,
        "PreventJobsReason": pj,
        "timestamp": ts,
    }


def _build_rows():
    """A window covering three hosts/classes/device types across two buckets:

    - gpu1 (researcher-owned, A100): primary Claimed+Unclaimed->Drained, a
      backfill slot that flips Unclaimed->Claimed (exercises drained_percent
      and Backfill-ResearcherOwned via the researcher-machine join).
    - gpu2 (CHTC-owned, H200): primary stays Claimed by "bob" both buckets
      (h200_user_stats), a backfill slot that flips Claimed->Unclaimed
      (Backfill-CHTCOwned, a second user "dave").
    - gpu3 (shared, old-GPU-type P100): two GPUs never claimed (zero-active
      machine) with PreventJobsReason set on one of them (prevent_jobs_stats).
    """
    b1 = datetime.datetime(2026, 5, 15, 10, 0, 0)
    b2 = datetime.datetime(2026, 5, 15, 10, 15, 0)
    a100 = "NVIDIA A100-SXM4-40GB"
    h200 = "NVIDIA H200"
    p100 = "Tesla P100-PCIE-16GB"
    return [
        _row(b1, "gpu1.chtc.wisc.edu", "slot1", "GPU-1A", "Claimed", a100, 40960, "ProjA", "alice", "100.0"),
        _row(b1, "gpu1.chtc.wisc.edu", "slot2", "GPU-1B", "Unclaimed", a100, 40960, "ProjA"),
        _row(b1, "gpu1.chtc.wisc.edu", "slot1_backfill", "GPU-1C", "Unclaimed", a100, 40960, ""),
        _row(b2, "gpu1.chtc.wisc.edu", "slot1", "GPU-1A", "Claimed", a100, 40960, "ProjA", "alice", "100.0"),
        _row(b2, "gpu1.chtc.wisc.edu", "slot2", "GPU-1B", "Drained", a100, 40960, "ProjA"),
        _row(b2, "gpu1.chtc.wisc.edu", "slot1_backfill", "GPU-1C", "Claimed", a100, 40960, "", "carol", "101.0"),
        _row(b1, "gpu2.chtc.wisc.edu", "slot1", "GPU-2A", "Claimed", h200, 143771, "ProjB", "bob", "200.0"),
        _row(b1, "gpu2.chtc.wisc.edu", "slot1_backfill", "GPU-2B", "Claimed", h200, 143771, "", "dave", "201.0"),
        _row(b2, "gpu2.chtc.wisc.edu", "slot1", "GPU-2A", "Claimed", h200, 143771, "ProjB", "bob", "200.0"),
        _row(b2, "gpu2.chtc.wisc.edu", "slot1_backfill", "GPU-2B", "Unclaimed", h200, 143771, ""),
        _row(b1, "gpu3.chtc.wisc.edu", "slot1", "GPU-3A", "Unclaimed", p100, 16384, "", pj="GPUHealthy == False"),
        _row(b1, "gpu3.chtc.wisc.edu", "slot2", "GPU-3B", "Unclaimed", p100, 16384, ""),
        _row(b2, "gpu3.chtc.wisc.edu", "slot1", "GPU-3A", "Unclaimed", p100, 16384, "", pj="GPUHealthy == False"),
        _row(b2, "gpu3.chtc.wisc.edu", "slot2", "GPU-3B", "Unclaimed", p100, 16384, ""),
    ]


@pytest.fixture
def parquet_data_dir(tmp_path, monkeypatch):
    """Write the synthetic window to gpu_state_2026-05.parquet and mark gpu2
    as CHTC-owned so the CHTCOwned/ResearcherOwned split is exercised on both
    sides of the comparison (read_data/classify_slots state is shared by both
    implementations)."""
    monkeypatch.setattr(data, "_CHTC_OWNED_HOSTS", {"gpu2.chtc.wisc.edu"})
    monkeypatch.setattr(calc, "HOST_EXCLUSIONS", {})
    df = pl.DataFrame(_build_rows()).cast(_SCHEMA)
    df.write_parquet(str(tmp_path / "gpu_state_2026-05.parquet"))
    return str(tmp_path)


def _assert_deep_equal(a, b, path="root"):
    """Recursively compare, tolerating float noise from pandas/polars summing
    in different orders."""
    if isinstance(a, float) or isinstance(b, float):
        assert a == pytest.approx(b, abs=1e-9), f"{path}: {a!r} != {b!r}"
    elif isinstance(a, dict):
        assert isinstance(b, dict) and a.keys() == b.keys(), f"{path}: {a!r} != {b!r}"
        for k in a:
            _assert_deep_equal(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list | tuple):
        assert isinstance(b, list | tuple) and len(a) == len(b), f"{path}: {a!r} != {b!r}"
        for i, (x, y) in enumerate(zip(a, b, strict=True)):
            _assert_deep_equal(x, y, f"{path}[{i}]")
    elif isinstance(a, set):
        assert a == b, f"{path}: {a!r} != {b!r}"
    else:
        assert a == b, f"{path}: {a!r} != {b!r}"


class TestDailyWeeklyParity:
    """emailer.sh's daily/weekly/test modes call run_analysis with
    group_by_device=True, which drives device/memory/h200/backfill-user/
    zero-active/prevent-jobs through prepare_frames(). Compare each against
    the frozen pandas baseline on identical input."""

    HOURS_BACK = 1
    END_TIME = datetime.datetime(2026, 5, 15, 10, 30, 0)

    @pytest.fixture(autouse=True)
    def _frames(self, parquet_data_dir):
        self.old_df = legacy_data.get_time_filtered_data(parquet_data_dir, self.HOURS_BACK, self.END_TIME)
        new_lf = data.scan_time_filtered(parquet_data_dir, self.HOURS_BACK, self.END_TIME)
        self.new_frames = calc.prepare_frames(new_lf)

    @pytest.mark.parametrize("include_all_devices", [True, False])
    def test_device_stats_parity(self, include_all_devices):
        old = legacy_calc.calculate_allocation_usage_by_device_enhanced(self.old_df, "", include_all_devices)
        new = calc.calculate_allocation_usage_by_device_enhanced(self.new_frames, "", include_all_devices)
        _assert_deep_equal(old, new)

    @pytest.mark.parametrize("include_all_devices", [True, False])
    def test_memory_stats_parity(self, include_all_devices):
        old = legacy_calc.calculate_allocation_usage_by_memory(self.old_df, "", include_all_devices)
        new = calc.calculate_allocation_usage_by_memory(self.new_frames, "", include_all_devices)
        _assert_deep_equal(old, new)

    def test_h200_user_stats_parity(self):
        old = legacy_calc.calculate_h200_user_breakdown(self.old_df, "", self.HOURS_BACK)
        new = calc.calculate_h200_user_breakdown(self.new_frames, "", self.HOURS_BACK)
        _assert_deep_equal(old, new)

    def test_backfill_user_stats_parity(self):
        old = legacy_calc.calculate_backfill_usage_by_user(self.old_df, "", self.HOURS_BACK, False)
        new = calc.calculate_backfill_usage_by_user(self.new_frames, "", self.HOURS_BACK, False)
        _assert_deep_equal(old, new)

    def test_zero_active_machines_parity(self):
        old = legacy_calc.calculate_machines_with_zero_active_gpus(self.old_df, "", True)
        new = calc.calculate_machines_with_zero_active_gpus(self.new_frames, "", True)
        _assert_deep_equal(old, new)

    def test_prevent_jobs_stats_parity(self):
        old = legacy_calc.calculate_prevent_jobs_stats(self.old_df)
        new = calc.calculate_prevent_jobs_stats(self.new_frames)
        _assert_deep_equal(old, new)


class TestMonthlySummaryParity:
    """emailer.sh's monthly mode calls calculate_monthly_summary directly;
    the task-40 manual validation covered only 24h/168h windows, not this
    path, so this is the first check it agrees with the pandas baseline."""

    END_TIME = datetime.datetime(2026, 6, 1, 0, 0, 0)  # previous month = May 2026

    def test_monthly_summary_matches_except_documented_diff(self, parquet_data_dir):
        old = legacy_calc.calculate_monthly_summary(parquet_data_dir, self.END_TIME)
        new = calc.calculate_monthly_summary(parquet_data_dir, self.END_TIME)

        assert old["month"] == new["month"] == "May 2026"
        assert old["start_date"] == new["start_date"]
        assert old["end_date"] == new["end_date"]
        assert old["total_hours"] == new["total_hours"]
        _assert_deep_equal(old["device_stats"], new["device_stats"])
        _assert_deep_equal(old["memory_stats"], new["memory_stats"])
        _assert_deep_equal(old["h200_user_stats"], new["h200_user_stats"])

        assert old["data_coverage"]["total_records"] == new["data_coverage"]["total_records"]
        assert old["data_coverage"]["start_time"] == new["data_coverage"]["start_time"]
        assert old["data_coverage"]["end_time"] == new["data_coverage"]["end_time"]

        # Documented intentional difference (task-40 notes): pandas never
        # populated 15min_bucket before this check, so it always reported 0.
        assert old["data_coverage"]["unique_intervals"] == 0
        assert new["data_coverage"]["unique_intervals"] == 2
