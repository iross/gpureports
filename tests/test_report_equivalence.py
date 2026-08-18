"""Regression tests guarding SQLite→Parquet report equivalence.

These lock in two behaviours that were found to differ (or be non-deterministic)
when comparing the email-ready HTML reports produced by the SQLite (main) and
Parquet (this branch) backends:

  1. get_draining_data must return drained-but-not-claimed GPUs for a time window,
     including across a month boundary (two Parquet files). The SQLite branch had a
     bug here (isoformat "T" separator vs the space-separated stored text) that
     silently returned an empty draining section.

  2. The "Real Slots by Memory Category" table must order categories deterministically
     ("<48GB" before an exact "48GB"). The two categories previously produced an equal
     sort key, so the rendered order depended on the physical row order returned by the
     storage backend — which differs between SQLite and Parquet.
"""

import datetime
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from classify_slots import calculate_prevent_jobs_stats, prepare_frames  # noqa: E402
from read_data import get_draining_data, get_time_filtered_data  # noqa: E402
from reporting import generate_html_report  # noqa: E402

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


def _row(ts, machine, gpu, state, mem_mb=40960, prevent_jobs_reason=None):
    return {
        "Name": f"slot@{machine}",
        "AssignedGPUs": gpu,
        "AvailableGPUs": gpu,
        "State": state,
        "GPUs_DeviceName": "NVIDIA A100-SXM4-80GB",
        "GPUs_GlobalMemoryMb": mem_mb,
        "PrioritizedProjects": None,
        "GPUsAverageUsage": 0.0,
        "Machine": machine,
        "RemoteOwner": None,
        "GlobalJobId": None,
        "PreventJobsReason": prevent_jobs_reason,
        "timestamp": ts,
    }


def _write(path, rows):
    pl.DataFrame(rows).cast(_SCHEMA).write_parquet(str(path), compression="zstd")


class TestDrainingData:
    def test_drained_not_claimed_returned(self, tmp_path):
        ts = datetime.datetime(2026, 7, 2, 12, 0, 0)
        rows = [
            _row(ts, "hostA", "GPU-1", "Drained"),  # drained, not claimed -> counted
            _row(ts, "hostB", "GPU-2", "Drained"),  # drained AND claimed at same ts -> excluded
            _row(ts, "hostB", "GPU-2", "Claimed"),
        ]
        _write(tmp_path / "gpu_state_2026-07.parquet", rows)

        end = datetime.datetime(2026, 7, 3, 0, 0, 0)
        df = get_draining_data(str(tmp_path), hours_back=24, end_time=end)

        pairs = set(zip(df["Machine"], df["AssignedGPUs"], strict=False))
        assert ("hostA", "GPU-1") in pairs
        assert ("hostB", "GPU-2") not in pairs

    def test_draining_across_month_boundary(self, tmp_path):
        """A window spanning two monthly Parquet files must see drained GPUs in both."""
        _write(
            tmp_path / "gpu_state_2026-06.parquet",
            [_row(datetime.datetime(2026, 6, 30, 12, 0, 0), "hostJun", "GPU-J", "Drained")],
        )
        _write(
            tmp_path / "gpu_state_2026-07.parquet",
            [_row(datetime.datetime(2026, 7, 1, 12, 0, 0), "hostJul", "GPU-K", "Drained")],
        )
        end = datetime.datetime(2026, 7, 2, 0, 0, 0)
        df = get_draining_data(str(tmp_path), hours_back=48, end_time=end)
        machines = set(df["Machine"])
        assert {"hostJun", "hostJul"} <= machines


class TestCrossMonthWindow:
    def test_time_window_spans_two_files(self, tmp_path):
        _write(
            tmp_path / "gpu_state_2026-06.parquet",
            [_row(datetime.datetime(2026, 6, 30, 20, 0, 0), "hostJun", "GPU-J", "Claimed")],
        )
        _write(
            tmp_path / "gpu_state_2026-07.parquet",
            [_row(datetime.datetime(2026, 7, 1, 4, 0, 0), "hostJul", "GPU-K", "Claimed")],
        )
        end = datetime.datetime(2026, 7, 1, 12, 0, 0)
        df = get_time_filtered_data(str(tmp_path), hours_back=24, end_time=end)
        machines = set(df["Machine"])
        assert {"hostJun", "hostJul"} <= machines


class TestMemoryCategorySortDeterminism:
    """The rendered memory-category order must not depend on dict insertion order."""

    @staticmethod
    def _results(memory_insertion_order):
        mem = {"avg_claimed": 30.0, "avg_drained": 0.0, "avg_total_available": 66.0}
        return {
            "metadata": {
                "hours_back": 24,
                "start_time": "2026-07-02 08:40:00",
                "end_time": "2026-07-03 08:35:00",
                "num_intervals": 97,
                "total_records": 1,
                "excluded_hosts": {},
                "filtered_hosts_info": [],
            },
            "device_stats": {
                "Priority-CHTCOwned": {
                    "NVIDIA A100-SXM4-80GB": {
                        "avg_claimed": 1.0,
                        "avg_total_available": 2.0,
                        "avg_drained": 0.0,
                    }
                }
            },
            "memory_stats": {cat: dict(mem) for cat in memory_insertion_order},
            "draining_stats": {"has_draining": False},
        }

    def test_less_than_48_sorts_before_48(self):
        lt = "<td style='font-weight: bold;'><48GB</td>"
        eq = "<td style='font-weight: bold;'>48GB</td>"
        for order in (["<48GB", "48GB"], ["48GB", "<48GB"]):
            html = generate_html_report(self._results(order))
            i_lt, i_eq = html.find(lt), html.find(eq)
            assert i_lt != -1 and i_eq != -1, f"memory rows missing for insertion {order}"
            assert i_lt < i_eq, f"'<48GB' must render before '48GB' for insertion {order}"


def _frames(rows, drop_prevent_jobs_column=False):
    """Build PreparedFrames from test rows (optionally without the PJ column)."""
    df = pl.DataFrame(rows).cast(_SCHEMA)
    if drop_prevent_jobs_column:
        df = df.drop("PreventJobsReason")
    return prepare_frames(df.lazy())


class TestPreventJobsStats:
    """calculate_prevent_jobs_stats correctly detects and counts blocked GPUs."""

    _ts = datetime.datetime(2026, 7, 2, 12, 0, 0)

    def test_detect_prevent_jobs_reason(self):
        """Rows with PreventJobsReason are counted; rows without are excluded."""
        frames = _frames(
            [
                _row(self._ts, "hostA", "GPU-1", "Claimed", prevent_jobs_reason="GPUHealthy == False"),
                _row(self._ts, "hostA", "GPU-2", "Claimed", prevent_jobs_reason="GPUHealthy == False"),
                _row(self._ts, "hostB", "GPU-3", "Unclaimed"),  # no reason → excluded
            ]
        )
        result = calculate_prevent_jobs_stats(frames)
        assert result["has_prevent_jobs"] is True
        assert result["num_hosts"] == 1
        assert result["num_unique_gpus"] == 2
        assert "hostA" in result["per_host"]
        assert result["per_host"]["hostA"]["num_gpus"] == 2
        assert "GPUHealthy == False" in result["per_host"]["hostA"]["reasons"]
        assert result["per_host"]["hostA"]["active"] is True
        assert result["per_host"]["hostA"]["last_seen"] == "2026-07-02 12:00"

    def test_lifted_reason_marked_inactive_but_kept_in_window_average(self):
        """A reason lifted mid-window shows active=False and still feeds per_class_avg.

        The host was idle+prevented in the first of two buckets only: per_host keeps it
        (whole-window summary), active flips to False because a later bucket has PJ
        data without it, and per_class_avg reflects the partial-window coverage.
        """
        ts2 = self._ts + datetime.timedelta(minutes=15)
        rows = [
            # bucket 1: both hosts idle + prevented
            {**_row(self._ts, "hostA", "GPU-A", "Unclaimed", prevent_jobs_reason="INF-1"), "PrioritizedProjects": ""},
            {**_row(self._ts, "hostB", "GPU-B", "Unclaimed", prevent_jobs_reason="INF-2"), "PrioritizedProjects": ""},
            # bucket 2: hostA's reason lifted (GPU back to work), hostB still prevented
            {**_row(ts2, "hostA", "GPU-A", "Claimed"), "PrioritizedProjects": ""},
            {**_row(ts2, "hostB", "GPU-B", "Unclaimed", prevent_jobs_reason="INF-2"), "PrioritizedProjects": ""},
        ]
        result = calculate_prevent_jobs_stats(_frames(rows))
        assert result["per_host"]["hostA"]["active"] is False
        assert result["per_host"]["hostA"]["last_seen"] == "2026-07-02 12:00"
        assert result["per_host"]["hostB"]["active"] is True
        # average: hostA 1/2 buckets + hostB 2/2 buckets
        assert result["per_class_avg"]["Shared"] == 1.5

    def test_empty_string_excluded(self):
        """PreventJobsReason='' is treated the same as absent."""
        frames = _frames(
            [
                _row(self._ts, "hostA", "GPU-1", "Claimed", prevent_jobs_reason=""),
                _row(self._ts, "hostA", "GPU-1", "Claimed", prevent_jobs_reason="   "),
            ]
        )
        result = calculate_prevent_jobs_stats(frames)
        assert result["has_prevent_jobs"] is False

    def test_claimed_gpus_excluded_from_per_class_counts(self):
        """A GPU running a job (Claimed on any slot) is Allocated, not Prevented.

        PreventJobsReason does not evict running jobs — it only stops new ones —
        so per_class_avg must count only idle prevented GPUs. A GPU that is
        prevented on its primary slot but Claimed on a backfill slot is busy and
        must also be excluded.
        """
        rows = [
            # Idle prevented GPU on a Shared machine -> counted
            {
                **_row(self._ts, "hostA", "GPU-idle", "Owner", prevent_jobs_reason="INF-1"),
                "PrioritizedProjects": "",
            },
            # Prevented GPU that is Claimed (still finishing a job) -> excluded
            {
                **_row(self._ts, "hostA", "GPU-busy", "Claimed", prevent_jobs_reason="INF-1"),
                "PrioritizedProjects": "",
            },
            # Prevented on primary slot, but the same GPU is Claimed on a backfill slot -> excluded
            {
                **_row(self._ts, "hostB", "GPU-bf", "Owner", prevent_jobs_reason="INF-2"),
                "PrioritizedProjects": "",
            },
            {
                **_row(self._ts, "hostB", "GPU-bf", "Claimed"),
                "Name": "backfill1_1@hostB",
                "PrioritizedProjects": "",
            },
        ]
        result = calculate_prevent_jobs_stats(_frames(rows))
        assert result["per_class_avg"]["Shared"] == 1.0  # only GPU-idle
        # The summary (attribute-is-set) counts still include all three GPUs
        assert result["num_unique_gpus"] == 3

    def test_idle_backfill_duplicate_does_not_hide_prevented_gpu(self):
        """An idle prevented GPU also offered as an idle backfill slot is still Prevented.

        The dedup ranking must keep the prevented primary row over an idle backfill
        row; only a Claimed backfill slot may displace it.
        """
        rows = [
            {
                **_row(self._ts, "hostA", "GPU-1", "Unclaimed", prevent_jobs_reason="INF-1"),
                "PrioritizedProjects": "",
            },
            {
                **_row(self._ts, "hostA", "GPU-1", "Unclaimed"),
                "Name": "backfill1_1@hostA",
                "PrioritizedProjects": "",
            },
        ]
        result = calculate_prevent_jobs_stats(_frames(rows))
        assert result["per_class_avg"]["Shared"] == 1.0

    def test_missing_column_returns_false(self):
        """Data without a PreventJobsReason column returns has_prevent_jobs=False."""
        frames = _frames(
            [_row(self._ts, "hostA", "GPU-1", "Claimed")],
            drop_prevent_jobs_column=True,
        )
        result = calculate_prevent_jobs_stats(frames)
        assert result["has_prevent_jobs"] is False
        assert result["num_hosts"] == 0

    def test_html_section_rendered(self):
        """HTML report includes PreventJobsReason section with host table."""
        results = {
            "metadata": {
                "hours_back": 24,
                "start_time": "2026-07-02 08:40:00",
                "end_time": "2026-07-03 08:35:00",
                "num_intervals": 97,
                "total_records": 1,
                "excluded_hosts": {},
                "filtered_hosts_info": [],
            },
            "device_stats": {
                "Priority-CHTCOwned": {
                    "NVIDIA A100-SXM4-80GB": {
                        "avg_claimed": 1.0,
                        "avg_total_available": 2.0,
                        "avg_drained": 0.0,
                    }
                }
            },
            "memory_stats": {},
            "draining_stats": {"has_draining": False},
            "prevent_jobs_stats": {
                "has_prevent_jobs": True,
                "num_hosts": 1,
                "num_unique_gpus": 2,
                "per_host": {
                    "hostA": {
                        "num_gpus": 2,
                        "reasons": ["GPUHealthy == False"],
                        "last_seen": "2026-07-02 12:00",
                        "active": False,
                    },
                },
                "by_reason": {"GPUHealthy == False": {"num_hosts": 1, "num_gpus": 2}},
                "per_class_avg": {"Priority-CHTCOwned": 1.5},
            },
        }
        html = generate_html_report(results)
        assert "PreventJobsReason" in html
        assert "hostA" in html
        assert "GPUHealthy == False" in html
        assert "Prevented (avg.)" in html
        assert "Lifted" in html
        assert "2026-07-02 12:00" in html
        assert "1.5" in html  # per_class_avg rendered in the Prevented (avg.) column
