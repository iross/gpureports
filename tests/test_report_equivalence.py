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

from stats_calculations import calculate_prevent_jobs_stats  # noqa: E402
from stats_data import get_draining_data, get_time_filtered_data  # noqa: E402
from stats_reporting import generate_html_report  # noqa: E402

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


class TestPreventJobsStats:
    """calculate_prevent_jobs_stats correctly detects and counts blocked GPUs."""

    _ts = datetime.datetime(2026, 7, 2, 12, 0, 0)

    def _make_df(self, rows):
        import pandas as pd

        return pd.DataFrame(rows).astype(
            {
                "Name": "object",
                "AssignedGPUs": "object",
                "State": "object",
                "Machine": "object",
                "PreventJobsReason": "object",
            }
        )

    def test_detect_prevent_jobs_reason(self):
        """Rows with PreventJobsReason are counted; rows without are excluded."""
        df = self._make_df(
            [
                _row(self._ts, "hostA", "GPU-1", "Claimed", prevent_jobs_reason="GPUHealthy == False"),
                _row(self._ts, "hostA", "GPU-2", "Claimed", prevent_jobs_reason="GPUHealthy == False"),
                _row(self._ts, "hostB", "GPU-3", "Unclaimed"),  # no reason → excluded
            ]
        )
        result = calculate_prevent_jobs_stats(df)
        assert result["has_prevent_jobs"] is True
        assert result["num_hosts"] == 1
        assert result["num_unique_gpus"] == 2
        assert "hostA" in result["per_host"]
        assert result["per_host"]["hostA"]["num_gpus"] == 2
        assert "GPUHealthy == False" in result["per_host"]["hostA"]["reasons"]

    def test_empty_string_excluded(self):
        """PreventJobsReason='' is treated the same as absent."""
        df = self._make_df(
            [
                _row(self._ts, "hostA", "GPU-1", "Claimed", prevent_jobs_reason=""),
                _row(self._ts, "hostA", "GPU-1", "Claimed", prevent_jobs_reason="   "),
            ]
        )
        result = calculate_prevent_jobs_stats(df)
        assert result["has_prevent_jobs"] is False

    def test_missing_column_returns_false(self):
        """DataFrame without PreventJobsReason column returns has_prevent_jobs=False."""
        import pandas as pd

        df = pd.DataFrame([{"AssignedGPUs": "GPU-1", "Machine": "hostA", "State": "Claimed"}])
        result = calculate_prevent_jobs_stats(df)
        assert result["has_prevent_jobs"] is False
        assert result["num_hosts"] == 0

    def test_html_section_rendered(self):
        """HTML report includes PreventJobsReason section with orange header and host table."""
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
                    "hostA": {"num_gpus": 2, "reasons": ["GPUHealthy == False"]},
                },
                "by_reason": {"GPUHealthy == False": {"num_hosts": 1, "num_gpus": 2}},
            },
        }
        html = generate_html_report(results)
        assert "PreventJobsReason" in html
        assert "e65100" in html  # orange colour
        assert "hostA" in html
        assert "GPUHealthy == False" in html
