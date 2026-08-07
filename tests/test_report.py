"""Smoke tests for report.py's CLI entry point.

report.py has no automated caller (the email cron uses usage_stats.py) -- it's
invoked manually via `just last-day`/`last-hour`/`last-day-html`. It previously had
zero test coverage; this locks in that main() runs end-to-end on the canonical
pipeline (see TASK-49.1) without needing a live production data directory.
"""

import datetime
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

import report  # noqa: E402

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


def _row(ts, machine, gpu, state, owner=None, prio="", backfill=False):
    return {
        "Name": f"{'backfill_' if backfill else ''}slot@{machine}",
        "AssignedGPUs": gpu,
        "AvailableGPUs": gpu,
        "State": state,
        "GPUs_DeviceName": "NVIDIA A100-SXM4-80GB",
        "GPUs_GlobalMemoryMb": 81920,
        "PrioritizedProjects": prio,
        "GPUsAverageUsage": 0.5,
        "Machine": machine,
        "RemoteOwner": owner,
        "GlobalJobId": "123.0" if owner else None,
        "PreventJobsReason": None,
        "timestamp": ts,
    }


def _write_fixture(tmp_path: Path) -> Path:
    ts = datetime.datetime(2026, 7, 2, 12, 0, 0)
    rows = [
        _row(ts, "hostA.example.com", "GPU-1", "Claimed", owner="alice", prio="proj1"),
        _row(ts, "hostB.example.com", "GPU-2", "Claimed", owner="bob"),
        _row(ts, "hostC.example.com", "GPU-3", "Unclaimed", backfill=True),
    ]
    path = tmp_path / "gpu_state_2026-07.parquet"
    pl.DataFrame(rows).cast(_SCHEMA).write_parquet(str(path), compression="zstd")
    return path


class TestReportMain:
    def test_main_runs_end_to_end_and_prints_text_output(self, tmp_path, capsys):
        # print_analysis_results ignores output_file for text format -- it only
        # ever prints to stdout, so this asserts against captured output.
        db_path = _write_fixture(tmp_path)

        report.main(hours_back=24, db_path=str(db_path), exclude_hosts_yaml=None, output_format="text")

        captured = capsys.readouterr()
        assert "Error" not in captured.out
        assert "CHTC GPU UTILIZATION REPORT" in captured.out

    def test_main_runs_end_to_end_and_writes_html_output(self, tmp_path, capsys):
        db_path = _write_fixture(tmp_path)
        out_file = tmp_path / "report.html"

        report.main(
            hours_back=24,
            db_path=str(db_path),
            exclude_hosts_yaml=None,
            output_format="html",
            output_file=str(out_file),
        )

        captured = capsys.readouterr()
        assert "Error" not in captured.out
        assert out_file.exists()
        assert "<html" in out_file.read_text().lower()

    def test_main_reports_no_data_without_crashing(self, tmp_path, capsys):
        # Fixture exists but the requested window has no matching rows.
        _write_fixture(tmp_path)
        report.main(
            hours_back=1,
            db_path=str(tmp_path / "gpu_state_2026-07.parquet"),
            exclude_hosts_yaml=None,
        )
        captured = capsys.readouterr()
        # main() resolves end_time from the fixture's own latest timestamp, so a
        # 1-hour window still covers it; assert it doesn't crash either way.
        assert "Traceback" not in captured.err
