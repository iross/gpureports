"""Tests for Parquet-based gpu_state storage.

Covers:
  - gpu_utils_polars: Parquet file discovery and latest-timestamp lookup
  - collector: collector write, append, and atomic rename
  - dashboard/data.py: _query_dbs reading Parquet (and SQLite fallback)
"""

import datetime
import sqlite3
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "dashboard"))

# htcondor is only available on the production host; stub it out for tests.
_htcondor_stub = ModuleType("htcondor")
_htcondor_stub.Collector = MagicMock()
_htcondor_stub.AdTypes = MagicMock()
sys.modules.setdefault("htcondor", _htcondor_stub)

from gpu_utils_polars import get_latest_timestamp_from_most_recent_parquet, get_required_parquet_files  # noqa: E402

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

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
    "timestamp": pl.Datetime("us"),
}


def _make_rows(ts: datetime.datetime, n: int = 3) -> pl.DataFrame:
    rows = [
        {
            "Name": f"slot{i}@host{i}.example.com",
            "AssignedGPUs": f"GPU-{i:03d}",
            "AvailableGPUs": f"GPU-{i:03d}",
            "State": "Claimed",
            "GPUs_DeviceName": "Tesla A100",
            "GPUs_GlobalMemoryMb": 40960,
            "PrioritizedProjects": "proj1",
            "GPUsAverageUsage": 0.5,
            "Machine": f"host{i}.example.com",
            "RemoteOwner": f"user{i}",
            "GlobalJobId": f"123{i}.0",
            "timestamp": ts,
        }
        for i in range(n)
    ]
    return pl.DataFrame(rows).cast(_SCHEMA)


# ---------------------------------------------------------------------------
# gpu_utils_polars: get_required_parquet_files
# ---------------------------------------------------------------------------


class TestGetRequiredParquetFiles:
    def test_single_month_parquet(self, tmp_path):
        (tmp_path / "gpu_state_2026-04.parquet").touch()
        start = datetime.datetime(2026, 4, 1)
        end = datetime.datetime(2026, 4, 15)
        result = get_required_parquet_files(start, end, str(tmp_path))
        assert result == [(str(tmp_path / "gpu_state_2026-04.parquet"), "parquet")]

    def test_falls_back_to_sqlite(self, tmp_path):
        (tmp_path / "gpu_state_2026-04.db").touch()
        start = datetime.datetime(2026, 4, 1)
        end = datetime.datetime(2026, 4, 15)
        result = get_required_parquet_files(start, end, str(tmp_path))
        assert result == [(str(tmp_path / "gpu_state_2026-04.db"), "sqlite")]

    def test_prefers_parquet_over_sqlite(self, tmp_path):
        (tmp_path / "gpu_state_2026-04.parquet").touch()
        (tmp_path / "gpu_state_2026-04.db").touch()
        start = datetime.datetime(2026, 4, 1)
        end = datetime.datetime(2026, 4, 15)
        result = get_required_parquet_files(start, end, str(tmp_path))
        assert len(result) == 1
        assert result[0][1] == "parquet"

    def test_multi_month_span(self, tmp_path):
        (tmp_path / "gpu_state_2026-03.parquet").touch()
        (tmp_path / "gpu_state_2026-04.parquet").touch()
        start = datetime.datetime(2026, 3, 28)
        end = datetime.datetime(2026, 4, 4)
        result = get_required_parquet_files(start, end, str(tmp_path))
        assert len(result) == 2
        assert all(fmt == "parquet" for _, fmt in result)

    def test_mixed_parquet_and_sqlite(self, tmp_path):
        (tmp_path / "gpu_state_2026-03.db").touch()
        (tmp_path / "gpu_state_2026-04.parquet").touch()
        start = datetime.datetime(2026, 3, 28)
        end = datetime.datetime(2026, 4, 4)
        result = get_required_parquet_files(start, end, str(tmp_path))
        assert result[0] == (str(tmp_path / "gpu_state_2026-03.db"), "sqlite")
        assert result[1] == (str(tmp_path / "gpu_state_2026-04.parquet"), "parquet")

    def test_missing_month_skipped(self, tmp_path):
        (tmp_path / "gpu_state_2026-04.parquet").touch()
        start = datetime.datetime(2026, 3, 28)
        end = datetime.datetime(2026, 4, 4)
        result = get_required_parquet_files(start, end, str(tmp_path))
        assert len(result) == 1
        assert result[0][1] == "parquet"

    def test_empty_directory(self, tmp_path):
        start = datetime.datetime(2026, 4, 1)
        end = datetime.datetime(2026, 4, 15)
        assert get_required_parquet_files(start, end, str(tmp_path)) == []


# ---------------------------------------------------------------------------
# gpu_utils_polars: get_latest_timestamp_from_most_recent_parquet
# ---------------------------------------------------------------------------


class TestGetLatestTimestampParquet:
    def test_reads_max_from_parquet(self, tmp_path):
        ts = datetime.datetime(2026, 4, 10, 12, 0, 0)
        df = _make_rows(ts)
        df.write_parquet(str(tmp_path / "gpu_state_2026-04.parquet"), compression="zstd")
        result = get_latest_timestamp_from_most_recent_parquet(str(tmp_path))
        assert result is not None
        assert result.replace(tzinfo=None) == ts

    def test_prefers_parquet_over_sqlite(self, tmp_path):
        ts_parquet = datetime.datetime(2026, 4, 20, 0, 0, 0)
        ts_sqlite = datetime.datetime(2026, 4, 15, 0, 0, 0)

        df = _make_rows(ts_parquet)
        df.write_parquet(str(tmp_path / "gpu_state_2026-04.parquet"), compression="zstd")

        conn = sqlite3.connect(str(tmp_path / "gpu_state_2026-04.db"))
        _make_rows(ts_sqlite).with_columns(pl.col("timestamp").cast(pl.Utf8)).to_pandas().to_sql(
            "gpu_state", conn, index=False
        )
        conn.close()

        result = get_latest_timestamp_from_most_recent_parquet(str(tmp_path))
        assert result is not None
        assert result.replace(tzinfo=None) == ts_parquet

    def test_falls_back_to_sqlite(self, tmp_path):
        ts = datetime.datetime(2026, 4, 10, 8, 0, 0)
        import pandas as pd

        df_pd = pd.DataFrame([{"timestamp": str(ts), "State": "Claimed", "Name": "slot1", "AssignedGPUs": "GPU-001"}])
        conn = sqlite3.connect(str(tmp_path / "gpu_state_2026-04.db"))
        df_pd.to_sql("gpu_state", conn, index=False)
        conn.close()

        result = get_latest_timestamp_from_most_recent_parquet(str(tmp_path))
        assert result is not None

    def test_returns_none_when_empty(self, tmp_path):
        result = get_latest_timestamp_from_most_recent_parquet(str(tmp_path))
        assert result is None


# ---------------------------------------------------------------------------
# Collector: write, append, atomic rename
# ---------------------------------------------------------------------------


class TestCollectorParquetWrite:
    def _run_main(self, tmp_path, rows: pl.DataFrame):
        """Invoke the collector's main() with a pre-built DataFrame."""
        import collector

        with patch.object(collector, "get_gpus", return_value=rows):
            collector.main(str(tmp_path))

    def test_fresh_write_creates_parquet(self, tmp_path):
        import collector

        ts = datetime.datetime.now().replace(microsecond=0)
        rows = _make_rows(ts, n=5)
        with patch.object(collector, "get_gpus", return_value=rows):
            collector.main(str(tmp_path))

        month = ts.strftime("%Y-%m")
        parquet_path = tmp_path / f"gpu_state_{month}.parquet"
        assert parquet_path.exists()
        result = pl.read_parquet(str(parquet_path))
        assert result.height == 5
        assert set(rows.columns).issubset(set(result.columns))

    def test_no_sqlite_file_created(self, tmp_path):
        import collector

        ts = datetime.datetime.now()
        rows = _make_rows(ts)
        with patch.object(collector, "get_gpus", return_value=rows):
            collector.main(str(tmp_path))

        assert list(tmp_path.glob("*.db")) == []

    def test_append_adds_rows(self, tmp_path):
        import collector

        ts1 = datetime.datetime(2026, 5, 1, 10, 0, 0)
        ts2 = datetime.datetime(2026, 5, 1, 10, 5, 0)
        rows1 = _make_rows(ts1, n=3)
        rows2 = _make_rows(ts2, n=4)

        with (
            patch.object(collector, "get_gpus", return_value=rows1),
            patch.object(collector, "_current_month", return_value="2026-05"),
        ):
            collector.main(str(tmp_path))
        with (
            patch.object(collector, "get_gpus", return_value=rows2),
            patch.object(collector, "_current_month", return_value="2026-05"),
        ):
            collector.main(str(tmp_path))

        result = pl.read_parquet(str(tmp_path / "gpu_state_2026-05.parquet"))
        assert result.height == 7

    def test_no_temp_file_left_after_write(self, tmp_path):
        import collector

        ts = datetime.datetime(2026, 5, 1, 10, 0, 0)
        rows = _make_rows(ts)
        with (
            patch.object(collector, "get_gpus", return_value=rows),
            patch.object(collector, "_current_month", return_value="2026-05"),
        ):
            collector.main(str(tmp_path))

        assert list(tmp_path.glob("*.tmp.parquet")) == []

    def test_column_schema_preserved(self, tmp_path):
        import collector

        ts = datetime.datetime.now().replace(microsecond=0)
        rows = _make_rows(ts)
        with patch.object(collector, "get_gpus", return_value=rows):
            collector.main(str(tmp_path))

        month = ts.strftime("%Y-%m")
        result = pl.read_parquet(str(tmp_path / f"gpu_state_{month}.parquet"))
        for col in ["Name", "AssignedGPUs", "State", "Machine", "timestamp"]:
            assert col in result.columns


# ---------------------------------------------------------------------------
# dashboard/data.py: _query_dbs with Parquet and SQLite fallback
# ---------------------------------------------------------------------------


class TestDashboardQueryDbs:
    _DASH_COLUMNS = [
        "Name",
        "AssignedGPUs",
        "State",
        "PrioritizedProjects",
        "Machine",
        "GPUs_DeviceName",
        "timestamp",
    ]

    def _make_parquet(self, tmp_path: Path, month: str, rows: pl.DataFrame) -> Path:
        p = tmp_path / f"gpu_state_{month}.parquet"
        rows.select(self._all_cols(rows)).write_parquet(str(p), compression="zstd")
        return p

    def _all_cols(self, df: pl.DataFrame) -> list[str]:
        return [c for c in self._DASH_COLUMNS if c in df.columns]

    def _make_sqlite(self, tmp_path: Path, month: str, rows: pl.DataFrame) -> Path:
        p = tmp_path / f"gpu_state_{month}.db"
        conn = sqlite3.connect(str(p))
        rows.with_columns(pl.col("timestamp").cast(pl.Utf8)).to_pandas().to_sql(
            "gpu_state", conn, index=False, if_exists="replace"
        )
        conn.close()
        return p

    def test_reads_parquet(self, tmp_path):
        import data as dash_data

        ts = datetime.datetime(2026, 4, 10, 12, 0, 0)
        rows = _make_rows(ts, n=6)
        self._make_parquet(tmp_path, "2026-04", rows)

        start = datetime.datetime(2026, 4, 10, 0, 0, 0)
        end = datetime.datetime(2026, 4, 10, 23, 59, 59)
        result = dash_data._query_dbs([(str(tmp_path / "gpu_state_2026-04.parquet"), "parquet")], start, end)
        assert result.height == 6

    def test_sqlite_fallback(self, tmp_path):
        import data as dash_data

        ts = datetime.datetime(2026, 4, 10, 12, 0, 0)
        rows = _make_rows(ts, n=4)
        self._make_sqlite(tmp_path, "2026-04", rows)

        start = datetime.datetime(2026, 4, 10, 0, 0, 0)
        end = datetime.datetime(2026, 4, 10, 23, 59, 59)
        result = dash_data._query_dbs([(str(tmp_path / "gpu_state_2026-04.db"), "sqlite")], start, end)
        assert result.height == 4

    def test_filters_by_time_range(self, tmp_path):
        import data as dash_data

        rows_in = _make_rows(datetime.datetime(2026, 4, 10, 12, 0, 0), n=3)
        rows_out = _make_rows(datetime.datetime(2026, 4, 8, 0, 0, 0), n=2)
        all_rows = pl.concat([rows_in, rows_out])
        self._make_parquet(tmp_path, "2026-04", all_rows)

        start = datetime.datetime(2026, 4, 10, 0, 0, 0)
        end = datetime.datetime(2026, 4, 10, 23, 59, 59)
        result = dash_data._query_dbs([(str(tmp_path / "gpu_state_2026-04.parquet"), "parquet")], start, end)
        assert result.height == 3

    def test_empty_when_no_files(self, tmp_path):
        import data as dash_data

        start = datetime.datetime(2026, 4, 10, 0, 0, 0)
        end = datetime.datetime(2026, 4, 10, 23, 59, 59)
        result = dash_data._query_dbs([], start, end)
        assert result.height == 0
