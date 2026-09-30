#!/usr/bin/env python3
"""
Tests for the job_pressure pipeline: interval bookkeeping in get_job_pressure.py, the
DuckDB loaders in read_data.py, and the SQLite -> Parquet conversion.
"""

import datetime
import os
import sqlite3
import sys

import polars as pl

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from get_job_pressure import record_jobs, should_query_schedd, update_intervals  # noqa: E402
from migrate_job_pressure import _merge_stream, _migrate  # noqa: E402
from read_data import JOB_PRESSURE_SCHEMA, attribute_claimed_jobs, load_job_pressure  # noqa: E402

STALE = 5400
T0 = 1_780_000_000


def job(gid="ap1#1.0#1", state="idle", project="Proj_A", owner="alice", **extra):
    base = {
        "GlobalJobId": gid,
        "ScheddName": "ap1",
        "Owner": owner,
        "RequestGPUs": 1.0,
        "RequestCPUs": 4.0,
        "RequestMemory": 8000.0,
        "RequestGPUMemory": None,
        "QDate": T0 - 100,
        "ChtcProjects": project,
        "JobState": state,
    }
    return base | extra


def empty():
    return pl.DataFrame(schema=JOB_PRESSURE_SCHEMA)


def rows(df):
    return {(r["GlobalJobId"], r["JobState"], r["first_seen"], r["last_seen"]) for r in df.iter_rows(named=True)}


class TestUpdateIntervals:
    def test_new_job_opens_interval(self):
        df = update_intervals(empty(), [job()], T0, STALE)
        assert rows(df) == {("ap1#1.0#1", "idle", T0, T0)}

    def test_continuing_job_extends_last_seen_only(self):
        df = update_intervals(empty(), [job()], T0, STALE)
        df = update_intervals(df, [job()], T0 + 1800, STALE)
        assert rows(df) == {("ap1#1.0#1", "idle", T0, T0 + 1800)}

    def test_job_gone_beyond_stale_window_opens_new_interval(self):
        df = update_intervals(empty(), [job()], T0, STALE)
        df = update_intervals(df, [job()], T0 + STALE + 1, STALE)
        assert rows(df) == {("ap1#1.0#1", "idle", T0, T0), ("ap1#1.0#1", "idle", T0 + STALE + 1, T0 + STALE + 1)}

    def test_job_returning_exactly_at_stale_boundary_is_continuing(self):
        df = update_intervals(empty(), [job()], T0, STALE)
        df = update_intervals(df, [job()], T0 + STALE, STALE)
        assert rows(df) == {("ap1#1.0#1", "idle", T0, T0 + STALE)}

    def test_idle_to_running_keeps_idle_interval_closed(self):
        df = update_intervals(empty(), [job(state="idle")], T0, STALE)
        df = update_intervals(df, [job(state="running")], T0 + 1800, STALE)
        df = update_intervals(df, [job(state="running")], T0 + 3600, STALE)
        assert rows(df) == {("ap1#1.0#1", "idle", T0, T0), ("ap1#1.0#1", "running", T0 + 1800, T0 + 3600)}

    def test_duplicate_job_in_one_poll_yields_one_row(self):
        df = update_intervals(empty(), [job(), job()], T0, STALE)
        assert len(df) == 1

    def test_untouched_jobs_are_not_extended(self):
        df = update_intervals(empty(), [job("ap1#1.0#1"), job("ap1#2.0#1")], T0, STALE)
        df = update_intervals(df, [job("ap1#1.0#1")], T0 + 1800, STALE)
        assert rows(df) == {("ap1#1.0#1", "idle", T0, T0 + 1800), ("ap1#2.0#1", "idle", T0, T0)}


class TestRecordJobs:
    def test_round_trip_and_no_temp_files_visible(self, tmp_path):
        path = tmp_path / "job_pressure_2026-09.parquet"
        record_jobs([job()], path, T0, STALE)
        record_jobs([job(), job("ap1#2.0#1", state="running")], path, T0 + 1800, STALE)

        assert rows(pl.read_parquet(path)) == {
            ("ap1#1.0#1", "idle", T0, T0 + 1800),
            ("ap1#2.0#1", "running", T0 + 1800, T0 + 1800),
        }
        assert sorted(p.name for p in tmp_path.glob("job_pressure_*.parquet")) == [path.name]

    def test_all_null_column_stays_readable_by_duckdb(self, tmp_path):
        # RequestGPUMemory is null for every job here; polars would type the column Null,
        # which DuckDB's Parquet reader rejects.
        record_jobs([job()], tmp_path / "job_pressure_2026-09.parquet", T0, STALE)
        out = load_job_pressure(str(tmp_path), utc(T0 - 10), utc(T0 + 10))
        assert out.height == 1


def write_pressure(tmp_path, records, name="job_pressure_2026-09.parquet"):
    df = pl.DataFrame(records, schema=JOB_PRESSURE_SCHEMA, orient="row") if records else empty()
    df.write_parquet(tmp_path / name)


def utc(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.UTC).replace(tzinfo=None)


def pressure_row(gid, state, first, last, project="Proj_A"):
    return (gid, "ap1", "alice", 1.0, 4.0, 8000.0, None, T0 - 100, project, state, first, last)


class TestLoadJobPressure:
    def test_empty_directory_returns_empty_frame_with_schema(self, tmp_path):
        out = load_job_pressure(str(tmp_path), utc(T0), utc(T0 + 100))
        assert out.is_empty()
        assert out.schema == pl.Schema(JOB_PRESSURE_SCHEMA)

    def test_window_selects_overlapping_intervals_inclusive(self, tmp_path):
        write_pressure(
            tmp_path,
            [
                pressure_row("before", "idle", T0 - 100, T0 - 1),
                pressure_row("touches_start", "idle", T0 - 100, T0),
                pressure_row("inside", "idle", T0 + 10, T0 + 20),
                pressure_row("touches_end", "idle", T0 + 100, T0 + 200),
                pressure_row("after", "idle", T0 + 101, T0 + 200),
                pressure_row("spans", "idle", T0 - 500, T0 + 500),
            ],
        )
        out = load_job_pressure(str(tmp_path), utc(T0), utc(T0 + 100))
        assert set(out["GlobalJobId"]) == {"touches_start", "inside", "touches_end", "spans"}

    def test_job_state_filter(self, tmp_path):
        write_pressure(tmp_path, [pressure_row("i", "idle", T0, T0), pressure_row("r", "running", T0, T0)])
        assert load_job_pressure(str(tmp_path), utc(T0), utc(T0), ("idle",))["GlobalJobId"].to_list() == ["i"]
        assert set(load_job_pressure(str(tmp_path), utc(T0), utc(T0))["GlobalJobId"]) == {"i", "r"}

    def test_reads_across_monthly_files(self, tmp_path):
        write_pressure(tmp_path, [pressure_row("aug", "idle", T0, T0)], "job_pressure_2026-08.parquet")
        write_pressure(tmp_path, [pressure_row("sep", "idle", T0, T0)], "job_pressure_2026-09.parquet")
        assert set(load_job_pressure(str(tmp_path), utc(T0), utc(T0))["GlobalJobId"]) == {"aug", "sep"}


NAIVE_T0 = datetime.datetime(2026, 9, 1, 12, 0, 0)


def write_gpu_state(tmp_path, records):
    pl.DataFrame(
        records,
        schema={"GlobalJobId": pl.Utf8, "RemoteOwner": pl.Utf8, "State": pl.Utf8, "timestamp": pl.Datetime("us")},
        orient="row",
    ).write_parquet(tmp_path / "gpu_state_2026-09.parquet")


class TestAttributeClaimedJobs:
    def test_matched_unmatched_and_empty_project(self, tmp_path):
        ts = NAIVE_T0
        write_gpu_state(
            tmp_path,
            [
                ("ap1#1.0#1", "alice@chtc.wisc.edu", "Claimed", ts),
                ("ap1#1.0#1", "alice@chtc.wisc.edu", "Claimed", ts + datetime.timedelta(minutes=5)),
                ("ap1#2.0#1", "bob@chtc.wisc.edu", "Claimed", ts),
                ("ap1#3.0#1", "carol@chtc.wisc.edu", "Claimed", ts),
            ],
        )
        write_pressure(
            tmp_path,
            [
                pressure_row("ap1#1.0#1", "idle", T0, T0, "Proj_A,Proj_B"),
                pressure_row("ap1#3.0#1", "running", T0, T0, ""),
            ],
        )
        out = attribute_claimed_jobs(str(tmp_path), ts, ts + datetime.timedelta(hours=1)).sort("GlobalJobId")
        assert out.to_dicts() == [
            {
                "GlobalJobId": "ap1#1.0#1",
                "RemoteOwner": "alice@chtc.wisc.edu",
                "ChtcProjects": "Proj_A,Proj_B",
                "matched": True,
            },
            {"GlobalJobId": "ap1#2.0#1", "RemoteOwner": "bob@chtc.wisc.edu", "ChtcProjects": None, "matched": False},
            {"GlobalJobId": "ap1#3.0#1", "RemoteOwner": "carol@chtc.wisc.edu", "ChtcProjects": "", "matched": True},
        ]

    def test_latest_record_wins_when_job_has_several_intervals(self, tmp_path):
        write_gpu_state(tmp_path, [("ap1#1.0#1", "alice@chtc.wisc.edu", "Claimed", NAIVE_T0)])
        write_pressure(
            tmp_path,
            [
                pressure_row("ap1#1.0#1", "idle", T0, T0, "Old"),
                pressure_row("ap1#1.0#1", "running", T0 + 50, T0 + 90, "New"),
            ],
        )
        out = attribute_claimed_jobs(str(tmp_path), NAIVE_T0, NAIVE_T0)
        assert out["ChtcProjects"].to_list() == ["New"]

    def test_only_claimed_slots_inside_window_count(self, tmp_path):
        write_gpu_state(
            tmp_path,
            [
                ("unclaimed", "a@x", "Unclaimed", NAIVE_T0),
                ("too_early", "a@x", "Claimed", NAIVE_T0 - datetime.timedelta(hours=1)),
                ("ok", "a@x", "Claimed", NAIVE_T0),
                ("no_id", "a@x", "Claimed", NAIVE_T0),
            ],
        )
        # Rewrite with a null GlobalJobId row
        df = pl.read_parquet(tmp_path / "gpu_state_2026-09.parquet").with_columns(
            pl.when(pl.col("GlobalJobId") == "no_id").then(None).otherwise(pl.col("GlobalJobId")).alias("GlobalJobId")
        )
        df.write_parquet(tmp_path / "gpu_state_2026-09.parquet")
        out = attribute_claimed_jobs(str(tmp_path), NAIVE_T0, NAIVE_T0 + datetime.timedelta(hours=1))
        assert out["GlobalJobId"].to_list() == ["ok"]

    def test_no_job_pressure_files_leaves_everything_unmatched(self, tmp_path):
        write_gpu_state(tmp_path, [("ap1#1.0#1", "alice@chtc.wisc.edu", "Claimed", NAIVE_T0)])
        out = attribute_claimed_jobs(str(tmp_path), NAIVE_T0, NAIVE_T0)
        assert out["matched"].to_list() == [False]
        assert out["ChtcProjects"].to_list() == [None]


class TestMerge:
    def test_merge_stream_joins_sightings_within_gap_only(self):
        def r(gid, first, last):
            return (gid, "s", "o", 1.0, 1.0, 1.0, None, 0, "P", first, last)

        merged = list(
            _merge_stream(
                iter([r("a", 0, 0), r("a", 1800, 1800), r("a", 10_000, 10_000), r("b", 10_100, 10_100)]), 3600
            )
        )
        assert [(m[0], m[9], m[10]) for m in merged] == [("a", 0, 1800), ("a", 10_000, 10_000), ("b", 10_100, 10_100)]


class TestMigrate:
    def _sqlite(self, path, create, insert, records):
        conn = sqlite3.connect(path)
        conn.execute(create)
        conn.executemany(insert, records)
        conn.commit()
        conn.close()

    def test_old_snapshot_schema_merges_and_applies_local_offset(self, tmp_path):
        db = tmp_path / "job_pressure_2026-05.db"
        self._sqlite(
            db,
            "CREATE TABLE job_pressure (timestamp TEXT, GlobalJobId TEXT, ScheddName TEXT, Owner TEXT, RequestGPUs REAL,"
            " RequestCPUs REAL, RequestMemory REAL, RequestGPUMemory REAL, QDate INTEGER, ChtcProjects TEXT)",
            "INSERT INTO job_pressure VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                ("2026-05-05T13:00:00", "ap1#1.0#1", "ap1", "alice", 1.0, 4.0, 8000.0, None, 1, "Proj_A"),
                ("2026-05-05T13:30:00", "ap1#1.0#1", "ap1", "alice", 1.0, 4.0, 8000.0, None, 1, "Proj_A"),
                ("2026-05-05T14:00:00", "ap1#1.0#1", "ap1", "alice", 1.0, 4.0, 8000.0, None, 1, "Proj_A"),
            ],
        )
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        _migrate(str(db), out_dir, None, 18000)

        out = pl.read_parquet(out_dir / "job_pressure_2026-05.parquet")
        start_utc = int(datetime.datetime(2026, 5, 5, 18, 0, tzinfo=datetime.UTC).timestamp())
        assert rows(out) == {("ap1#1.0#1", "idle", start_utc, start_utc + 3600)}
        assert out.schema == pl.Schema(JOB_PRESSURE_SCHEMA)

    def test_interval_schema_is_copied_unshifted_as_idle(self, tmp_path):
        db = tmp_path / "job_pressure_2026-06.db"
        self._sqlite(
            db,
            "CREATE TABLE job_pressure (GlobalJobId TEXT, ScheddName TEXT, Owner TEXT, RequestGPUs REAL, RequestCPUs REAL,"
            " RequestMemory REAL, RequestGPUMemory REAL, QDate INTEGER, ChtcProjects TEXT, first_seen INTEGER, last_seen INTEGER)",
            "INSERT INTO job_pressure VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [("ap1#1.0#1", "ap1", "alice", 1.0, 4.0, 8000.0, None, 1, "Proj_A", T0, T0 + 900)],
        )
        _migrate(str(db), None, None, 18000)
        out = pl.read_parquet(tmp_path / "job_pressure_2026-06.parquet")
        assert rows(out) == {("ap1#1.0#1", "idle", T0, T0 + 900)}
        assert db.exists()

    def test_interval_schema_intervals_are_not_remerged_without_gap(self, tmp_path):
        # Two idle intervals of one job far apart: the job went idle, was matched, went idle again.
        db = tmp_path / "job_pressure_2026-06.db"
        self._sqlite(
            db,
            "CREATE TABLE job_pressure (GlobalJobId TEXT, ScheddName TEXT, Owner TEXT, RequestGPUs REAL, RequestCPUs REAL,"
            " RequestMemory REAL, RequestGPUMemory REAL, QDate INTEGER, ChtcProjects TEXT, first_seen INTEGER, last_seen INTEGER)",
            "INSERT INTO job_pressure VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("ap1#1.0#1", "ap1", "alice", 1.0, 4.0, 8000.0, None, 1, "Proj_A", T0, T0 + 900),
                ("ap1#1.0#1", "ap1", "alice", 1.0, 4.0, 8000.0, None, 1, "Proj_A", T0 + 20_000, T0 + 21_000),
            ],
        )
        _migrate(str(db), None, None, 18000)
        out = pl.read_parquet(tmp_path / "job_pressure_2026-06.parquet")
        assert rows(out) == {("ap1#1.0#1", "idle", T0, T0 + 900), ("ap1#1.0#1", "idle", T0 + 20_000, T0 + 21_000)}


class TestScheddSelection:
    def test_default_skips_icecube_and_queries_the_rest(self):
        assert not should_query_schedd("grid-submitter.icecube.wisc.edu", None)
        assert should_query_schedd("ap2001.chtc.wisc.edu", None)
        assert should_query_schedd("wright-ap4000.chtc.wisc.edu", [])

    def test_explicit_allow_list_is_exact(self):
        assert should_query_schedd("ap2001.chtc.wisc.edu", ["ap2001.chtc.wisc.edu"])
        assert not should_query_schedd("ap2002.chtc.wisc.edu", ["ap2001.chtc.wisc.edu"])
        assert should_query_schedd("grid-submitter.icecube.wisc.edu", ["grid-submitter.icecube.wisc.edu"])


class TestMonthBoundary:
    def test_job_running_across_months_gets_an_interval_in_each_file(self, tmp_path):
        aug = tmp_path / "job_pressure_2026-08.parquet"
        sep = tmp_path / "job_pressure_2026-09.parquet"
        record_jobs([job(state="running")], aug, T0, STALE)
        record_jobs([job(state="running")], sep, T0 + 1800, STALE)

        assert rows(pl.read_parquet(aug)) == {("ap1#1.0#1", "running", T0, T0)}
        assert rows(pl.read_parquet(sep)) == {("ap1#1.0#1", "running", T0 + 1800, T0 + 1800)}
        both = load_job_pressure(str(tmp_path), utc(T0), utc(T0 + 1800), ("running",))
        assert sorted(both["first_seen"]) == [T0, T0 + 1800]
