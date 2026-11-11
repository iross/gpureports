"""Tests for scripts/gpu_job_timeline.py."""

import datetime
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.gpu_job_timeline import build_runs, load_gpu_rows, parse_bound, render, short_job_id

T0 = datetime.datetime(2026, 10, 12, 0, 0)
GPU = "GPU-30d65cc1"


def row(
    minutes: int, state: str = "Claimed", job: str | None = "ap2002.chtc.wisc.edu#100.0#1", gpus: str = GPU
) -> dict:
    return {
        "timestamp": T0 + datetime.timedelta(minutes=minutes),
        "Name": "slot2_1@host.chtc.wisc.edu",
        "State": state,
        "GlobalJobId": job if state != "Unclaimed" else None,
        "RemoteOwner": "alice@chtc.wisc.edu" if state != "Unclaimed" else None,
        "AssignedGPUs": gpus,
    }


def test_parse_bound_bare_end_date_is_inclusive():
    assert parse_bound("2026-10-13", is_end=True) == datetime.datetime(2026, 10, 14)
    assert parse_bound("2026-10-13", is_end=False) == datetime.datetime(2026, 10, 13)
    assert parse_bound("2026-10-13 06:00", is_end=True) == datetime.datetime(2026, 10, 13, 6)


def test_short_job_id():
    assert short_job_id("ap2002.chtc.wisc.edu#6305565.8#1790364111") == "ap2002 6305565.8"
    assert short_job_id("weird") == "weird"


def test_build_runs_splits_on_gap_and_ignores_unclaimed():
    rows = pl.DataFrame([row(0), row(5), row(10), row(15, "Unclaimed"), row(60), row(65)])
    runs = build_runs(rows)
    assert [(r.start, r.end) for r in runs] == [
        (T0, T0 + datetime.timedelta(minutes=15)),
        (T0 + datetime.timedelta(minutes=60), T0 + datetime.timedelta(minutes=70)),
    ]
    assert runs[0].owner == "alice"


def test_load_gpu_rows_matches_prefix_within_multi_gpu_list(tmp_path):
    path = tmp_path / "state.parquet"
    pl.DataFrame([row(0, gpus=f"GPU-aaaa1111,{GPU}"), row(5, gpus="GPU-bbbb2222"), row(2000, gpus=GPU)]).write_parquet(
        path
    )
    rows = load_gpu_rows(path, "GPU-30d6", T0, T0 + datetime.timedelta(days=1))
    assert rows["timestamp"].to_list() == [T0]


def test_render_marks_job_and_idle_bins():
    rows = pl.DataFrame([row(0), row(5), row(30, "Unclaimed"), row(35, "Unclaimed")])
    out = render(GPU, rows, build_runs(rows), T0, T0 + datetime.timedelta(minutes=60), width=6)
    job_line, idle_line = out.splitlines()[-2:]
    assert job_line.endswith("█     ")
    assert idle_line.endswith("   ·  ")


@pytest.mark.parametrize("width", [1, 96, 300])
def test_render_line_lengths_are_consistent(width):
    rows = pl.DataFrame([row(m) for m in range(0, 600, 5)])
    lines = render(GPU, rows, build_runs(rows), T0, T0 + datetime.timedelta(hours=10), width=width).splitlines()[2:]
    assert len({len(line) for line in lines}) == 1
