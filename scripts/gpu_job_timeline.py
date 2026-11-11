#!/usr/bin/env python3
"""
ASCII timeline of the jobs that ran on one GPU over a time window.

Each row is a job; '█' marks bins where the job was seen holding the GPU.
Overlapping bars mean two slots (e.g. a backfill slot and a primary slot)
both claimed the GPU at once.
The bottom row shows the GPU's state when no job held it: '·' idle (seen
unclaimed), ' ' not observed at all.

Run from the project root:
    uv run scripts/gpu_job_timeline.py GPU-30d65cc1 --start 2026-10-12 --end 2026-10-13
    uv run scripts/gpu_job_timeline.py GPU-30d65cc1 -s "2026-10-12 06:00" -e "2026-10-12 18:00" -w 144
"""

import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

JOB_STATES = ("Claimed", "Preempting")
RUN_GAP = datetime.timedelta(minutes=15)
SAMPLE_INTERVAL = datetime.timedelta(minutes=5)
LABEL_WIDTH = 42


@dataclass(frozen=True)
class JobRun:
    """A contiguous stretch of samples in which one job held the GPU."""

    job_id: str
    owner: str
    slot: str
    start: datetime.datetime
    end: datetime.datetime


def parse_bound(value: str, is_end: bool) -> datetime.datetime:
    """Parse a date or datetime; a bare end date means through the end of that day."""
    parsed = datetime.datetime.fromisoformat(value)
    if is_end and len(value) <= len("YYYY-MM-DD"):
        parsed += datetime.timedelta(days=1)
    return parsed


def load_gpu_rows(parquet: Path, gpu: str, start: datetime.datetime, end: datetime.datetime) -> pl.DataFrame:
    """Return every slot row in the window whose AssignedGPUs includes the GPU (prefix match)."""
    return (
        pl.scan_parquet(parquet)
        .filter(pl.col("timestamp").is_between(start, end, closed="left"))
        .filter(pl.col("AssignedGPUs").str.split(",").list.eval(pl.element().str.starts_with(gpu)).list.any())
        .select("timestamp", "Name", "State", "GlobalJobId", "RemoteOwner", "AssignedGPUs")
        .sort("timestamp")
        .collect()
    )


def short_job_id(global_job_id: str) -> str:
    """Turn 'ap2002.chtc.wisc.edu#6305565.8#1790364111' into 'ap2002 6305565.8'."""
    parts = global_job_id.split("#")
    if len(parts) < 2:
        return global_job_id
    return f"{parts[0].split('.')[0]} {parts[1]}"


def build_runs(rows: pl.DataFrame) -> list[JobRun]:
    """Group job samples into runs, splitting a job when it vanishes for longer than RUN_GAP."""
    jobs = rows.filter(pl.col("State").is_in(JOB_STATES) & pl.col("GlobalJobId").is_not_null())
    runs: list[JobRun] = []
    for (job_id,), group in jobs.group_by(["GlobalJobId"], maintain_order=True):
        times = group["timestamp"].to_list()
        owner = (group["RemoteOwner"][0] or "?").split("@")[0]
        slot = group["Name"][0].split("@")[0]
        run_start = prev = times[0]
        for t in times[1:]:
            if t - prev > RUN_GAP:
                runs.append(JobRun(str(job_id), owner, slot, run_start, prev + SAMPLE_INTERVAL))
                run_start = t
            prev = t
        runs.append(JobRun(str(job_id), owner, slot, run_start, prev + SAMPLE_INTERVAL))
    return sorted(runs, key=lambda r: r.start)


def bin_index(t: datetime.datetime, start: datetime.datetime, bin_size: datetime.timedelta) -> int:
    return int((t - start) / bin_size)


def paint(run: JobRun, start: datetime.datetime, bin_size: datetime.timedelta, width: int) -> str:
    cells = [" "] * width
    first = max(bin_index(run.start, start, bin_size), 0)
    last = min(bin_index(run.end - datetime.timedelta(microseconds=1), start, bin_size), width - 1)
    for i in range(first, last + 1):
        cells[i] = "█"
    return "".join(cells)


def idle_row(
    rows: pl.DataFrame, runs: list[JobRun], start: datetime.datetime, bin_size: datetime.timedelta, width: int
) -> str:
    """'·' where the GPU was observed but no job held it, ' ' where it was never observed."""
    cells = [" "] * width
    for t in rows["timestamp"].to_list():
        cells[bin_index(t, start, bin_size)] = "·"
    for run in runs:
        for i, ch in enumerate(paint(run, start, bin_size, width)):
            if ch != " ":
                cells[i] = " "
    return "".join(cells)


def axis_rows(start: datetime.datetime, bin_size: datetime.timedelta, width: int) -> tuple[str, str]:
    """Hour labels and tick marks; day boundaries get the date instead of the hour."""
    span_hours = bin_size.total_seconds() * width / 3600
    step_hours = next((h for h in (1, 2, 3, 6, 12) if span_hours / h <= width / 6), 24)
    labels = [" "] * width
    ticks = ["─"] * width
    t = start.replace(minute=0, second=0, microsecond=0)
    if t < start:
        t += datetime.timedelta(hours=1)
    while (i := bin_index(t, start, bin_size)) < width:
        if t.hour % step_hours == 0:
            text = t.strftime("%m-%d") if t.hour == 0 else f"{t.hour:02d}"
            ticks[i] = "┼" if t.hour == 0 else "┬"
            for j, ch in enumerate(text):
                if i + j < width:
                    labels[i + j] = ch
        t += datetime.timedelta(hours=1)
    return "".join(labels), "".join(ticks)


def group_by_job(runs: list[JobRun]) -> list[list[JobRun]]:
    """Runs grouped per job, ordered by each job's first appearance."""
    grouped: dict[str, list[JobRun]] = {}
    for run in runs:
        grouped.setdefault(run.job_id, []).append(run)
    return list(grouped.values())


def render(
    gpu: str, rows: pl.DataFrame, runs: list[JobRun], start: datetime.datetime, end: datetime.datetime, width: int
) -> str:
    bin_size = (end - start) / width
    pad = " " * LABEL_WIDTH
    labels, ticks = axis_rows(start, bin_size, width)
    machine = rows["Name"][0].split("@")[-1] if rows.height else "unknown host"
    lines = [
        f"{gpu} on {machine}   {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}   (1 col = {bin_size.total_seconds() / 60:.0f} min)",
        "",
        f"{pad}{labels}",
        f"{pad}{ticks}",
    ]
    for job_runs in group_by_job(runs):
        label = f"{job_runs[0].owner[:14]:<14} {short_job_id(job_runs[0].job_id)}"
        bars = [paint(run, start, bin_size, width) for run in job_runs]
        merged = "".join("█" if any(b[i] != " " for b in bars) else " " for i in range(width))
        lines.append(f"{label[: LABEL_WIDTH - 1]:<{LABEL_WIDTH - 1}} {merged}")
    lines.append(f"{'(idle · / no data blank)':<{LABEL_WIDTH - 1}} {idle_row(rows, runs, start, bin_size, width)}")
    return "\n".join(lines)


def render_table(runs: list[JobRun]) -> str:
    header = f"{'owner':<16} {'job':<26} {'slot':<12} {'first seen':<12} {'last seen':<12} {'hours':>6}"
    lines = [header, "-" * len(header)]
    for run in runs:
        hours = (run.end - run.start).total_seconds() / 3600
        lines.append(
            f"{run.owner[:16]:<16} {short_job_id(run.job_id):<26} {run.slot:<12} "
            f"{run.start:%m-%d %H:%M}  {run.end - SAMPLE_INTERVAL:%m-%d %H:%M}  {hours:>6.1f}"
        )
    return "\n".join(lines)


def main(
    gpu: Annotated[str, typer.Argument(help="GPU UUID or prefix, e.g. GPU-30d65cc1")],
    start: Annotated[str, typer.Option("--start", "-s", help="Window start, e.g. 2026-10-12 or '2026-10-12 06:00'")],
    end: Annotated[str, typer.Option("--end", "-e", help="Window end; a bare date is inclusive of that whole day")],
    parquet: Annotated[
        Path | None, typer.Option("--parquet", "-p", help="Defaults to gpu_state_<YYYY-MM>.parquet for the start month")
    ] = None,
    width: Annotated[int, typer.Option("--width", "-w", help="Number of timeline columns")] = 96,
) -> None:
    """Print an ASCII timeline of the jobs that held a GPU during a time window (local time)."""
    window_start = parse_bound(start, is_end=False)
    window_end = parse_bound(end, is_end=True)
    if window_end <= window_start:
        raise typer.BadParameter(f"--end ({window_end}) must be after --start ({window_start})")
    parquet = parquet or Path(f"gpu_state_{window_start:%Y-%m}.parquet")
    if not parquet.exists():
        raise typer.BadParameter(f"{parquet} not found; pass --parquet explicitly")

    rows = load_gpu_rows(parquet, gpu, window_start, window_end)
    if rows.is_empty():
        typer.echo(f"No rows for {gpu} in {parquet} between {window_start} and {window_end}", err=True)
        raise typer.Exit(1)

    runs = build_runs(rows)
    typer.echo(render(gpu, rows, runs, window_start, window_end, width))
    typer.echo("")
    typer.echo(render_table(runs))


if __name__ == "__main__":
    typer.run(main)
