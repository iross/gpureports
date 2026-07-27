#!/usr/bin/env python3
"""GPU State Collection Script — writes directly to monthly Parquet files.

Each run appends new rows to gpu_state_YYYY-MM.parquet via a read-concat-atomic-rename
cycle. Crash safety: writes to a dot-prefixed .tmp file (named so it can't match the
gpu_state_*.parquet glob readers use) then os.replace() so the live Parquet is never
partially written and concurrent readers never see the temp file.
"""

import datetime
import os
from pathlib import Path

import polars as pl
import typer


def _current_month() -> str:
    return datetime.datetime.now().strftime("%Y-%m")


def get_gpus() -> pl.DataFrame:
    """Query HTCondor collector for GPU information and return as a Polars DataFrame."""
    import htcondor2 as htcondor

    coll = htcondor.Collector("cm.chtc.wisc.edu")
    PROJ = [
        "Name",
        "AssignedGPUs",
        "AvailableGPUs",
        "State",
        "GPUs_DeviceName",
        "GPUs_GlobalMemoryMb",
        "PrioritizedProjects",
        "GPUsAverageUsage",
        "Machine",
        "RemoteOwner",
        "GlobalJobId",
        "PreventJobsReason",
    ]

    res = coll.query(htcondor.AdTypes.Startd, constraint="GPUs >= 1", projection=PROJ)

    records = []
    for ad in res:
        ad["AvailableGPUs"] = ",".join(
            [i.__str__().replace("GPUs_", "").replace("_", "-") for i in ad["AvailableGPUs"]]
        )
        _KEEP_GPUS_KEYS = {"GPUs_DeviceName", "GPUs_GlobalMemoryMb"}
        ad = {k: v for k, v in ad.items() if not k.startswith("GPUs_") or k in _KEEP_GPUS_KEYS}
        records.append(dict(ad))

    df = pl.DataFrame(records)

    # Backfill slots expose GPUs via AvailableGPUs, not AssignedGPUs.
    df = df.with_columns(
        pl.when(pl.col("Name").str.contains("backfill"))
        .then(pl.col("AvailableGPUs"))
        .otherwise(pl.col("AssignedGPUs"))
        .alias("AssignedGPUs")
    )

    df = df.with_columns(pl.col("AssignedGPUs").str.replace_all("GPU_", "GPU-"))
    df = df.with_columns(pl.col("AssignedGPUs").str.split(",")).explode("AssignedGPUs")
    df = df.with_columns(pl.lit(datetime.datetime.now()).alias("timestamp"))

    return df


def _write_parquet_atomic(df: pl.DataFrame, parquet_path: Path) -> None:
    """Append df to parquet_path, replacing it atomically via a temp file.

    The temp file is named so it can never match the `gpu_state_*.parquet` glob
    that readers use, even mid-write: it's dot-prefixed (fails the "gpu_state_"
    prefix check) and suffixed `.tmp` rather than `.parquet` (fails the suffix
    check). A name like `gpu_state_2026-07.tmp.parquet` would still match that
    glob and let a concurrent reader pick up a partially written file.
    """
    if parquet_path.exists():
        existing = pl.read_parquet(str(parquet_path))
        df = pl.concat([existing, df], how="diagonal_relaxed")
    tmp = parquet_path.with_name(f".{parquet_path.name}.tmp")
    df.write_parquet(str(tmp), compression="zstd")
    os.replace(tmp, parquet_path)


def main(db_path: str = typer.Argument("/home/iaross/gpureports")):
    """Collect GPU state from HTCondor and append to the monthly Parquet file."""
    df = get_gpus()
    month = _current_month()
    parquet_file = Path(db_path) / f"gpu_state_{month}.parquet"
    _write_parquet_atomic(df, parquet_file)
    typer.echo(f"Wrote {len(df)} GPU state records to {parquet_file}")


if __name__ == "__main__":
    typer.run(main)
