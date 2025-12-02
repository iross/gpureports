#!/usr/bin/env python3
"""
GPU State Collection Script - Polars Version

Queries HTCondor for GPU states and stores in SQLite database using Polars.
This is a performance-optimized version using Polars instead of pandas.
"""

import datetime

import htcondor
import polars as pl
import typer

coll = htcondor.Collector("cm.chtc.wisc.edu")


def get_gpus() -> pl.DataFrame:
    """
    Query HTCondor collector for GPU information and return as Polars DataFrame.

    Returns:
        Polars DataFrame with GPU state information
    """
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
    ]

    # Query HTCondor
    res = coll.query(htcondor.AdTypes.Startd, constraint="GPUs >= 1", projection=PROJ)

    # Collect results into list of dicts
    records = []
    for ad in res:
        # Process AvailableGPUs
        ad["AvailableGPUs"] = ",".join([i.__str__().replace("GPUs_", "") for i in ad["AvailableGPUs"]])

        # Filter out GPUs_GPU_ keys
        ad = {k: v for k, v in ad.items() if not k.startswith("GPUs_GPU_")}
        records.append(dict(ad))

    # Create Polars DataFrame from records
    df = pl.DataFrame(records)

    # Backfill slots don't actually have these GPUs assigned, but for ease downstream, we'll pretend.
    # Use when-then-otherwise for conditional assignment
    df = df.with_columns(
        pl.when(pl.col("Name").str.contains("backfill"))
        .then(pl.col("AvailableGPUs"))
        .otherwise(pl.col("AssignedGPUs"))
        .alias("AssignedGPUs")
    )

    # Replace GPU_ with GPU-
    df = df.with_columns(pl.col("AssignedGPUs").str.replace_all("GPU_", "GPU-"))

    # Split AssignedGPUs by comma and explode
    df = df.with_columns(pl.col("AssignedGPUs").str.split(",")).explode("AssignedGPUs")

    # Add timestamp column
    df = df.with_columns(pl.lit(datetime.datetime.now()).alias("timestamp"))

    return df


def main(db_path: str = typer.Argument("/home/iaross/gpureports")):
    """
    Main entry point: collect GPU data and write to SQLite.

    Args:
        db_path: Directory path where database files are stored
    """
    df = get_gpus()

    # Generate database filename with current month
    month = datetime.datetime.now().strftime("%Y-%m")
    db_file = f"{db_path}/gpu_state_{month}.db"

    # Write to SQLite using Polars' native write_database
    connection_uri = f"sqlite:///{db_file}"

    df.write_database(table_name="gpu_state", connection=connection_uri, if_table_exists="append", engine="sqlalchemy")

    typer.echo(f"Successfully wrote {len(df)} GPU state records to {db_file}")


if __name__ == "__main__":
    typer.run(main)
