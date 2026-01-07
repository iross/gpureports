#!/usr/bin/env python3
"""
Generate a Gantt chart showing hosts with GPUs in Draining state over time.
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import typer

app = typer.Typer()


def get_database_paths(start_time: datetime, end_time: datetime) -> list[Path]:
    """
    Get list of database files covering the time range.

    Args:
        start_time: Start of time range
        end_time: End of time range

    Returns:
        List of database file paths
    """
    db_paths = []
    current = start_time.replace(day=1)

    while current <= end_time:
        db_name = f"gpu_state_{current.strftime('%Y-%m')}.db"
        db_path = Path(db_name)
        if db_path.exists():
            db_paths.append(db_path)
        current = (current + timedelta(days=32)).replace(day=1)

    return db_paths


def fetch_draining_data(db_paths: list[Path], start_time: datetime, end_time: datetime) -> pd.DataFrame:
    """
    Fetch GPU draining data from databases.
    Only includes GPUs that are drained and NOT claimed by any slot at that timestamp.

    Args:
        db_paths: List of database paths to query
        start_time: Start of time range
        end_time: End of time range

    Returns:
        DataFrame with draining data
    """
    all_data = []

    for db_path in db_paths:
        try:
            conn = sqlite3.connect(db_path)

            # Query to find GPUs that have Drained state but exclude those that
            # also have a Claimed state at the same timestamp (different slot)
            query = """
            WITH DrainedGPUs AS (
                SELECT DISTINCT
                    Machine,
                    AssignedGPUs,
                    timestamp
                FROM gpu_state
                WHERE timestamp >= ?
                    AND timestamp <= ?
                    AND State = 'Drained'
                    AND AssignedGPUs IS NOT NULL
            ),
            ClaimedGPUs AS (
                SELECT DISTINCT
                    Machine,
                    AssignedGPUs,
                    timestamp
                FROM gpu_state
                WHERE timestamp >= ?
                    AND timestamp <= ?
                    AND State = 'Claimed'
                    AND AssignedGPUs IS NOT NULL
            )
            SELECT
                d.Machine,
                d.AssignedGPUs,
                d.timestamp
            FROM DrainedGPUs d
            LEFT JOIN ClaimedGPUs c
                ON d.Machine = c.Machine
                AND d.AssignedGPUs = c.AssignedGPUs
                AND d.timestamp = c.timestamp
            WHERE c.AssignedGPUs IS NULL
            ORDER BY d.Machine, d.timestamp
            """

            df = pd.read_sql_query(
                query,
                conn,
                params=(start_time.isoformat(), end_time.isoformat(), start_time.isoformat(), end_time.isoformat()),
            )

            if not df.empty:
                all_data.append(df)

            conn.close()

        except sqlite3.Error as e:
            typer.echo(f"Warning: Error reading {db_path}: {e}", err=True)
            continue

    if not all_data:
        return pd.DataFrame()

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df["timestamp"] = pd.to_datetime(combined_df["timestamp"])

    return combined_df


def create_gantt_chart(df: pd.DataFrame, start_time: datetime, end_time: datetime, output_file: str):
    """
    Create a Gantt chart showing draining periods by individual GPU.

    Args:
        df: DataFrame with draining data
        start_time: Start of time range
        end_time: End of time range
        output_file: Output file path for the chart
    """
    if df.empty:
        typer.echo("No draining data found in the specified time period.")
        return

    # Group by machine+GPU and create draining intervals
    # For each individual GPU, find continuous draining periods
    draining_intervals = []

    # Group by machine and GPU
    for (machine, gpu_id), gpu_df in df.groupby(["Machine", "AssignedGPUs"]):
        gpu_df = gpu_df.sort_values("timestamp").copy()

        # Group consecutive timestamps (within 20 minutes) into intervals
        gpu_df["time_diff"] = gpu_df["timestamp"].diff()

        # Start a new interval if gap is > 20 minutes (allowing for some data collection lag)
        gpu_df["new_interval"] = gpu_df["time_diff"] > pd.Timedelta(minutes=20)
        gpu_df["interval_id"] = gpu_df["new_interval"].cumsum()

        # Get start and end time for each interval
        for _interval_id, group in gpu_df.groupby("interval_id"):
            start = group["timestamp"].min()
            end = group["timestamp"].max()

            # If only one data point, assume it lasted at least 15 minutes
            if start == end:
                end = start + pd.Timedelta(minutes=15)

            draining_intervals.append(
                {
                    "machine": machine,
                    "gpu_id": gpu_id,
                    "gpu_label": f"{machine} - {gpu_id}",
                    "start": start,
                    "end": end,
                    "duration": (end - start).total_seconds() / 3600,  # hours
                }
            )

    # Create the Gantt chart
    intervals_df = pd.DataFrame(draining_intervals)

    # Sort GPUs by machine first, then GPU ID
    # Group by machine to maintain host grouping in the chart
    gpu_order = intervals_df.groupby("gpu_label")["start"].min().sort_index().index.tolist()

    # Calculate figure height based on number of GPUs
    num_gpus = len(gpu_order)
    fig_height = max(8, num_gpus * 0.35)

    fig, ax = plt.subplots(figsize=(16, fig_height))

    # Use a single color for all bars (since each bar is now a single GPU)
    bar_color = "#d62728"  # Red color

    # Plot each interval as a horizontal bar
    for idx, gpu_label in enumerate(gpu_order):
        gpu_intervals = intervals_df[intervals_df["gpu_label"] == gpu_label]

        for _, row in gpu_intervals.iterrows():
            duration = (row["end"] - row["start"]).total_seconds() / 3600  # in hours
            ax.barh(
                idx,
                duration,
                left=mdates.date2num(row["start"]),
                height=0.7,
                color=bar_color,
                edgecolor="black",
                linewidth=0.5,
                alpha=0.8,
            )

    # Configure axes
    ax.set_yticks(range(len(gpu_order)))
    ax.set_yticklabels(gpu_order, fontsize=8)
    ax.set_xlabel("Time", fontsize=11)
    ax.set_ylabel("Host - GPU", fontsize=11)
    ax.set_title(
        f'GPU Draining Timeline (by Individual GPU)\n{start_time.strftime("%Y-%m-%d %H:%M")} to {end_time.strftime("%Y-%m-%d %H:%M")}',
        fontsize=13,
        fontweight="bold",
    )

    # Format x-axis as dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Add grid
    ax.grid(True, axis="x", alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    # Set x-axis limits to the requested time range
    ax.set_xlim(mdates.date2num(start_time), mdates.date2num(end_time))

    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches="tight")
    typer.echo(f"Gantt chart saved to: {output_file}")

    # Print summary statistics
    typer.echo("\n" + "=" * 80)
    typer.echo("DRAINING SUMMARY")
    typer.echo("=" * 80)

    unique_hosts = intervals_df["machine"].nunique()
    unique_gpus = len(gpu_order)
    total_intervals = len(intervals_df)
    total_duration = intervals_df["duration"].sum()
    avg_duration = intervals_df["duration"].mean()

    typer.echo(f"\nTotal hosts with drained GPUs: {unique_hosts}")
    typer.echo(f"Total individual GPUs drained: {unique_gpus}")
    typer.echo(f"Total draining intervals: {total_intervals}")
    typer.echo(f"Total draining time (sum across all GPUs): {total_duration:.2f} hours")
    typer.echo(f"Average interval duration: {avg_duration:.2f} hours")

    typer.echo("\nPer-host breakdown:")
    for machine in sorted(intervals_df["machine"].unique()):
        machine_intervals = intervals_df[intervals_df["machine"] == machine]
        num_gpus = machine_intervals["gpu_id"].nunique()
        num_intervals = len(machine_intervals)
        total_time = machine_intervals["duration"].sum()

        typer.echo(f"  {machine}:")
        typer.echo(f"    - Number of GPUs drained: {num_gpus}")
        typer.echo(f"    - Total intervals: {num_intervals}")
        typer.echo(f"    - Total draining time (across all GPUs): {total_time:.2f} hours")

        # Show per-GPU details
        for gpu_id in sorted(machine_intervals["gpu_id"].unique()):
            gpu_intervals = machine_intervals[machine_intervals["gpu_id"] == gpu_id]
            gpu_total_time = gpu_intervals["duration"].sum()
            gpu_num_intervals = len(gpu_intervals)
            typer.echo(f"      • {gpu_id}: {gpu_num_intervals} interval(s), {gpu_total_time:.2f} hours total")


@app.command()
def main(
    hours: int = typer.Option(24, "--hours", "-h", help="Number of hours to look back (default: 24)"),
    output: str = typer.Option(
        None, "--output", "-o", help="Output file path (default: draining_report_<timestamp>.png)"
    ),
    start: str | None = typer.Option(None, "--start", help="Start time (YYYY-MM-DD HH:MM), overrides --hours"),
    end: str | None = typer.Option(None, "--end", help="End time (YYYY-MM-DD HH:MM), defaults to now"),
):
    """
    Generate a Gantt chart showing hosts with GPUs in Draining state.

    Examples:
        # Last 24 hours (default)
        python draining_report.py

        # Last 48 hours
        python draining_report.py --hours 48

        # Specific time range
        python draining_report.py --start "2026-01-06 00:00" --end "2026-01-07 12:00"
    """
    # Determine time range
    if start:
        try:
            start_time = datetime.strptime(start, "%Y-%m-%d %H:%M")
        except ValueError:
            typer.echo("Error: Invalid start time format. Use YYYY-MM-DD HH:MM", err=True)
            raise typer.Exit(1) from ValueError
    else:
        start_time = datetime.now() - timedelta(hours=hours)

    if end:
        try:
            end_time = datetime.strptime(end, "%Y-%m-%d %H:%M")
        except ValueError:
            typer.echo("Error: Invalid end time format. Use YYYY-MM-DD HH:MM", err=True)
            raise typer.Exit(1) from ValueError
    else:
        end_time = datetime.now()

    # Validate time range
    if start_time >= end_time:
        typer.echo("Error: Start time must be before end time", err=True)
        raise typer.Exit(1)

    # Generate output filename if not provided
    if not output:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output = f"draining_report_{timestamp}.png"

    typer.echo(f"Analyzing draining data from {start_time} to {end_time}")
    typer.echo(f"Time range: {(end_time - start_time).total_seconds() / 3600:.1f} hours")

    # Get database paths
    db_paths = get_database_paths(start_time, end_time)

    if not db_paths:
        typer.echo("Error: No database files found for the specified time range", err=True)
        raise typer.Exit(1)

    typer.echo(f"Found {len(db_paths)} database file(s): {', '.join(str(p) for p in db_paths)}")

    # Fetch data
    typer.echo("\nFetching draining data...")
    df = fetch_draining_data(db_paths, start_time, end_time)

    if df.empty:
        typer.echo("No draining data found in the specified time period.")
        raise typer.Exit(0)

    typer.echo(f"Found {len(df)} draining records across {df['Machine'].nunique()} hosts")

    # Create chart
    create_gantt_chart(df, start_time, end_time, output)


if __name__ == "__main__":
    app()
