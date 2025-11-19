#!/usr/bin/env python3
"""
Generate GPU Timeline Heatmaps for Prioritized Hosts

Creates individual GPU timeline heatmaps for all hosts that have prioritized projects.
This script discovers hosts with non-empty PrioritizedProjects and generates a timeline
heatmap for each one, showing detailed GPU activity over a specified time period.

Performance: Loads database once and processes all hosts in-memory for speed.
"""

import pandas as pd
import sqlite3
import datetime
import typer
from typing import Optional, Tuple
from pathlib import Path
import warnings

# Import shared utilities
from gpu_utils import filter_df, get_most_recent_database
from gpu_timeline_heatmap import (
    get_time_filtered_data,
    create_heatmap,
    create_html_heatmap
)
from gpu_timeline_heatmap_fast import prepare_timeline_data_fast

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)


def discover_prioritized_hosts(df: pd.DataFrame) -> list:
    """
    Discover all hosts with prioritized projects in the dataset.

    Args:
        df: GPU state DataFrame

    Returns:
        List of hostnames with prioritized projects
    """
    # Filter to only hosts with non-empty PrioritizedProjects
    prioritized_df = df[
        (df['PrioritizedProjects'] != '') &
        (df['PrioritizedProjects'].notna())
    ]

    # Get unique hostnames
    hosts = sorted(prioritized_df['Machine'].unique())

    return hosts


def generate_heatmap_for_host(
    hostname: str,
    timeline_df: pd.DataFrame,
    output_dir: Path,
    output_format: str = "html",
    figsize: Tuple[int, int] = (16, 6)
) -> bool:
    """
    Generate a timeline heatmap for a specific host using pre-loaded data.

    Args:
        hostname: Host to generate heatmap for
        timeline_df: Pre-prepared timeline DataFrame (from prepare_timeline_data)
        output_dir: Output directory
        output_format: Output format ('png' or 'html')
        figsize: Figure size for PNG output

    Returns:
        True if successful, False otherwise
    """
    # Filter timeline data to this host
    host_df = timeline_df[timeline_df['hostname'] == hostname].copy()

    if host_df.empty:
        return False

    # Generate filename
    safe_hostname = hostname.replace('.', '_').replace('-', '_')
    time_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_extension = output_format.lower()
    filename = f"gpu_timeline_{safe_hostname}_{time_suffix}.{file_extension}"
    output_path = output_dir / filename

    # Generate title with time range
    start_time = host_df['time_bucket'].min()
    end_time = host_df['time_bucket'].max()
    title = f"GPU Timeline - {hostname}\n{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}"

    try:
        # Create heatmap based on output format
        if output_format.lower() == 'png':
            create_heatmap(
                host_df,
                str(output_path),
                title=title,
                figsize=figsize
            )
        else:  # html
            create_html_heatmap(
                host_df,
                str(output_path),
                title=title
            )
        return True
    except Exception as e:
        print(f"\n      Error: {e}")
        return False


def main(
    db_path: Optional[str] = typer.Option(None, help="Path to SQLite database (defaults to most recent)"),
    hours_back: int = typer.Option(24, help="Number of hours to analyze (default: 24)"),
    output_dir: str = typer.Option("priority_host_heatmaps", help="Output directory for heatmaps"),
    end_time: Optional[str] = typer.Option(None, help="End time for analysis (YYYY-MM-DD HH:MM:SS)"),
    output_format: str = typer.Option("html", help="Output format: 'png' or 'html'"),
    max_hosts: Optional[int] = typer.Option(None, help="Maximum number of hosts to process (for testing)"),
    list_only: bool = typer.Option(False, help="Only list prioritized hosts without generating heatmaps")
):
    """
    Generate GPU timeline heatmaps for all hosts with prioritized projects.

    This tool discovers hosts that have non-empty PrioritizedProjects and creates
    individual timeline heatmaps showing GPU activity patterns over the specified time period.
    """

    # Determine database path
    if db_path is None:
        db_path = get_most_recent_database()
        if db_path is None:
            print("❌ Error: No database files found. Please specify --db-path")
            return
        print(f"📊 Using most recent database: {db_path}")
    else:
        print(f"📊 Using database: {db_path}")

    # Parse end_time if provided
    parsed_end_time = None
    if end_time:
        try:
            parsed_end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"❌ Error: Invalid end_time format. Use YYYY-MM-DD HH:MM:SS")
            return

    # Load data
    print(f"📥 Loading GPU data for the last {hours_back} hours...")
    df = get_time_filtered_data(db_path, hours_back, parsed_end_time)

    if df.empty:
        print("❌ No data found for the specified time range")
        return

    print(f"✅ Loaded {len(df)} records")

    # Apply host filtering (from gpu_utils)
    df = filter_df(df)
    print(f"✅ After host filtering: {len(df)} records")

    # Discover prioritized hosts
    print(f"\n🔍 Discovering hosts with prioritized projects...")
    prioritized_hosts = discover_prioritized_hosts(df)

    if not prioritized_hosts:
        print("❌ No hosts with prioritized projects found")
        return

    print(f"✅ Found {len(prioritized_hosts)} hosts with prioritized projects:")
    for i, host in enumerate(prioritized_hosts, 1):
        print(f"  {i:3d}. {host}")

    # If list_only mode, exit here
    if list_only:
        print(f"\n📋 List-only mode enabled. Exiting without generating heatmaps.")
        return

    # Limit hosts for testing if specified
    if max_hosts:
        prioritized_hosts = prioritized_hosts[:max_hosts]
        print(f"\n🔧 Limited to {len(prioritized_hosts)} hosts for testing")

    # Prepare timeline data ONCE for all hosts (using optimized version)
    print(f"\n⚙️  Preparing timeline data (5-minute buckets)...")
    timeline_df = prepare_timeline_data_fast(df, bucket_minutes=5)

    if timeline_df.empty:
        print("❌ No timeline data to visualize")
        return

    print(f"✅ Prepared timeline data: {len(timeline_df)} data points")

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Validate output format
    if output_format.lower() not in ['png', 'html']:
        print(f"❌ Error: Invalid output format '{output_format}'. Use 'png' or 'html'")
        return

    # Generate heatmaps for each host
    print(f"\n📊 Generating {output_format.upper()} heatmaps for {len(prioritized_hosts)} hosts...")
    print(f"📁 Output directory: {output_path}")

    successful = 0
    failed = 0

    for i, hostname in enumerate(prioritized_hosts, 1):
        print(f"  {i}/{len(prioritized_hosts)} Processing {hostname}...", end=' ', flush=True)

        success = generate_heatmap_for_host(
            hostname=hostname,
            timeline_df=timeline_df,
            output_dir=output_path,
            output_format=output_format
        )

        if success:
            print("✅")
            successful += 1
        else:
            print("❌")
            failed += 1

    # Print summary
    print(f"\n{'='*60}")
    print(f"🎉 Generation complete!")
    print(f"✅ Successful: {successful}/{len(prioritized_hosts)}")
    if failed > 0:
        print(f"❌ Failed: {failed}/{len(prioritized_hosts)}")
    print(f"📁 Output directory: {output_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    typer.run(main)
