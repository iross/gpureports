#!/usr/bin/env python3
"""
GPU Type Summary Script

Generates a summary table of GPU statistics grouped by GPU type, showing:
- Total GPUs
- Primary slot average allocated
- Backfill slot average available
- Total allocated percentage

With optional expansion to show individual machines under each GPU type.
"""

import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import typer

# Import shared utilities
import gpu_utils
from device_name_mappings import get_human_readable_device_name
from gpu_utils import get_most_recent_database, load_host_exclusions, filter_df_by_machine_category


def get_time_filtered_data(db_path: str, hours_back: int = 24, end_time: datetime | None = None) -> pd.DataFrame:
    """
    Load GPU state data from database filtered by time range.

    Args:
        db_path: Path to SQLite database
        hours_back: Number of hours to look back
        end_time: Optional end time (defaults to latest timestamp in database)

    Returns:
        DataFrame with filtered GPU state data
    """
    conn = sqlite3.connect(db_path)

    # Get end time from database if not specified
    if end_time is None:
        end_time_query = "SELECT MAX(timestamp) FROM gpu_state"
        end_time_df = pd.read_sql_query(end_time_query, conn)
        end_time = pd.to_datetime(end_time_df.iloc[0, 0])

    start_time = end_time - timedelta(hours=hours_back)

    query = f"""
    SELECT * FROM gpu_state
    WHERE timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
    AND timestamp <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def calculate_gpu_type_summary(
    df: pd.DataFrame,
    include_all_devices: bool = True,
    expand_machines: bool = False,
    machine_category: str = None
) -> dict:
    """
    Calculate summary statistics by GPU type.

    Args:
        df: DataFrame with GPU state data
        include_all_devices: Whether to include old/uncommon GPU types
        expand_machines: Whether to include per-machine breakdown under each GPU type
        machine_category: Optional filter by machine category ("Open Capacity", "Researcher Owned", or None for all)

    Returns:
        Dictionary with GPU type statistics
    """
    # Apply host exclusions if configured (respects masked_hosts.yaml)
    if gpu_utils.HOST_EXCLUSIONS:
        for excluded_host in gpu_utils.HOST_EXCLUSIONS.keys():
            df = df[~df["Machine"].str.contains(excluded_host, case=False, na=False)]

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Determine GPU category based on primary slots
    # For each GPU, find its PrioritizedProjects from the primary slot
    primary_slots = df[~df["Name"].str.contains("backfill", case=False, na=False)]

    # Create a mapping of (timestamp, GPU) -> PrioritizedProjects from primary slots
    gpu_category_map = {}
    for _, row in primary_slots.iterrows():
        if pd.notna(row["AssignedGPUs"]):
            key = (row["timestamp"], row["AssignedGPUs"])
            # Store the PrioritizedProjects value from the primary slot
            gpu_category_map[key] = row["PrioritizedProjects"] if pd.notna(row["PrioritizedProjects"]) else ""

    # Apply the category to all rows (including backfill) based on GPU mapping
    def get_gpu_category(row):
        key = (row["timestamp"], row["AssignedGPUs"])
        if key in gpu_category_map:
            return gpu_category_map[key]
        # If not found in mapping, use the row's own value (for primary slots not in map)
        return row["PrioritizedProjects"] if pd.notna(row["PrioritizedProjects"]) else ""

    df["GPUCategory"] = df.apply(get_gpu_category, axis=1)

    # Filter by machine category if specified
    if machine_category:
        if machine_category == "Open Capacity":
            # Open Capacity: GPUs whose primary slots have no PrioritizedProjects
            df = df[(df["GPUCategory"] == "") | (df["GPUCategory"].isna())]
        elif machine_category == "Researcher Owned":
            # Researcher Owned: GPUs whose primary slots have PrioritizedProjects
            df = df[(df["GPUCategory"] != "") & (df["GPUCategory"].notna())]

    # Separate primary and backfill slots
    primary_df = df[~df["Name"].str.contains("backfill", case=False, na=False)].copy()
    backfill_df = df[df["Name"].str.contains("backfill", case=False, na=False)].copy()

    # Identify partitionable slots (like slot1@) vs dynamic slots (like slot1_1@)
    # We need partitionable slots to get total GPU count, but only dynamic slots for claimed count
    partitionable_pattern = r'^slot\d+@'  # Matches slot1@, slot2@, etc.
    primary_df["is_partitionable"] = primary_df["Name"].str.match(partitionable_pattern, case=False, na=False)

    # Get unique GPU types
    gpu_types = df["GPUs_DeviceName"].dropna().unique()

    gpu_type_stats = {}

    for gpu_type in gpu_types:
        # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
        if not include_all_devices and any(
            old_gpu in gpu_type for old_gpu in ["GTX 1080", "P100", "Quadro", "A30", "A40"] # TODO: this should be externally defined.
        ):
            continue

        # Filter data for this GPU type
        gpu_primary = primary_df[primary_df["GPUs_DeviceName"] == gpu_type]
        gpu_backfill = backfill_df[backfill_df["GPUs_DeviceName"] == gpu_type]

        # Calculate primary slot statistics (average allocated across timestamps)
        # For total GPUs: count all unique GPUs (includes partitionable slots)
        # For claimed GPUs: count only dynamic slots (exclude partitionable slots)
        total_primary_claimed = 0
        total_primary_available = 0
        num_primary_intervals = 0

        # For backfill display statistics (averaged over all timestamps)
        total_backfill_claimed = 0
        total_backfill_available = 0

        # For slot creation efficiency: only count timestamps where backfill could exist (unused_primary > 0)
        total_unused_primary_when_possible = 0
        total_backfill_when_possible = 0
        num_intervals_with_unused = 0

        # Get all unique timestamps across both primary and backfill
        all_timestamps = sorted(set(gpu_primary["timestamp"].unique()) | set(gpu_backfill["timestamp"].unique()))

        for timestamp in all_timestamps:
            # Primary statistics
            primary_snapshot = gpu_primary[gpu_primary["timestamp"] == timestamp]
            backfill_snapshot = gpu_backfill[gpu_backfill["timestamp"] == timestamp]

            # Count claimed GPUs only from dynamic slots (not partitionable)
            dynamic_df = primary_snapshot[~primary_snapshot["is_partitionable"]]
            primary_claimed = len(dynamic_df[dynamic_df["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())

            # Count total GPUs from all slots
            primary_available = len(primary_snapshot["AssignedGPUs"].dropna().unique())

            # Count backfill available
            backfill_available = len(backfill_snapshot["AssignedGPUs"].dropna().unique())
            backfill_claimed = len(backfill_snapshot[backfill_snapshot["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())

            # Accumulate for overall averages
            total_primary_claimed += primary_claimed
            total_primary_available += primary_available
            total_backfill_claimed += backfill_claimed
            total_backfill_available += backfill_available
            num_primary_intervals += 1

            # For slot creation efficiency: only count timestamps where unused primary GPUs exist
            unused_at_timestamp = primary_available - primary_claimed
            if unused_at_timestamp > 0:
                total_unused_primary_when_possible += unused_at_timestamp
                total_backfill_when_possible += backfill_available
                num_intervals_with_unused += 1

        avg_primary_claimed = total_primary_claimed / num_primary_intervals if num_primary_intervals > 0 else 0
        avg_primary_available = total_primary_available / num_primary_intervals if num_primary_intervals > 0 else 0

        # Calculate backfill averages (for display, averaged over all timestamps)
        avg_backfill_claimed = total_backfill_claimed / num_primary_intervals if num_primary_intervals > 0 else 0
        avg_backfill_available = total_backfill_available / num_primary_intervals if num_primary_intervals > 0 else 0

        # Calculate total allocated percentage (includes both primary and backfill claimed)
        total_gpus = avg_primary_available
        allocated_gpus = avg_primary_claimed + avg_backfill_claimed
        allocated_pct = (allocated_gpus / total_gpus * 100) if total_gpus > 0 else 0

        # Calculate slot creation efficiency: only over timestamps where backfill could exist
        if num_intervals_with_unused == 0:
            # No intervals with unused GPUs, so efficiency is not applicable
            slot_creation_efficiency = None
        else:
            avg_unused_when_possible = total_unused_primary_when_possible / num_intervals_with_unused
            avg_backfill_when_possible = total_backfill_when_possible / num_intervals_with_unused
            slot_creation_efficiency = (avg_backfill_when_possible / avg_unused_when_possible * 100) if avg_unused_when_possible > 0 else 0

        gpu_type_stats[gpu_type] = {
            "total_gpus": avg_primary_available,
            "avg_primary_allocated": avg_primary_claimed,
            "avg_backfill_allocated": avg_backfill_claimed,
            "avg_backfill_available": avg_backfill_available,
            "allocated_pct": allocated_pct,
            "slot_creation_efficiency": slot_creation_efficiency,
        }

        # Add per-machine breakdown if requested
        if expand_machines:
            machines = {}
            for machine in gpu_primary["Machine"].unique():
                machine_primary = gpu_primary[gpu_primary["Machine"] == machine]
                machine_backfill = gpu_backfill[gpu_backfill["Machine"] == machine]

                # Calculate per-machine statistics
                # For total GPUs: count all unique GPUs (includes partitionable slots)
                # For claimed GPUs: count only dynamic slots (exclude partitionable slots)
                total_machine_primary_claimed = 0
                total_machine_primary_available = 0
                num_machine_primary_intervals = 0

                # For backfill display statistics (averaged over all timestamps)
                total_machine_backfill_claimed = 0
                total_machine_backfill_available = 0

                # For slot creation efficiency: only count timestamps where backfill could exist (unused_primary > 0)
                total_machine_unused_when_possible = 0
                total_machine_backfill_when_possible = 0
                num_machine_intervals_with_unused = 0

                # Get all unique timestamps for this machine
                machine_timestamps = sorted(set(machine_primary["timestamp"].unique()) | set(machine_backfill["timestamp"].unique()))

                for timestamp in machine_timestamps:
                    primary_snapshot = machine_primary[machine_primary["timestamp"] == timestamp]
                    backfill_snapshot = machine_backfill[machine_backfill["timestamp"] == timestamp]

                    # Count claimed GPUs only from dynamic slots (not partitionable)
                    dynamic_df = primary_snapshot[~primary_snapshot["is_partitionable"]]
                    primary_claimed = len(dynamic_df[dynamic_df["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())

                    # Count total GPUs from all slots
                    primary_available = len(primary_snapshot["AssignedGPUs"].dropna().unique())

                    # Count backfill
                    backfill_claimed = len(backfill_snapshot[backfill_snapshot["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())
                    backfill_available = len(backfill_snapshot["AssignedGPUs"].dropna().unique())

                    # Accumulate for overall averages
                    total_machine_primary_claimed += primary_claimed
                    total_machine_primary_available += primary_available
                    total_machine_backfill_claimed += backfill_claimed
                    total_machine_backfill_available += backfill_available
                    num_machine_primary_intervals += 1

                    # For slot creation efficiency: only count timestamps where unused primary GPUs exist
                    unused_at_timestamp = primary_available - primary_claimed
                    if unused_at_timestamp > 0:
                        total_machine_unused_when_possible += unused_at_timestamp
                        total_machine_backfill_when_possible += backfill_available
                        num_machine_intervals_with_unused += 1

                avg_machine_primary_claimed = total_machine_primary_claimed / num_machine_primary_intervals if num_machine_primary_intervals > 0 else 0
                avg_machine_primary_available = total_machine_primary_available / num_machine_primary_intervals if num_machine_primary_intervals > 0 else 0

                # Calculate backfill averages (for display, averaged over all timestamps)
                avg_machine_backfill_claimed = total_machine_backfill_claimed / num_machine_primary_intervals if num_machine_primary_intervals > 0 else 0
                avg_machine_backfill_available = total_machine_backfill_available / num_machine_primary_intervals if num_machine_primary_intervals > 0 else 0

                # Calculate per-machine allocated percentage (includes both primary and backfill claimed)
                machine_total_gpus = avg_machine_primary_available
                machine_allocated_gpus = avg_machine_primary_claimed + avg_machine_backfill_claimed
                machine_allocated_pct = (machine_allocated_gpus / machine_total_gpus * 100) if machine_total_gpus > 0 else 0

                # Calculate per-machine slot creation efficiency: only over timestamps where backfill could exist
                if num_machine_intervals_with_unused == 0:
                    # No intervals with unused GPUs, so efficiency is not applicable
                    machine_slot_creation_efficiency = None
                else:
                    avg_machine_unused_when_possible = total_machine_unused_when_possible / num_machine_intervals_with_unused
                    avg_machine_backfill_when_possible = total_machine_backfill_when_possible / num_machine_intervals_with_unused
                    machine_slot_creation_efficiency = (avg_machine_backfill_when_possible / avg_machine_unused_when_possible * 100) if avg_machine_unused_when_possible > 0 else 0

                machines[machine] = {
                    "total_gpus": avg_machine_primary_available,
                    "avg_primary_allocated": avg_machine_primary_claimed,
                    "avg_backfill_allocated": avg_machine_backfill_claimed,
                    "avg_backfill_available": avg_machine_backfill_available,
                    "allocated_pct": machine_allocated_pct,
                    "slot_creation_efficiency": machine_slot_creation_efficiency,
                }

            gpu_type_stats[gpu_type]["machines"] = machines

    return gpu_type_stats


def print_gpu_type_summary(stats: dict, expand_machines: bool = False, category: str = None):
    """
    Print GPU type summary table.

    Args:
        stats: GPU type statistics dictionary
        expand_machines: Whether to show per-machine breakdown
        category: Optional category label for the table header
    """
    if category:
        print(f"\nGPU TYPE SUMMARY - {category.upper()}:")
    else:
        print("\nGPU TYPE SUMMARY:")
    print("=" * 160)

    # Table header
    print(f"{'GPU Type':<30} {'GPUs':>8} {'Primary Avg Alloc':>18} {'Backfill Avg Alloc':>20} {'Backfill Avg Avail':>20} {'Total Alloc %':>15} {'Slot Creation Eff %':>20}")
    print("-" * 160)

    # Sort by GPU type name
    for gpu_type in sorted(stats.keys()):
        gpu_stats = stats[gpu_type]
        short_name = get_human_readable_device_name(gpu_type)

        # Format slot creation efficiency - show "--" if not applicable (None)
        eff = gpu_stats['slot_creation_efficiency']
        eff_str = f"{eff:>19.1f}%" if eff is not None else f"{'--':>20}"

        print(f"{short_name:<30} {gpu_stats['total_gpus']:>8.1f} {gpu_stats['avg_primary_allocated']:>18.1f} "
              f"{gpu_stats['avg_backfill_allocated']:>20.1f} {gpu_stats['avg_backfill_available']:>20.1f} "
              f"{gpu_stats['allocated_pct']:>14.1f}% {eff_str}")

        # Show per-machine breakdown if requested
        if expand_machines and "machines" in gpu_stats:
            machines = gpu_stats["machines"]
            if machines:
                print(f"  {'Machine':<38} {'GPUs':>8} {'Primary Avg Alloc':>18} {'Backfill Avg Alloc':>20} {'Backfill Avg Avail':>20} {'Total Alloc %':>15} {'Slot Creation Eff %':>20}")
                print(f"  {'-' * 158}")

                for machine in sorted(machines.keys()):
                    machine_stats = machines[machine]

                    # Format slot creation efficiency - show "--" if not applicable (None)
                    machine_eff = machine_stats['slot_creation_efficiency']
                    machine_eff_str = f"{machine_eff:>19.1f}%" if machine_eff is not None else f"{'--':>20}"

                    print(f"  {machine:<38} {machine_stats['total_gpus']:>8.1f} {machine_stats['avg_primary_allocated']:>18.1f} "
                          f"{machine_stats['avg_backfill_allocated']:>20.1f} {machine_stats['avg_backfill_available']:>20.1f} "
                          f"{machine_stats['allocated_pct']:>14.1f}% {machine_eff_str}")
                print()


def main(
    db_path: str = None,
    hours_back: int = 24,
    expand_machines: bool = False,
    all_devices: bool = False,
    exclude_hosts_yaml: str = "masked_hosts.yaml",
):
    """
    Generate GPU type summary report.

    Args:
        db_path: Path to SQLite database (auto-detected if not provided)
        hours_back: Number of hours to analyze
        expand_machines: Show per-machine breakdown under each GPU type
        all_devices: Include all device types (including old/uncommon GPUs)
        exclude_hosts_yaml: Path to YAML file with host exclusions
    """
    # Auto-detect database path if not provided
    if db_path is None:
        db_path = get_most_recent_database()
        if not db_path:
            print("Error: No database file found. Please specify --db-path.")
            return
        print(f"Using database: {db_path}")

    # Load host exclusions
    gpu_utils.HOST_EXCLUSIONS = load_host_exclusions(None, exclude_hosts_yaml)

    # Load data
    print(f"Analyzing last {hours_back} hours...")
    df = get_time_filtered_data(db_path, hours_back)

    if len(df) == 0:
        print("No data found in the specified time range.")
        return

    # Calculate and print statistics for Open Capacity machines
    open_capacity_stats = calculate_gpu_type_summary(
        df,
        include_all_devices=all_devices,
        expand_machines=expand_machines,
        machine_category="Open Capacity"
    )
    print_gpu_type_summary(open_capacity_stats, expand_machines=expand_machines, category="Open Capacity")

    # Calculate and print statistics for Researcher Owned machines
    researcher_owned_stats = calculate_gpu_type_summary(
        df,
        include_all_devices=all_devices,
        expand_machines=expand_machines,
        machine_category="Researcher Owned"
    )
    print_gpu_type_summary(researcher_owned_stats, expand_machines=expand_machines, category="Researcher Owned")

    # Show metadata
    print("\n" + "=" * 160)
    print(f"Data Period: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Total Records: {len(df):,}")

    if gpu_utils.HOST_EXCLUSIONS:
        print("\nEXCLUDED HOSTS:")
        for host, reason in gpu_utils.HOST_EXCLUSIONS.items():
            print(f"  {host}: {reason}")

    print("=" * 160)


if __name__ == "__main__":
    typer.run(main)
