#!/usr/bin/env python3
"""
Q4 2025 GPU Statistics Analysis

Analyzes GPU usage for Q4 2025 (October, November, December) including:
- Number of unique jobs run
- GPU hours consumed

Broken down by:
- All of CHTC (Total)
- Prioritized hosts (slot*_* and interactive*_* with non-empty PrioritizedProjects)
- Open Capacity (slot*_* and interactive*_* with empty PrioritizedProjects)
- Backfill slots (backfill*_*)

=== DEFINITIONS ===

"GPU HOURS CONSUMED":
    A measure of GPU allocation time, NOT actual GPU utilization.
    Calculated as: (number of GPUs claimed) × (time they remained claimed)

    A GPU is "consumed" when it's in the 'Claimed' state in HTCondor, meaning
    it has been allocated to a running job. This counts allocation regardless of
    whether the job is actively using the GPU at 100% or sitting idle.

    Example: A job that claims 4 GPUs for 2 hours = 8 GPU hours consumed,
    even if the GPUs were only 50% utilized during that time.

    Time intervals are calculated between successive database snapshots
    (typically 5 minutes apart) to determine how long GPUs remained claimed.

"UNIQUE JOBS":
    Count of distinct GlobalJobId values that ran during the period.
    Each unique job is counted once, regardless of:
    - How many GPUs it used
    - How long it ran
    - How many times it was restarted/rescheduled

    Jobs are identified by GlobalJobId format: scheduler#cluster.proc#timestamp
    Example: "osggrid01.hep.wisc.edu#3313615.0#1759127456"

CATEGORIES (mutually exclusive for regular slots):
    - Total: All GPU slots regardless of type
    - Prioritized: Regular and interactive slots (slot*, interactive*) where PrioritizedProjects is not empty
    - Open Capacity: Regular and interactive slots (slot*, interactive*) where PrioritizedProjects is empty
    - Backfill: backfill* slots (backfill2, backfill3, backfill5, etc.) counted separately regardless of priority

    Note: Regular slot*_* and interactive*_* slots are either Prioritized OR Open Capacity, never both.
    Backfill slots are tracked separately and may have PrioritizedProjects set.
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import argparse


def connect_databases(db_paths):
    """Connect to multiple databases and return connections."""
    connections = []
    for db_path in db_paths:
        if not Path(db_path).exists():
            print(f"Warning: Database {db_path} not found, skipping")
            continue
        try:
            conn = sqlite3.connect(db_path)
            connections.append((db_path, conn))
            print(f"Connected to {db_path}")
        except Exception as e:
            print(f"Error connecting to {db_path}: {e}")
    return connections


def analyze_jobs_and_hours(connections, start_date, end_date):
    """
    Analyze unique jobs and GPU hours for Q4 2025.

    The calculation dynamically determines time intervals between snapshots
    rather than assuming a fixed 15-minute interval. Database snapshots are
    typically 5 minutes apart.

    Returns dict with:
    - total: stats for all CHTC GPUs
    - prioritized: stats for regular/interactive slots with PrioritizedProjects
    - open_capacity: stats for regular/interactive slots without PrioritizedProjects
    - backfill: stats for all backfill*_* slots
    """
    results = {
        'total': {'unique_jobs': set(), 'gpu_hours': 0, 'samples': 0},
        'prioritized': {'unique_jobs': set(), 'gpu_hours': 0, 'samples': 0},
        'open_capacity': {'unique_jobs': set(), 'gpu_hours': 0, 'samples': 0},
        'backfill': {'unique_jobs': set(), 'gpu_hours': 0, 'samples': 0}
    }

    # Track total available GPU hours for efficiency calculation
    available_gpu_hours = 0

    for db_path, conn in connections:
        print(f"\nProcessing {db_path}...")
        cursor = conn.cursor()

        # Query for all claimed GPUs with timestamps
        query = """
        SELECT
            timestamp,
            Name,
            State,
            GlobalJobId,
            PrioritizedProjects,
            AssignedGPUs
        FROM gpu_state
        WHERE State = 'Claimed'
            AND timestamp >= ?
            AND timestamp < ?
            AND GlobalJobId IS NOT NULL
            AND GlobalJobId != ''
        ORDER BY timestamp
        """

        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()

        print(f"  Found {len(rows):,} claimed GPU records")

        # Process records to calculate GPU hours and collect unique jobs
        # Group by timestamp to calculate intervals
        timestamp_data = defaultdict(lambda: {
            'total': [],
            'prioritized': [],
            'open_capacity': [],
            'backfill': []
        })

        for row in rows:
            timestamp, name, state, job_id, prioritized_projects, assigned_gpus = row

            # Count number of GPUs for this slot
            # Each slot can have multiple GPUs assigned (e.g., 4-GPU jobs)
            # AssignedGPUs is a comma-separated list like "GPU-abc123,GPU-def456,GPU-ghi789"
            if assigned_gpus:
                # Count comma-separated GPU IDs
                num_gpus = len([g for g in assigned_gpus.split(',') if g.strip()])
            else:
                # If no AssignedGPUs field, assume 1 GPU (shouldn't happen for Claimed state)
                num_gpus = 1

            # Determine which categories this slot belongs to
            # Backfill slots: backfill2_*, backfill3_*, backfill5_*, etc.
            is_backfill = name.startswith('backfill')
            # Regular slots: slot1_*, slot2_*, slot3_*, slot4_*, etc. (anything starting with 'slot' that's not backfill or interactive)
            is_regular_slot = name.startswith('slot') and not is_backfill
            # Interactive slots - treat like regular slots and classify by priority
            is_interactive = name.startswith('interactive')
            has_priority = prioritized_projects is not None and prioritized_projects.strip() != ''

            # Add to total (everything counts here)
            timestamp_data[timestamp]['total'].append((job_id, num_gpus))
            results['total']['unique_jobs'].add(job_id)

            # Categorize based on slot type and priority
            if is_backfill:
                # Backfill slots - separate category regardless of priority
                # Includes backfill2, backfill3, backfill5, etc.
                timestamp_data[timestamp]['backfill'].append((job_id, num_gpus))
                results['backfill']['unique_jobs'].add(job_id)
            elif is_regular_slot or is_interactive:
                # Regular slots (slot1, slot2, slot3, slot4, etc.) and interactive slots
                # Classify as either prioritized or open capacity based on PrioritizedProjects
                if has_priority:
                    # Slots with PrioritizedProjects = Prioritized
                    timestamp_data[timestamp]['prioritized'].append((job_id, num_gpus))
                    results['prioritized']['unique_jobs'].add(job_id)
                else:
                    # Slots without PrioritizedProjects = Open Capacity
                    timestamp_data[timestamp]['open_capacity'].append((job_id, num_gpus))
                    results['open_capacity']['unique_jobs'].add(job_id)
            # Note: any remaining slots (rare) are only counted in total

        # Calculate GPU hours based on time intervals between snapshots
        #
        # The database contains periodic snapshots of GPU state (typically 5 minutes apart).
        # We calculate the actual time between snapshots rather than assuming a fixed interval.
        #
        # To calculate GPU hours "consumed", we:
        # 1. For each timestamp, count how many GPUs were claimed
        # 2. Calculate the actual time until the next timestamp
        # 3. Assume those GPUs remained claimed for that entire interval
        # 4. GPU hours = number_of_gpus × interval_hours
        # 5. Sum across all intervals for total GPU hours
        #
        # Example timeline with 5-minute intervals:
        #   10:00 AM - 5 GPUs claimed
        #   10:05 AM - 8 GPUs claimed
        #   10:10 AM - 3 GPUs claimed
        #
        # Calculation:
        #   10:00-10:05: 5 GPUs × (5/60) hours = 0.417 GPU hours
        #   10:05-10:10: 8 GPUs × (5/60) hours = 0.667 GPU hours
        #   10:10-next:  3 GPUs × (5/60) hours = 0.250 GPU hours (estimated)
        #   Total: 1.334 GPU hours consumed
        timestamps = sorted(timestamp_data.keys())

        for i in range(len(timestamps)):
            current_time_str = timestamps[i]
            current_time = datetime.fromisoformat(current_time_str.replace('Z', '+00:00'))

            # Calculate time interval to next snapshot
            # This interval represents how long we assume the current GPU state persisted
            if i < len(timestamps) - 1:
                # Use actual time to next snapshot (typically 5 minutes)
                next_time_str = timestamps[i + 1]
                next_time = datetime.fromisoformat(next_time_str.replace('Z', '+00:00'))
                interval_hours = (next_time - current_time).total_seconds() / 3600
            else:
                # For the last timestamp, we don't know how long that state persisted
                # Use the previous interval as an estimate
                if i > 0:
                    prev_time_str = timestamps[i - 1]
                    prev_time = datetime.fromisoformat(prev_time_str.replace('Z', '+00:00'))
                    interval_hours = (current_time - prev_time).total_seconds() / 3600
                else:
                    # If we only have one sample, assume 5-minute interval
                    interval_hours = 5.0 / 60.0  # 5 minutes in hours

            # Calculate and accumulate GPU hours for each category
            for category in ['total', 'prioritized', 'open_capacity', 'backfill']:
                jobs_at_time = timestamp_data[current_time_str][category]
                # Sum up all GPUs claimed in this category at this time
                # (remembering that one job can claim multiple GPUs)
                total_gpus = sum(num_gpus for _, num_gpus in jobs_at_time)

                # GPU hours consumed = number of GPUs × hours they were claimed
                results[category]['gpu_hours'] += total_gpus * interval_hours
                if total_gpus > 0:  # Only count samples where we had data
                    results[category]['samples'] += 1

        # Calculate total available GPU hours (all GPUs regardless of state)
        # This is used to calculate overall efficiency
        # IMPORTANT: Count UNIQUE GPU IDs per timestamp to avoid double-counting
        # (same physical GPU can appear in parent slot, child slots, and backfill slots)
        available_query = """
        SELECT
            timestamp,
            Name,
            AssignedGPUs
        FROM gpu_state
        WHERE timestamp >= ?
            AND timestamp < ?
            AND AssignedGPUs IS NOT NULL
            AND AssignedGPUs != ''
        ORDER BY timestamp
        """

        cursor.execute(available_query, (start_date, end_date))
        available_rows = cursor.fetchall()

        print(f"  Found {len(available_rows):,} total GPU records for efficiency calculation")

        # Group by timestamp and count UNIQUE GPU IDs (to avoid double-counting)
        # available_by_timestamp maps timestamp -> set of GPU IDs
        available_by_timestamp = defaultdict(set)

        for row in available_rows:
            timestamp, name, assigned_gpus = row

            # Add each GPU ID to the set for this timestamp
            # Using a set automatically handles deduplication
            if assigned_gpus:
                gpu_ids = [g.strip() for g in assigned_gpus.split(',') if g.strip()]
                available_by_timestamp[timestamp].update(gpu_ids)

        # Calculate available GPU hours using unique GPU counts
        available_timestamps = sorted(available_by_timestamp.keys())

        for i in range(len(available_timestamps)):
            current_time_str = available_timestamps[i]
            current_time = datetime.fromisoformat(current_time_str.replace('Z', '+00:00'))

            if i < len(available_timestamps) - 1:
                next_time_str = available_timestamps[i + 1]
                next_time = datetime.fromisoformat(next_time_str.replace('Z', '+00:00'))
                interval_hours = (next_time - current_time).total_seconds() / 3600
            else:
                if i > 0:
                    prev_time_str = available_timestamps[i - 1]
                    prev_time = datetime.fromisoformat(prev_time_str.replace('Z', '+00:00'))
                    interval_hours = (current_time - prev_time).total_seconds() / 3600
                else:
                    interval_hours = 5.0 / 60.0

            # Count unique GPUs at this timestamp (set size)
            num_unique_gpus = len(available_by_timestamp[current_time_str])
            available_gpu_hours += num_unique_gpus * interval_hours

        cursor.close()

    # Convert sets to counts
    for category in results:
        results[category]['unique_jobs'] = len(results[category]['unique_jobs'])

    return results, available_gpu_hours


def print_results(results, start_date, end_date, available_gpu_hours=None):
    """Print formatted results."""
    print("\n" + "=" * 80)
    print("Q4 2025 GPU USAGE STATISTICS")
    print("=" * 80)
    print(f"Analysis Period: {start_date} to {end_date}")
    print()

    categories = [
        ('Total', 'total'),
        ('Prioritized', 'prioritized'),
        ('Open Capacity', 'open_capacity'),
        ('Backfill', 'backfill')
    ]

    for label, key in categories:
        stats = results[key]
        print(f"{label}:")
        print(f"  Unique Jobs Run:     {stats['unique_jobs']:,}")
        print(f"  GPU Hours Consumed:  {stats['gpu_hours']:,.1f}")
        print(f"  Data Samples:        {stats['samples']:,}")
        print()

    # Calculate percentages
    total_jobs = results['total']['unique_jobs']
    total_hours = results['total']['gpu_hours']

    if total_jobs > 0:
        print("Job Distribution as % of Total:")
        print(f"  Prioritized Jobs:    {results['prioritized']['unique_jobs'] / total_jobs * 100:.1f}%")
        print(f"  Open Capacity Jobs:  {results['open_capacity']['unique_jobs'] / total_jobs * 100:.1f}%")
        print(f"  Backfill Jobs:       {results['backfill']['unique_jobs'] / total_jobs * 100:.1f}%")
        print()

    if total_hours > 0:
        print("GPU Hours Distribution as % of Total:")
        print(f"  Prioritized Hours:   {results['prioritized']['gpu_hours'] / total_hours * 100:.1f}%")
        print(f"  Open Capacity Hours: {results['open_capacity']['gpu_hours'] / total_hours * 100:.1f}%")
        print(f"  Backfill Hours:      {results['backfill']['gpu_hours'] / total_hours * 100:.1f}%")

        # Verify the sum
        accounted_hours = (results['prioritized']['gpu_hours'] +
                          results['open_capacity']['gpu_hours'] +
                          results['backfill']['gpu_hours'])
        print(f"\n  Accounted Hours:     {accounted_hours:,.1f} ({accounted_hours / total_hours * 100:.1f}%)")
        if abs(accounted_hours - total_hours) > 1:
            unaccounted = total_hours - accounted_hours
            print(f"  Unaccounted Hours:   {unaccounted:,.1f} ({unaccounted / total_hours * 100:.1f}%)")
            print(f"    (likely from other slot types like slot2, slot3, etc.)")
        print()

    # Calculate and display efficiency
    if available_gpu_hours and available_gpu_hours > 0 and total_hours > 0:
        efficiency = (total_hours / available_gpu_hours) * 100
        print("=" * 80)
        print("OVERALL EFFICIENCY")
        print("=" * 80)
        print(f"Total Available GPU Hours:  {available_gpu_hours:,.1f}")
        print(f"Total Claimed GPU Hours:    {total_hours:,.1f}")
        print(f"Overall Efficiency:         {efficiency:.1f}%")
        print()
        print("Note: Efficiency = (Claimed GPU Hours / Available GPU Hours) × 100%")
        print("This measures what percentage of available GPU capacity was allocated to jobs.")
        print()


def main():
    parser = argparse.ArgumentParser(description='Analyze Q4 2025 GPU statistics')
    parser.add_argument(
        '--databases',
        '-d',
        nargs='+',
        default=[
            'gpu_state_2025-10.db',
            'gpu_state_2025-11.db',
            'gpu_state_2025-12.db'
        ],
        help='Database files to analyze (default: Q4 2025 databases)'
    )
    parser.add_argument(
        '--start-date',
        default='2025-10-01',
        help='Start date (YYYY-MM-DD, default: 2025-10-01)'
    )
    parser.add_argument(
        '--end-date',
        default='2026-01-01',
        help='End date (YYYY-MM-DD, default: 2026-01-01)'
    )

    args = parser.parse_args()

    print("Q4 2025 GPU Statistics Analysis")
    print("=" * 80)

    # Connect to databases
    connections = connect_databases(args.databases)

    if not connections:
        print("Error: No databases available")
        return 1

    # Analyze
    results, available_gpu_hours = analyze_jobs_and_hours(connections, args.start_date, args.end_date)

    # Print results
    print_results(results, args.start_date, args.end_date, available_gpu_hours)

    # Close connections
    for _, conn in connections:
        conn.close()

    return 0


if __name__ == '__main__':
    exit(main())
