#!/usr/bin/env python3
"""GPU Usage Statistics Calculator - CLI entry point for ad-hoc reports.

Manual, on-demand reports (see justfile's last-day/last-hour/last-day-html
targets). The automated email cron uses usage_stats.py, which already runs
on this same canonical pipeline -- see classify_slots.py/read_data.py
for the implementation.
"""

import datetime
from pathlib import Path


def main(
    hours_back: int = 24,
    host: str = "",
    db_path: str | None = None,
    all_devices: bool = False,
    exclude_hosts_yaml: str | None = "masked_hosts.yaml",
    output_format: str = "text",
    output_file: str | None = None,
):
    """
    GPU Usage Statistics Calculator.

    Args:
        hours_back: Number of hours to analyze (default: 24)
        host: Host name to filter results
        db_path: Path to a gpu_state data file (defaults to current month)
        all_devices: Include all device types (if False, filters out older GPUs)
        exclude_hosts_yaml: Path to YAML file containing host exclusions
        output_format: Output format: 'text' or 'html'
        output_file: Output file path (optional)
    """
    import glob
    import os
    import time

    import classify_slots
    from classify_slots import (
        calculate_allocation_usage_by_device_enhanced,
        calculate_allocation_usage_by_memory,
        calculate_backfill_usage_by_user,
        calculate_h200_user_breakdown,
        prepare_frames,
    )
    from read_data import load_host_exclusions, scan_time_filtered
    from reporting import print_analysis_results

    analysis_start_time = time.time()
    analysis_start_datetime = datetime.datetime.now()

    # Auto-detect data file path if not provided
    if db_path is None:
        current_date = datetime.datetime.now()
        current_month_parquet = f"gpu_state_{current_date.strftime('%Y-%m')}.parquet"

        if os.path.exists(current_month_parquet):
            db_path = current_month_parquet
            print(f"Using current month data file: {db_path}")
        else:
            parquet_files = glob.glob("gpu_state_*.parquet")
            if parquet_files:
                db_path = sorted(parquet_files)[-1]
                print(f"Current month file not found, using most recent: {db_path}")
            else:
                print("Error: No gpu_state data files found. Please specify --db-path.")
                return

    classify_slots.HOST_EXCLUSIONS = load_host_exclusions(None, exclude_hosts_yaml)
    classify_slots.FILTERED_HOSTS_INFO = []

    db_path_obj = Path(db_path)
    data_dir = str(db_path_obj.parent) if db_path_obj.parent != Path(".") else "."

    print(f"Loading data (last {hours_back} hours)...")
    frames = prepare_frames(scan_time_filtered(data_dir, hours_back, None))

    if frames.original_count == 0:
        print("Error: No data found in the specified time range.")
        return

    results = {
        "metadata": {
            "start_time": frames.start_time,
            "end_time": frames.end_time,
            "num_intervals": frames.total_buckets,
            "total_records": frames.original_count,
            "hours_back": hours_back,
            "excluded_hosts": classify_slots.HOST_EXCLUSIONS,
            "filtered_hosts_info": classify_slots.FILTERED_HOSTS_INFO,
        }
    }

    print("Calculating device statistics...")
    results["device_stats"] = calculate_allocation_usage_by_device_enhanced(frames, host, all_devices)
    results["memory_stats"] = calculate_allocation_usage_by_memory(frames, host, all_devices)

    print("Calculating user statistics...")
    results["h200_user_stats"] = calculate_h200_user_breakdown(frames, host, hours_back)
    results["backfill_user_stats"] = calculate_backfill_usage_by_user(frames, host, hours_back, all_devices)
    results["host_filter"] = host

    # Add runtime information
    analysis_end_time = time.time()
    runtime_seconds = analysis_end_time - analysis_start_time
    results["metadata"]["analysis_runtime_seconds"] = round(runtime_seconds, 3)
    results["metadata"]["analysis_start_datetime"] = analysis_start_datetime.isoformat()
    results["metadata"]["analysis_end_datetime"] = datetime.datetime.now().isoformat()

    # Print results
    print_analysis_results(results, output_format, output_file)

    print(f"\nTotal runtime: {runtime_seconds:.2f} seconds")


if __name__ == "__main__":
    import typer

    typer.run(main)
