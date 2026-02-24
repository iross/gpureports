#!/usr/bin/env python3
"""
GPU Usage Statistics Calculator

Thin orchestration layer that coordinates data loading, calculations,
and reporting for GPU usage analysis. See stats_data, stats_calculations,
and stats_reporting for the implementation details.
"""

import datetime
import os

import pandas as pd
import typer

import gpu_utils
from gpu_utils import load_host_exclusions
from stats_calculations import (
    analyze_gpu_model_at_time,
    calculate_allocation_usage_by_device_enhanced,
    calculate_allocation_usage_by_memory,
    calculate_allocation_usage_enhanced,
    calculate_backfill_usage_by_user,
    calculate_h200_user_breakdown,
    calculate_machines_with_zero_active_gpus,
    calculate_monthly_summary,
    calculate_time_series_usage,
    get_gpu_models_at_time,
)
from stats_data import get_time_filtered_data
from stats_reporting import (
    generate_html_report,
    print_analysis_results,
    print_gpu_model_analysis,
    send_email_report,
)


def run_analysis(
    db_path: str,
    hours_back: int = 24,
    host: str = "",
    analysis_type: str = "allocation",
    bucket_minutes: int = 15,
    end_time: datetime.datetime | None = None,
    group_by_device: bool = False,
    all_devices: bool = False,
    exclude_hosts: str | None = None,
    exclude_hosts_yaml: str | None = None,
    use_enhanced_classification: bool = True,
) -> dict:
    """
    Core analysis function that can be called programmatically.

    Args:
        exclude_hosts: JSON string with host exclusions
        exclude_hosts_yaml: Path to YAML file with host exclusions
        use_enhanced_classification: Use enhanced backfill classification (default: True)

    Returns:
        Dictionary containing analysis results and metadata
    """
    import time

    analysis_start_time = time.time()
    analysis_start_datetime = datetime.datetime.now()

    # Set up host exclusions
    gpu_utils.HOST_EXCLUSIONS = load_host_exclusions(exclude_hosts, exclude_hosts_yaml)
    gpu_utils.FILTERED_HOSTS_INFO = []  # Reset tracking

    # Get filtered data
    df = get_time_filtered_data(db_path, hours_back, end_time)

    if len(df) == 0:
        return {"error": "No data found in the specified time range."}

    # Calculate time buckets for interval counting
    df_temp = df.copy()
    df_temp["timestamp"] = pd.to_datetime(df_temp["timestamp"])
    df_temp["15min_bucket"] = df_temp["timestamp"].dt.floor("15min")
    num_intervals = df_temp["15min_bucket"].nunique()

    result = {
        "metadata": {
            "start_time": df["timestamp"].min(),
            "end_time": df["timestamp"].max(),
            "num_intervals": num_intervals,
            "total_records": len(df),
            "hours_back": hours_back,
            "excluded_hosts": gpu_utils.HOST_EXCLUSIONS,
            "filtered_hosts_info": gpu_utils.FILTERED_HOSTS_INFO,
        }
    }

    if analysis_type == "allocation":
        if group_by_device:
            result["device_stats"] = calculate_allocation_usage_by_device_enhanced(df, host, all_devices)
            result["memory_stats"] = calculate_allocation_usage_by_memory(df, host, all_devices)
            result["h200_user_stats"] = calculate_h200_user_breakdown(df, host, hours_back)
            result["backfill_user_stats"] = calculate_backfill_usage_by_user(df, host, hours_back, all_devices)
            result["zero_active_machines"] = calculate_machines_with_zero_active_gpus(df, host, all_devices)
            result["raw_data"] = df  # Pass raw data for unique cluster totals calculation
            result["host_filter"] = host  # Pass host filter for consistency
        else:
            result["allocation_stats"] = calculate_allocation_usage_enhanced(df, host)

    elif analysis_type == "timeseries":
        result["timeseries_data"] = calculate_time_series_usage(df, bucket_minutes, host)

    elif analysis_type == "monthly":
        result["monthly_stats"] = calculate_monthly_summary(db_path, end_time)

    # Add runtime information to metadata
    analysis_end_time = time.time()
    runtime_seconds = analysis_end_time - analysis_start_time

    result["metadata"]["analysis_runtime_seconds"] = round(runtime_seconds, 3)
    result["metadata"]["analysis_start_datetime"] = analysis_start_datetime.isoformat()
    result["metadata"]["analysis_end_datetime"] = datetime.datetime.now().isoformat()

    return result


def main(
    hours_back: int = typer.Option(24, help="Number of hours to analyze (default: 24)"),
    host: str = typer.Option("", help="Host name to filter results"),
    db_path: str | None = typer.Option(None, help="Path to SQLite database (defaults to current month)"),
    analysis_type: str = typer.Option(
        "allocation", help="Type of analysis: allocation (% GPUs claimed), timeseries, gpu_model_snapshot, or monthly"
    ),
    bucket_minutes: int = typer.Option(15, help="Time bucket size in minutes for timeseries analysis"),
    end_time: str | None = typer.Option(
        None, help="End time for analysis (YYYY-MM-DD HH:MM:SS), defaults to latest in DB"
    ),
    group_by_device: bool = typer.Option(True, help="Group results by GPU device type"),
    all_devices: bool = typer.Option(False, help="Include all device types (if False, filters out older GPUs)"),
    gpu_model: str | None = typer.Option(None, help="GPU model for snapshot analysis (e.g., 'NVIDIA A100-SXM4-80GB')"),
    snapshot_time: str | None = typer.Option(None, help="Specific time for GPU model snapshot (YYYY-MM-DD HH:MM:SS)"),
    window_minutes: int = typer.Option(5, help="Time window in minutes for snapshot search"),
    exclude_hosts: str | None = typer.Option(
        None,
        help='JSON string of hosts to exclude from analysis with reasons, e.g., \'{"host1": "misconfigured", "host2": "maintenance"}\'',
    ),
    exclude_hosts_yaml: str | None = typer.Option(
        "masked_hosts.yaml", help="Path to YAML file containing host exclusions in format: hostname1: reason1"
    ),
    output_format: str = typer.Option("text", help="Output format: 'text' or 'html'"),
    output_file: str | None = typer.Option(None, help="Output file path (optional)"),
    email_to: str | None = typer.Option(None, help="Email address(es) to send HTML report to (comma-separated)"),
    email_from: str = typer.Option("iaross@wisc.edu", help="Sender email address"),
    smtp_server: str = typer.Option("smtp.wiscmail.wisc.edu", help="SMTP server hostname"),
    smtp_port: int = typer.Option(25, help="SMTP server port (25 for standard SMTP, 587 for submission)"),
    email_timeout: int = typer.Option(30, help="SMTP connection timeout in seconds"),
    email_debug: bool = typer.Option(False, help="Enable SMTP debug output"),
):
    """
    Calculate GPU usage statistics for Priority, Shared, and Backfill classes.

    This tool provides flexible analysis of GPU usage patterns over time.
    """
    # Validate host exclusion options
    if exclude_hosts and exclude_hosts_yaml and exclude_hosts_yaml != "masked_hosts.yaml":
        print("Error: Cannot use both --exclude-hosts and --exclude-hosts-yaml. Choose one.")
        return

    # If both exclude_hosts is provided and we're using the default yaml file,
    # prioritize the explicit exclude_hosts option
    if exclude_hosts and exclude_hosts_yaml == "masked_hosts.yaml":
        exclude_hosts_yaml = None

    # Auto-detect database path if not provided
    if db_path is None:
        current_date = datetime.datetime.now()
        current_month_db = f"gpu_state_{current_date.strftime('%Y-%m')}.db"

        # Check if current month database exists
        if os.path.exists(current_month_db):
            db_path = current_month_db
            print(f"Using current month database: {db_path}")
        else:
            # Fall back to most recent database file
            import glob

            db_files = glob.glob("gpu_state_*.db")
            if db_files:
                # Sort by filename (which includes date) to get most recent
                db_path = sorted(db_files)[-1]
                print(f"Current month database not found, using most recent: {db_path}")
            else:
                print("Error: No database files found. Please specify --db-path.")
                return

    # Parse end_time if provided
    parsed_end_time = None
    if end_time:
        try:
            parsed_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print("Error: Invalid end_time format. Use YYYY-MM-DD HH:MM:SS")
            return

    # Handle GPU model snapshot analysis
    if analysis_type == "gpu_model_snapshot":
        if not snapshot_time:
            print("Error: --snapshot-time is required for gpu_model_snapshot analysis")
            return

        # Parse snapshot_time
        try:
            parsed_snapshot_time = datetime.datetime.strptime(snapshot_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print("Error: Invalid snapshot_time format. Use YYYY-MM-DD HH:MM:SS")
            return

        if gpu_model:
            # Analyze specific GPU model
            analysis = analyze_gpu_model_at_time(db_path, gpu_model, parsed_snapshot_time, window_minutes)
            print_gpu_model_analysis(analysis)
        else:
            # Show all available GPU models at that time
            available_models = get_gpu_models_at_time(db_path, parsed_snapshot_time, window_minutes)
            if not available_models:
                print(f"No GPU models found around {parsed_snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}")
                return

            print(f"\nAvailable GPU models at {parsed_snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}:")
            print("=" * 60)
            for i, model in enumerate(available_models, 1):
                print(f"  {i}. {model}")

            print("\nTo analyze a specific model, use:")
            print(f'  --analysis-type gpu_model_snapshot --gpu-model "<model_name>" --snapshot-time "{snapshot_time}"')
        return

    # Run the standard analysis
    try:
        results = run_analysis(
            db_path=db_path,
            hours_back=hours_back,
            host=host,
            analysis_type=analysis_type,
            bucket_minutes=bucket_minutes,
            end_time=parsed_end_time,
            group_by_device=group_by_device,
            all_devices=all_devices,
            exclude_hosts=exclude_hosts,
            exclude_hosts_yaml=exclude_hosts_yaml,
        )
    except ValueError as e:
        print(f"Error: {e}")
        return

    # Print results
    print_analysis_results(results, output_format, output_file)

    # Send email if requested
    if email_to:
        if output_format != "html":
            print("Warning: Email functionality requires HTML format. Generating HTML for email...")

        # Generate HTML content for email
        html_content = generate_html_report(results)

        # Extract usage percentages for email subject
        usage_percentages = {}
        if "device_stats" in results:
            device_stats = results["device_stats"]
            for class_name, device_data in device_stats.items():
                if device_data:
                    # Calculate total percentage for this class
                    total_claimed = sum(stats["avg_claimed"] for stats in device_data.values())
                    total_available = sum(stats["avg_total_available"] for stats in device_data.values())
                    if total_available > 0:
                        usage_percentages[class_name] = (total_claimed / total_available) * 100
        elif "allocation_stats" in results:
            allocation_stats = results["allocation_stats"]
            for class_name, stats in allocation_stats.items():
                usage_percentages[class_name] = stats["allocation_usage_percent"]

        # Send email
        success = send_email_report(
            html_content=html_content,
            to_email=email_to,
            from_email=email_from,
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            usage_percentages=usage_percentages,
            lookback_hours=hours_back,
            timeout=email_timeout,
            debug=email_debug,
            device_stats=results.get("device_stats"),
            analysis_type=analysis_type,
            month=results.get("monthly_stats", {}).get("month")
            if analysis_type == "monthly"
            else (results.get("metadata", {}).get("monthly_period")),
        )

        if not success:
            print("Failed to send email")
            return


if __name__ == "__main__":
    typer.run(main)
