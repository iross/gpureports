#!/usr/bin/env python3
"""
Example script demonstrating GPU usage statistics plotting

This script shows various ways to use the plotting functionality
for different analysis scenarios.
"""

import datetime
from pathlib import Path
from usage_stats import run_analysis, get_time_filtered_data, calculate_time_series_usage
from plot_usage_stats import (
    create_usage_timeline_plot,
    create_summary_dashboard,
    create_device_usage_heatmap
)

def example_basic_plots():
    """Example: Create basic plots for the last 6 hours."""
    print("Example 1: Basic plots for last 6 hours")
    
    # Get data for last 6 hours
    db_path = "gpu_state_2025-06.db"
    df = get_time_filtered_data(db_path, hours_back=6)
    
    if len(df) == 0:
        print("No data available")
        return
    
    # Calculate time series
    ts_df = calculate_time_series_usage(df, bucket_minutes=15)
    
    # Create timeline plot
    print("Creating timeline plot...")
    create_usage_timeline_plot(
        ts_df, 
        "GPU Usage - Last 6 Hours",
        "example_timeline.png"
    )
    
    # Create device heatmap
    print("Creating device heatmap...")
    create_device_usage_heatmap(
        df,
        "Device Usage - Last 6 Hours", 
        "example_heatmap.png"
    )
    
    print("Basic plots saved as example_timeline.png and example_heatmap.png")


def example_programmatic_analysis():
    """Example: Programmatic analysis with custom time ranges."""
    print("\nExample 2: Programmatic analysis")
    
    # Define time range
    end_time = datetime.datetime.now()
    start_time = end_time - datetime.timedelta(hours=8)
    
    # Run analysis
    results = run_analysis(
        "gpu_state_2025-06.db",
        hours_back=8,
        analysis_type="timeseries",
        bucket_minutes=15
    )
    
    if "error" in results:
        print(f"Error: {results['error']}")
        return
    
    # Get the data
    df = get_time_filtered_data("gpu_state_2025-06.db", hours_back=8)
    ts_df = results["timeseries_data"]
    
    # Create summary dashboard
    print("Creating summary dashboard...")
    period_str = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}"
    
    create_summary_dashboard(
        df, ts_df, period_str,
        "example_dashboard.png"
    )
    
    print("Dashboard saved as example_dashboard.png")
    
    # Print some basic statistics
    metadata = results["metadata"]
    print(f"Analysis period: {period_str}")
    print(f"Total records: {metadata['total_records']:,}")
    print(f"Time intervals: {metadata['num_intervals']}")
    
    # Calculate averages
    for gpu_class in ['priority', 'shared', 'backfill']:
        usage_col = f'{gpu_class}_usage_percent'
        if usage_col in ts_df.columns:
            avg_usage = ts_df[usage_col].mean()
            print(f"{gpu_class.title()} average usage: {avg_usage:.1f}%")


def example_host_specific_analysis():
    """Example: Analysis for specific host pattern."""
    print("\nExample 3: Host-specific analysis")
    
    # Analyze specific host pattern
    host_pattern = "gpu"  # Will match hosts containing "gpu"
    
    # Get data
    df = get_time_filtered_data("gpu_state_2025-06.db", hours_back=4)
    
    if len(df) == 0:
        print("No data available")
        return
    
    # Filter for specific host and calculate time series
    ts_df = calculate_time_series_usage(df, bucket_minutes=15, host=host_pattern)
    
    if len(ts_df) == 0:
        print(f"No data found for host pattern: {host_pattern}")
        return
    
    # Create timeline plot for this host
    print(f"Creating timeline for hosts matching '{host_pattern}'...")
    create_usage_timeline_plot(
        ts_df,
        f"GPU Usage - Hosts matching '{host_pattern}'",
        f"example_host_{host_pattern}.png"
    )
    
    print(f"Host-specific plot saved as example_host_{host_pattern}.png")


def example_comparison_plots():
    """Example: Create comparison plots for different time periods."""
    print("\nExample 4: Time period comparison")
    
    # Compare last 3 hours vs previous 3 hours
    time_periods = [
        {"name": "Recent (Last 3 hours)", "hours_back": 3, "file_suffix": "recent"},
        {"name": "Earlier (3-6 hours ago)", "hours_back": 6, "file_suffix": "earlier"}
    ]
    
    for period in time_periods:
        print(f"Creating plots for: {period['name']}")
        
        # Calculate end time for the period
        if period['file_suffix'] == 'earlier':
            # For earlier period, end 3 hours ago
            end_time = datetime.datetime.now() - datetime.timedelta(hours=3)
        else:
            # For recent period, end now
            end_time = None
        
        # Get data
        df = get_time_filtered_data(
            "gpu_state_2025-06.db", 
            hours_back=3,
            end_time=end_time
        )
        
        if len(df) == 0:
            print(f"No data for {period['name']}")
            continue
        
        # Calculate time series
        ts_df = calculate_time_series_usage(df, bucket_minutes=15)
        
        # Create timeline
        create_usage_timeline_plot(
            ts_df,
            f"GPU Usage - {period['name']}",
            f"example_comparison_{period['file_suffix']}.png"
        )
    
    print("Comparison plots saved as example_comparison_recent.png and example_comparison_earlier.png")


def example_export_data():
    """Example: Export time series data to CSV for external analysis."""
    print("\nExample 5: Export data to CSV")
    
    # Get recent data
    df = get_time_filtered_data("gpu_state_2025-06.db", hours_back=12)
    
    if len(df) == 0:
        print("No data available")
        return
    
    # Calculate time series
    ts_df = calculate_time_series_usage(df, bucket_minutes=15)
    
    # Export to CSV
    output_file = "gpu_usage_timeseries.csv"
    ts_df.to_csv(output_file, index=False)
    
    print(f"Time series data exported to {output_file}")
    print(f"Columns: {list(ts_df.columns)}")
    print(f"Time range: {ts_df['timestamp'].min()} to {ts_df['timestamp'].max()}")
    print(f"Records: {len(ts_df)}")


def main():
    """Run all examples."""
    print("GPU Usage Statistics Plotting Examples")
    print("=" * 50)
    
    # Create output directory
    output_dir = Path("examples")
    output_dir.mkdir(exist_ok=True)
    
    # Change to output directory for examples
    import os
    original_dir = os.getcwd()
    os.chdir(output_dir)
    
    try:
        # Run examples
        example_basic_plots()
        example_programmatic_analysis()
        example_host_specific_analysis()
        example_comparison_plots()
        example_export_data()
        
        print(f"\nAll examples completed! Check the '{output_dir}' directory for output files.")
        
    except Exception as e:
        print(f"Error running examples: {e}")
        print("Make sure the database file exists and contains recent data.")
    
    finally:
        # Return to original directory
        os.chdir(original_dir)


if __name__ == "__main__":
    main()