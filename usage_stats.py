#!/usr/bin/env python3
"""
GPU Usage Statistics Calculator

This script calculates average usage statistics for GPU classes (Priority, Shared, Backfill)
over a specified time range. It provides both allocation-based usage (percentage of available
GPUs in use) and performance-based usage (actual GPU utilization metrics).
"""

import pandas as pd
import datetime
import typer
import sqlite3
from typing import Optional

# Define the filtering functions locally to avoid htcondor dependency

def filter_df(df, utilization="", state="", host=""):
    """Filter DataFrame based on utilization type, state, and host."""
    # Always work with a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    if utilization == "Backfill":
        df = df[(df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (df['Name'].str.contains("backfill"))]
    elif utilization == "Shared":
        df = df[(df['PrioritizedProjects'] == "") & (df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (~df['Name'].str.contains("backfill"))]
    elif utilization == "Priority":
        # Do some cleanup -- primary slots still have in-use GPUs listed as Assigned, so remove them if they're in use
        duplicated_gpus = df[~df['AssignedGPUs'].isna()]['AssignedGPUs'].duplicated(keep=False)
        # For duplicated GPUs, we want to keep the Claimed state and drop Unclaimed
        if duplicated_gpus.any():
            # Create a temporary rank column to sort out duplicates. Prefer claimed to unclaimed and primary slots to backfill.
            df['_rank'] = 0  # Default rank for Unclaimed
            df.loc[(df['State'] == 'Claimed') & (~df['Name'].str.contains("backfill")), '_rank'] = 3
            df.loc[(df['State'] == 'Claimed') & (df['Name'].str.contains("backfill")), '_rank'] = 2
            df.loc[(df['State'] == 'Unclaimed') & (~df['Name'].str.contains("backfill")), '_rank'] = 1
            
            # Sort by AssignedGPUs and rank (keeping highest rank first)
            df = df.sort_values(['AssignedGPUs', '_rank'], ascending=[True, False])
            # Drop duplicates, keeping the first occurrence (which will be highest rank)
            df = df.drop_duplicates(subset=['AssignedGPUs'], keep='first')
            # Remove the temporary rank column
            df = df.drop(columns=['_rank'])
        if state == "Claimed": # Only care about claimed and prioritized
            df = df[(df['PrioritizedProjects'] != "") & (df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (~df['Name'].str.contains("backfill"))] 
        elif state == "Unclaimed": # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[((df['PrioritizedProjects'] != "") & (df['State'] == state if state != "" else True) & (df['Name'].str.contains(host) if host != "" else True) & (~df['Name'].str.contains("backfill"))) |
                    ((df['PrioritizedProjects'] != "") & (df['State'] == "Claimed") & (df['Name'].str.contains(host) if host != "" else True) & (df['Name'].str.contains("backfill")))
            ]
    return df

def count_backfill(df, state="", host=""):
    """Count backfill GPUs."""
    df = filter_df(df, "Backfill", state, host)
    return df.shape[0]

def count_shared(df, state="", host=""):
    """Count shared GPUs."""
    df = filter_df(df, "Shared", state, host)
    return df.shape[0]

def count_prioritized(df, state="", host=""):
    """Count prioritized GPUs."""
    df = filter_df(df, "Priority", state, host)
    return df.shape[0]


def get_time_filtered_data(
    db_path: str, 
    hours_back: int = 24,
    end_time: Optional[datetime.datetime] = None
) -> pd.DataFrame:
    """
    Get GPU state data filtered by time range.
    
    Args:
        db_path: Path to SQLite database
        hours_back: Number of hours to look back from end_time
        end_time: End time for the range (defaults to latest timestamp in DB)
    
    Returns:
        DataFrame filtered to the specified time range
    """
    # Get the data
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM gpu_state", conn)
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Determine the time range
    if end_time is None:
        end_time = df['timestamp'].max()
    start_time = end_time - datetime.timedelta(hours=hours_back)
    
    # Filter by time range
    filtered_df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
    
    return filtered_df


def calculate_allocation_usage(df: pd.DataFrame, host: str = "") -> dict:
    """
    Calculate allocation-based usage: percentage of available GPUs that are claimed,
    averaged across 15-minute intervals.
    
    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
    
    Returns:
        Dictionary with usage statistics for each class
    """
    # Create 15-minute time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')
    
    stats = {}
    
    for utilization_type in ["Priority", "Shared", "Backfill"]:
        interval_usage_percentages = []
        total_claimed_gpus = 0
        total_available_gpus = 0
        
        # For each 15-minute interval, count unique GPUs
        for bucket in sorted(df['15min_bucket'].unique()):
            bucket_df = df[df['15min_bucket'] == bucket]
            
            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority":
                claimed_gpus = len(filter_df(bucket_df, "Priority", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Priority", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Shared":
                claimed_gpus = len(filter_df(bucket_df, "Shared", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Shared", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Backfill":
                claimed_gpus = len(filter_df(bucket_df, "Backfill", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Backfill", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            
            total_gpus_this_interval = claimed_gpus + unclaimed_gpus
            
            if total_gpus_this_interval > 0:
                interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                interval_usage_percentages.append(interval_usage)
                total_claimed_gpus += claimed_gpus
                total_available_gpus += total_gpus_this_interval
        
        # Calculate average usage percentage across all intervals
        avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages) if interval_usage_percentages else 0
        
        # Calculate average GPU counts across intervals
        num_intervals = len(df['15min_bucket'].unique())
        avg_claimed = total_claimed_gpus / num_intervals if num_intervals > 0 else 0
        avg_total = total_available_gpus / num_intervals if num_intervals > 0 else 0
        
        stats[utilization_type] = {
            'avg_claimed': avg_claimed,
            'avg_total_available': avg_total,
            'allocation_usage_percent': avg_usage_percentage,
            'num_intervals': num_intervals
        }
    
    return stats


def calculate_performance_usage(df: pd.DataFrame, host: str = "") -> dict:
    """
    Calculate performance-based usage: actual GPU utilization metrics averaged over time.
    
    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
    
    Returns:
        Dictionary with performance usage statistics for each class
    """
    stats = {}
    
    for utilization_type in ["Priority", "Shared", "Backfill"]:
        # Filter to only claimed GPUs with utilization data
        filtered_df = filter_df(df, utilization_type, "Claimed", host)
        
        # Only consider records with valid utilization data
        util_df = filtered_df[
            (filtered_df['GPUsAverageUsage'].notna()) & 
            (filtered_df['GPUsAverageUsage'] >= 0)
        ]
        
        if len(util_df) > 0:
            avg_utilization = util_df['GPUsAverageUsage'].mean() * 100  # Convert to percentage
            total_records = len(util_df)
            unique_gpus = util_df['AssignedGPUs'].nunique()
        else:
            avg_utilization = 0
            total_records = 0
            unique_gpus = 0
        
        stats[utilization_type] = {
            'avg_gpu_utilization_percent': avg_utilization,
            'records_with_utilization': total_records,
            'unique_gpus_used': unique_gpus
        }
    
    return stats


def calculate_time_series_usage(
    df: pd.DataFrame, 
    bucket_minutes: int = 15,
    host: str = ""
) -> pd.DataFrame:
    """
    Calculate usage over time in buckets, counting unique GPUs per interval.
    
    Args:
        df: DataFrame with GPU state data
        bucket_minutes: Size of time buckets in minutes
        host: Optional host filter
    
    Returns:
        DataFrame with time series usage statistics
    """
    # Create time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df[f'{bucket_minutes}min_bucket'] = df['timestamp'].dt.floor(f'{bucket_minutes}min')
    
    time_series_data = []
    
    for bucket in sorted(df[f'{bucket_minutes}min_bucket'].unique()):
        bucket_df = df[df[f'{bucket_minutes}min_bucket'] == bucket]
        bucket_stats = {'timestamp': bucket}
        
        for utilization_type in ["Priority", "Shared", "Backfill"]:
            # Count unique GPUs for this utilization type in this interval
            if utilization_type == "Priority":
                claimed_gpus = len(filter_df(bucket_df, "Priority", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Priority", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Shared":
                claimed_gpus = len(filter_df(bucket_df, "Shared", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Shared", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            elif utilization_type == "Backfill":
                claimed_gpus = len(filter_df(bucket_df, "Backfill", "Claimed", host)['AssignedGPUs'].dropna().unique())
                unclaimed_gpus = len(filter_df(bucket_df, "Backfill", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
            
            total_gpus = claimed_gpus + unclaimed_gpus
            usage_percent = (claimed_gpus / total_gpus * 100) if total_gpus > 0 else 0
            
            bucket_stats[f'{utilization_type.lower()}_claimed'] = claimed_gpus
            bucket_stats[f'{utilization_type.lower()}_total'] = total_gpus
            bucket_stats[f'{utilization_type.lower()}_usage_percent'] = usage_percent
        
        time_series_data.append(bucket_stats)
    
    return pd.DataFrame(time_series_data)


def calculate_allocation_usage_by_device(df: pd.DataFrame, host: str = "", include_all_devices: bool = True) -> dict:
    """
    Calculate allocation-based usage grouped by device type, averaged across 15-minute intervals.
    
    Args:
        df: DataFrame with GPU state data
        host: Optional host filter
        include_all_devices: Whether to include all device types or filter out older ones
    
    Returns:
        Dictionary with usage statistics for each class and device type
    """
    # Create 15-minute time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')
    
    # Get unique device types
    device_types = df['GPUs_DeviceName'].dropna().unique()
    
    stats = {}
    
    for utilization_type in ["Priority", "Shared", "Backfill"]:
        stats[utilization_type] = {}
        
        for device_type in device_types:
            # Skip old/uncommon GPU types for cleaner output (unless requested to include all)
            if not include_all_devices and any(old_gpu in device_type for old_gpu in ["GTX 1080", "P100", "Quadro", "A30", "A40"]):
                continue
                
            interval_usage_percentages = []
            total_claimed_gpus = 0
            total_available_gpus = 0
            
            # For each 15-minute interval, count unique GPUs of this device type
            for bucket in sorted(df['15min_bucket'].unique()):
                bucket_df = df[df['15min_bucket'] == bucket]
                
                # Filter by device type
                device_df = bucket_df[bucket_df['GPUs_DeviceName'] == device_type]
                
                if device_df.empty:
                    continue
                
                # Count unique GPUs for this utilization type and device in this interval
                if utilization_type == "Priority":
                    claimed_gpus = len(filter_df(device_df, "Priority", "Claimed", host)['AssignedGPUs'].dropna().unique())
                    unclaimed_gpus = len(filter_df(device_df, "Priority", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
                elif utilization_type == "Shared":
                    claimed_gpus = len(filter_df(device_df, "Shared", "Claimed", host)['AssignedGPUs'].dropna().unique())
                    unclaimed_gpus = len(filter_df(device_df, "Shared", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
                elif utilization_type == "Backfill":
                    claimed_gpus = len(filter_df(device_df, "Backfill", "Claimed", host)['AssignedGPUs'].dropna().unique())
                    unclaimed_gpus = len(filter_df(device_df, "Backfill", "Unclaimed", host)['AssignedGPUs'].dropna().unique())
                
                total_gpus_this_interval = claimed_gpus + unclaimed_gpus
                
                if total_gpus_this_interval > 0:
                    interval_usage = (claimed_gpus / total_gpus_this_interval) * 100
                    interval_usage_percentages.append(interval_usage)
                    total_claimed_gpus += claimed_gpus
                    total_available_gpus += total_gpus_this_interval
            
            if interval_usage_percentages:
                # Calculate average usage percentage across all intervals
                avg_usage_percentage = sum(interval_usage_percentages) / len(interval_usage_percentages)
                
                # Calculate average GPU counts across intervals
                num_intervals_with_data = len(interval_usage_percentages)
                avg_claimed = total_claimed_gpus / num_intervals_with_data if num_intervals_with_data > 0 else 0
                avg_total = total_available_gpus / num_intervals_with_data if num_intervals_with_data > 0 else 0
                
                stats[utilization_type][device_type] = {
                    'avg_claimed': avg_claimed,
                    'avg_total_available': avg_total,
                    'allocation_usage_percent': avg_usage_percentage,
                    'num_intervals': num_intervals_with_data
                }
    
    return stats


def run_analysis(
    db_path: str,
    hours_back: int = 24,
    host: str = "",
    analysis_type: str = "allocation",
    bucket_minutes: int = 15,
    end_time: Optional[datetime.datetime] = None,
    group_by_device: bool = False,
    all_devices: bool = False
) -> dict:
    """
    Core analysis function that can be called programmatically.
    
    Returns:
        Dictionary containing analysis results and metadata
    """
    # Get filtered data
    df = get_time_filtered_data(db_path, hours_back, end_time)
    
    if len(df) == 0:
        return {"error": "No data found in the specified time range."}
    
    # Calculate time buckets for interval counting
    df_temp = df.copy()
    df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'])
    df_temp['15min_bucket'] = df_temp['timestamp'].dt.floor('15min')
    num_intervals = df_temp['15min_bucket'].nunique()
    
    result = {
        "metadata": {
            "start_time": df['timestamp'].min(),
            "end_time": df['timestamp'].max(),
            "num_intervals": num_intervals,
            "total_records": len(df)
        }
    }
    
    if analysis_type == "allocation":
        if group_by_device:
            result["device_stats"] = calculate_allocation_usage_by_device(df, host, all_devices)
        else:
            result["allocation_stats"] = calculate_allocation_usage(df, host)
    
    elif analysis_type == "timeseries":
        result["timeseries_data"] = calculate_time_series_usage(df, bucket_minutes, host)
    
    return result


def print_analysis_results(results: dict):
    """Print analysis results in a formatted way."""
    if "error" in results:
        print(results["error"])
        return
    
    metadata = results["metadata"]
    
    print(f"\n{'='*70}")
    print(f"{'CHTC GPU UTILIZATION REPORT':^70}")
    print(f"{'='*70}")
    print(f"Period: {metadata['start_time'].strftime('%Y-%m-%d %H:%M')} to {metadata['end_time'].strftime('%Y-%m-%d %H:%M')} ({metadata['num_intervals']} intervals)")
    print(f"{'='*70}")
    print("NOTES:")
    print("A100-80GB - voyles2000 appears to be prioritized but not using PrioritizedProjects attribute")
    print("A100-40GB - Interactive slots are not filtered out")
    
    if "allocation_stats" in results:
        print(f"\nUtilization Summary:")
        print(f"{'-'*70}")
        allocation_stats = results["allocation_stats"]
        
        for class_name, stats in allocation_stats.items():
            print(f"{class_name:>10}: {stats['allocation_usage_percent']:6.1f}% "
                  f"({stats['avg_claimed']:5.1f}/{stats['avg_total_available']:5.1f} GPUs)")
    
    elif "device_stats" in results:
        print(f"\nUsage by Device Type:")
        print(f"{'-'*70}")
        device_stats = results["device_stats"]
        
        # Calculate and display grand totals
        grand_totals = {}
        
        for class_name, device_data in device_stats.items():
            if device_data:  # Only show classes that have data
                print(f"\n{class_name}:")
                print(f"{'-'*50}")
                
                # Calculate totals for this class
                total_claimed = 0
                total_available = 0
                
                for device_type, stats in device_data.items():
                    print(f"  {device_type[:35]:35}: {stats['allocation_usage_percent']:6.1f}% "
                          f"(avg {stats['avg_claimed']:4.1f}/{stats['avg_total_available']:4.1f} GPUs)")
                    total_claimed += stats['avg_claimed']
                    total_available += stats['avg_total_available']
                
                # Calculate and store grand total for this class
                if total_available > 0:
                    grand_total_percent = (total_claimed / total_available) * 100
                    grand_totals[class_name] = {
                        'claimed': total_claimed,
                        'total': total_available,
                        'percent': grand_total_percent
                    }
                    
                    print(f"  {'-'*35:35}   {'-'*6}   {'-'*15}")
                    print(f"  {'TOTAL ' + class_name:35}: {grand_total_percent:6.1f}% "
                          f"(avg {total_claimed:4.1f}/{total_available:4.1f} GPUs)")
        
        # Display overall summary
        if grand_totals:
            print(f"\nCluster Summary:")
            print(f"{'-'*70}")
            
            overall_claimed = sum(stats['claimed'] for stats in grand_totals.values())
            overall_total = sum(stats['total'] for stats in grand_totals.values())
            overall_percent = (overall_claimed / overall_total * 100) if overall_total > 0 else 0
            
            for class_name, stats in grand_totals.items():
                print(f"{class_name:>10}: {stats['percent']:6.1f}% "
                      f"({stats['claimed']:5.1f}/{stats['total']:5.1f} GPUs)")
            
            print(f"{'-'*35}")
            print(f"{'TOTAL':>10}: {overall_percent:6.1f}% "
                  f"({overall_claimed:5.1f}/{overall_total:5.1f} GPUs)")
    
    elif "timeseries_data" in results:
        print(f"\nTime Series Analysis:")
        print(f"{'-'*70}")
        ts_df = results["timeseries_data"]
        
        # Calculate and display averages
        for class_name in ["priority", "shared", "backfill"]:
            usage_col = f"{class_name}_usage_percent"
            claimed_col = f"{class_name}_claimed"
            total_col = f"{class_name}_total"
            
            if all(col in ts_df.columns for col in [usage_col, claimed_col, total_col]):
                avg_usage = ts_df[usage_col].mean()
                avg_claimed = ts_df[claimed_col].mean()
                avg_total = ts_df[total_col].mean()
                print(f"{class_name.title():>10}: {avg_usage:6.1f}% "
                      f"({avg_claimed:5.1f}/{avg_total:5.1f} GPUs)")
        
        # Show recent trend
        print(f"\nRecent Trend:")
        print(f"{'-'*70}")
        recent_df = ts_df.tail(5)
        for _, row in recent_df.iterrows():
            print(f"{row['timestamp'].strftime('%m-%d %H:%M')}: "
                  f"Priority {row['priority_usage_percent']:5.1f}% "
                  f"({int(row['priority_claimed'])}/{int(row['priority_total'])}), "
                  f"Shared {row['shared_usage_percent']:5.1f}% "
                  f"({int(row['shared_claimed'])}/{int(row['shared_total'])}), "
                  f"Backfill {row['backfill_usage_percent']:5.1f}% "
                  f"({int(row['backfill_claimed'])}/{int(row['backfill_total'])})")


def main(
    hours_back: int = typer.Option(24, help="Number of hours to analyze (default: 24)"),
    host: str = typer.Option("", help="Host name to filter results"),
    db_path: str = typer.Option("gpu_state_2025-06.db", help="Path to SQLite database"),
    analysis_type: str = typer.Option(
        "allocation", 
        help="Type of analysis: allocation (% GPUs claimed) or timeseries"
    ),
    bucket_minutes: int = typer.Option(15, help="Time bucket size in minutes for timeseries analysis"),
    end_time: Optional[str] = typer.Option(None, help="End time for analysis (YYYY-MM-DD HH:MM:SS), defaults to latest in DB"),
    group_by_device: bool = typer.Option(False, help="Group results by GPU device type"),
    all_devices: bool = typer.Option(False, help="Include all device types (if False, filters out older GPUs)")
):
    """
    Calculate GPU usage statistics for Priority, Shared, and Backfill classes.
    
    This tool provides flexible analysis of GPU usage patterns over time.
    """
    # Parse end_time if provided
    parsed_end_time = None
    if end_time:
        try:
            parsed_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print(f"Error: Invalid end_time format. Use YYYY-MM-DD HH:MM:SS")
            return
    
    # Run the analysis
    results = run_analysis(
        db_path=db_path,
        hours_back=hours_back,
        host=host,
        analysis_type=analysis_type,
        bucket_minutes=bucket_minutes,
        end_time=parsed_end_time,
        group_by_device=group_by_device,
        all_devices=all_devices
    )
    
    # Print results
    print_analysis_results(results)


if __name__ == "__main__":
    typer.run(main)