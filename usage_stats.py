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


def get_gpu_models_at_time(
    db_path: str,
    target_time: datetime.datetime,
    window_minutes: int = 5
) -> list:
    """
    Get all GPU models available at a specific time.
    
    Args:
        db_path: Path to SQLite database
        target_time: Time to query for GPU models
        window_minutes: Time window around target_time to search (default: 5 minutes)
    
    Returns:
        List of GPU model names available at the specified time
    """
    conn = sqlite3.connect(db_path)
    
    # Define time window
    start_time = target_time - datetime.timedelta(minutes=window_minutes)
    end_time = target_time + datetime.timedelta(minutes=window_minutes)
    
    query = """
    SELECT DISTINCT GPUs_DeviceName
    FROM gpu_state 
    WHERE GPUs_DeviceName IS NOT NULL
    AND timestamp BETWEEN ? AND ?
    ORDER BY GPUs_DeviceName
    """
    
    df = pd.read_sql_query(query, conn, params=[start_time, end_time])
    conn.close()
    
    return df['GPUs_DeviceName'].tolist()


def get_gpu_model_activity_at_time(
    db_path: str,
    gpu_model: str,
    target_time: datetime.datetime,
    window_minutes: int = 5
) -> pd.DataFrame:
    """
    Get detailed activity for a specific GPU model at a specific time.
    
    Args:
        db_path: Path to SQLite database
        gpu_model: GPU model name (e.g., 'NVIDIA A100-SXM4-80GB')
        target_time: Time to query for activity
        window_minutes: Time window around target_time to search (default: 5 minutes)
    
    Returns:
        DataFrame with detailed GPU activity information
    """
    conn = sqlite3.connect(db_path)
    
    # Define time window
    start_time = target_time - datetime.timedelta(minutes=window_minutes)
    end_time = target_time + datetime.timedelta(minutes=window_minutes)
    
    query = """
    SELECT timestamp, Name, AssignedGPUs, State, GPUs_DeviceName, 
           GPUsAverageUsage, Machine, RemoteOwner, GlobalJobId,
           PrioritizedProjects
    FROM gpu_state 
    WHERE GPUs_DeviceName = ? 
    AND timestamp BETWEEN ? AND ?
    ORDER BY timestamp DESC, Machine, AssignedGPUs
    """
    
    df = pd.read_sql_query(query, conn, params=[gpu_model, start_time, end_time])
    conn.close()
    
    if len(df) > 0:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    return df


def analyze_gpu_model_at_time(
    db_path: str,
    gpu_model: str,
    target_time: datetime.datetime,
    window_minutes: int = 5
) -> dict:
    """
    Analyze what's happening with a specific GPU model at a specific time.
    
    Args:
        db_path: Path to SQLite database
        gpu_model: GPU model name
        target_time: Time to analyze
        window_minutes: Time window to search
    
    Returns:
        Dictionary with analysis results
    """
    df = get_gpu_model_activity_at_time(db_path, gpu_model, target_time, window_minutes)
    
    if len(df) == 0:
        return {
            "error": f"No data found for {gpu_model} around {target_time.strftime('%Y-%m-%d %H:%M:%S')}"
        }
    
    # Get the closest timestamp to target
    df['time_diff'] = abs(df['timestamp'] - target_time)
    closest_time = df.loc[df['time_diff'].idxmin(), 'timestamp']
    
    # Filter to records from the closest timestamp
    snapshot_df = df[df['timestamp'] == closest_time]
    
    # Analyze the snapshot - count unique GPUs only
    unique_gpus = snapshot_df['AssignedGPUs'].dropna().nunique()
    
    # Count active GPUs (those actually running jobs with RemoteOwner)
    active_gpus_count = snapshot_df[
        (snapshot_df['State'] == 'Claimed') & 
        (snapshot_df['RemoteOwner'].notna())
    ]['AssignedGPUs'].dropna().nunique()
    
    # Count idle GPUs (those not running jobs)
    idle_gpus_count = unique_gpus - active_gpus_count
    
    total_gpus = unique_gpus
    claimed_gpus = active_gpus_count  # Rename for compatibility with existing code
    unclaimed_gpus = idle_gpus_count  # Rename for compatibility with existing code
    
    # Categorize by utilization class
    priority_gpus = filter_df(snapshot_df, "Priority", "", "")
    shared_gpus = filter_df(snapshot_df, "Shared", "", "")
    backfill_gpus = filter_df(snapshot_df, "Backfill", "", "")
    
    # Get unique machines
    machines = snapshot_df['Machine'].unique()
    
    # Calculate utilization stats
    claimed_with_usage = snapshot_df[
        (snapshot_df['State'] == 'Claimed') & 
        (snapshot_df['GPUsAverageUsage'].notna())
    ]
    
    avg_utilization = claimed_with_usage['GPUsAverageUsage'].mean() if len(claimed_with_usage) > 0 else 0
    
    # Get job information - ensure unique GPU IDs
    active_jobs_df = snapshot_df[
        (snapshot_df['State'] == 'Claimed') & 
        (snapshot_df['RemoteOwner'].notna())
    ][['RemoteOwner', 'GlobalJobId', 'AssignedGPUs', 'Machine']].copy()
    
    # Remove duplicates based on AssignedGPUs, keeping first occurrence
    active_jobs = active_jobs_df.drop_duplicates(subset=['AssignedGPUs'], keep='first')
    
    # Get inactive GPUs - ensure unique GPU IDs and exclude ones that appear in active jobs
    inactive_gpus_df = snapshot_df[
        snapshot_df['State'] == 'Unclaimed'
    ][['AssignedGPUs', 'Machine', 'PrioritizedProjects']].copy()
    
    # Remove duplicates based on AssignedGPUs, keeping first occurrence
    inactive_gpus_unique = inactive_gpus_df.drop_duplicates(subset=['AssignedGPUs'], keep='first')
    
    # Get list of GPU IDs that are active (have jobs running)
    active_gpu_ids = set(active_jobs['AssignedGPUs'].dropna().tolist())
    
    # Filter out GPUs that appear in active jobs list
    inactive_gpus = inactive_gpus_unique[~inactive_gpus_unique['AssignedGPUs'].isin(active_gpu_ids)]
    
    return {
        "gpu_model": gpu_model,
        "snapshot_time": closest_time,
        "target_time": target_time,
        "window_minutes": window_minutes,
        "summary": {
            "total_gpus": total_gpus,
            "claimed_gpus": claimed_gpus,  # This is now active_gpus_count
            "unclaimed_gpus": unclaimed_gpus,  # This is now idle_gpus_count
            "utilization_percent": (claimed_gpus / total_gpus * 100) if total_gpus > 0 else 0,
            "avg_gpu_usage_percent": avg_utilization * 100 if avg_utilization else 0,
            "num_machines": len(machines)
        },
        "by_class": {
            "Priority": {
                "total": priority_gpus['AssignedGPUs'].dropna().nunique(),
                "claimed": priority_gpus[priority_gpus['State'] == 'Claimed']['AssignedGPUs'].dropna().nunique()
            },
            "Shared": {
                "total": shared_gpus['AssignedGPUs'].dropna().nunique(),
                "claimed": shared_gpus[shared_gpus['State'] == 'Claimed']['AssignedGPUs'].dropna().nunique()
            },
            "Backfill": {
                "total": backfill_gpus['AssignedGPUs'].dropna().nunique(),
                "claimed": backfill_gpus[backfill_gpus['State'] == 'Claimed']['AssignedGPUs'].dropna().nunique()
            }
        },
        "machines": list(machines),
        "active_jobs": active_jobs.to_dict('records') if len(active_jobs) > 0 else [],
        "inactive_gpus": inactive_gpus.to_dict('records') if len(inactive_gpus) > 0 else [],
        "raw_data": snapshot_df
    }


def print_gpu_model_analysis(analysis: dict):
    """Print GPU model analysis results in a formatted way."""
    if "error" in analysis:
        print(analysis["error"])
        return
    
    gpu_model = analysis["gpu_model"]
    snapshot_time = analysis["snapshot_time"]
    target_time = analysis["target_time"]
    summary = analysis["summary"]
    by_class = analysis["by_class"]
    machines = analysis["machines"]
    active_jobs = analysis["active_jobs"]
    inactive_gpus = analysis["inactive_gpus"]
    
    print(f"\n{'='*80}")
    print(f"GPU MODEL ACTIVITY REPORT: {gpu_model}")
    print(f"{'='*80}")
    print(f"Target Time: {target_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Snapshot Time: {snapshot_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Time Difference: {abs((snapshot_time - target_time).total_seconds())} seconds")
    
    print(f"\nSUMMARY:")
    print(f"{'-'*40}")
    print(f"Total GPUs: {summary['total_gpus']}")
    print(f"Active (with jobs): {summary['claimed_gpus']} ({summary['utilization_percent']:.1f}%)")
    print(f"Idle (no jobs): {summary['unclaimed_gpus']}")
    print(f"Avg GPU Usage: {summary['avg_gpu_usage_percent']:.1f}%")
    print(f"Machines: {summary['num_machines']}")
    
    print(f"\nBY UTILIZATION CLASS:")
    print(f"{'-'*40}")
    for class_name, stats in by_class.items():
        if stats['total'] > 0:
            usage_pct = (stats['claimed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"{class_name:>10}: {stats['claimed']:2d}/{stats['total']:2d} ({usage_pct:5.1f}%)")
    
    print(f"\nMACHINES ({len(machines)}):")
    print(f"{'-'*40}")
    for machine in sorted(machines):
        print(f"  {machine}")
    
    if active_jobs:
        print(f"\nACTIVE JOBS ({len(active_jobs)}):")
        print(f"{'-'*70}")
        print(f"{'User':<20} {'Job ID':<15} {'GPU ID':<12} {'Machine':<20}")
        print(f"{'-'*70}")
        for job in active_jobs:
            user = (job.get('RemoteOwner') or 'N/A')[:19]
            job_id = (job.get('GlobalJobId') or 'N/A')[:14]
            gpu_id = (job.get('AssignedGPUs') or 'N/A')[:11]
            machine = (job.get('Machine') or 'N/A')[:19]
            print(f"{user:<20} {job_id:<15} {gpu_id:<12} {machine:<20}")
    else:
        print(f"\nNo active jobs found.")
    
    if inactive_gpus:
        print(f"\nINACTIVE GPUs ({len(inactive_gpus)}):")
        print(f"{'-'*70}")
        print(f"{'GPU ID':<12} {'Machine':<20} {'Priority Projects':<30}")
        print(f"{'-'*70}")
        for gpu in inactive_gpus:
            gpu_id = (gpu.get('AssignedGPUs') or 'N/A')[:11]
            machine = (gpu.get('Machine') or 'N/A')[:19]
            priority_projects = (gpu.get('PrioritizedProjects') or 'None')[:29]
            print(f"{gpu_id:<12} {machine:<20} {priority_projects:<30}")
    else:
        print(f"\nNo inactive GPUs found.")


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
    db_path: str = typer.Option("gpu_state_2025-07.db", help="Path to SQLite database"),
    analysis_type: str = typer.Option(
        "allocation", 
        help="Type of analysis: allocation (% GPUs claimed), timeseries, or gpu_model_snapshot"
    ),
    bucket_minutes: int = typer.Option(15, help="Time bucket size in minutes for timeseries analysis"),
    end_time: Optional[str] = typer.Option(None, help="End time for analysis (YYYY-MM-DD HH:MM:SS), defaults to latest in DB"),
    group_by_device: bool = typer.Option(False, help="Group results by GPU device type"),
    all_devices: bool = typer.Option(False, help="Include all device types (if False, filters out older GPUs)"),
    gpu_model: Optional[str] = typer.Option(None, help="GPU model for snapshot analysis (e.g., 'NVIDIA A100-SXM4-80GB')"),
    snapshot_time: Optional[str] = typer.Option(None, help="Specific time for GPU model snapshot (YYYY-MM-DD HH:MM:SS)"),
    window_minutes: int = typer.Option(5, help="Time window in minutes for snapshot search")
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
    
    # Handle GPU model snapshot analysis
    if analysis_type == "gpu_model_snapshot":
        if not snapshot_time:
            print("Error: --snapshot-time is required for gpu_model_snapshot analysis")
            return
        
        # Parse snapshot_time
        try:
            parsed_snapshot_time = datetime.datetime.strptime(snapshot_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print(f"Error: Invalid snapshot_time format. Use YYYY-MM-DD HH:MM:SS")
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
            print("="*60)
            for i, model in enumerate(available_models, 1):
                print(f"{i:2d}. {model}")
            
            print(f"\nTo analyze a specific model, use:")
            print(f"  --analysis-type gpu_model_snapshot --gpu-model \"<model_name>\" --snapshot-time \"{snapshot_time}\"")
        return
    
    # Run the standard analysis
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