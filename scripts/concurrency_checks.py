#!/usr/bin/env python3
"""
GPU Job Concurrency Analysis

This script analyzes concurrent GPU job usage by users across 15-minute windows,
providing insights into which users have the most jobs running concurrently
for each GPUJobLength category.
"""

import pandas as pd
import sqlite3
import datetime
import typer
from typing import Optional, Dict, List
from collections import defaultdict


def load_gpu_state_data(db_path: str, hours_back: int = 24) -> pd.DataFrame:
    """Load GPU state data from database within time range."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM gpu_state", conn)
    conn.close()
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Filter to recent time range
    end_time = df['timestamp'].max()
    start_time = end_time - datetime.timedelta(hours=hours_back)
    df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
    
    return df



def analyze_concurrency_from_db(df: pd.DataFrame, window_minutes: int = 15) -> pd.DataFrame:
    """
    Analyze concurrent job usage using GPU state database data.
    This gives us real-time snapshots of running jobs.
    """
    if df.empty:
        return pd.DataFrame()
    
    # Create time buckets
    df = df.copy()
    df['time_bucket'] = df['timestamp'].dt.floor(f'{window_minutes}min')
    
    # Filter to only claimed GPUs with job information
    active_jobs = df[
        (df['State'] == 'Claimed') & 
        (df['GlobalJobId'].notna()) & 
        (df['RemoteOwner'].notna())
    ].copy()
    
    if active_jobs.empty:
        return pd.DataFrame()
    
    # Extract user from RemoteOwner (format: user@domain)
    active_jobs['user'] = active_jobs['RemoteOwner'].str.split('@').str[0]
    
    # For each time bucket, count concurrent jobs per user
    results = []
    
    for bucket in sorted(active_jobs['time_bucket'].unique()):
        bucket_data = active_jobs[active_jobs['time_bucket'] == bucket]
        
        # Count unique jobs per user in this time window
        user_job_counts = bucket_data.groupby('user')['GlobalJobId'].nunique().reset_index()
        user_job_counts.columns = ['user', 'concurrent_jobs']
        user_job_counts['time_bucket'] = bucket
        
        # Add GPU device information
        for _, row in user_job_counts.iterrows():
            user_data = bucket_data[bucket_data['user'] == row['user']]
            gpu_types = user_data['GPUs_DeviceName'].value_counts().to_dict()
            
            result_row = {
                'time_bucket': bucket,
                'user': row['user'],
                'concurrent_jobs': row['concurrent_jobs'],
                'gpu_types': gpu_types,
                'total_gpus': len(user_data)
            }
            results.append(result_row)
    
    return pd.DataFrame(results)


def get_top_concurrent_users(df: pd.DataFrame, metric: str = 'concurrent_jobs_total', 
                           top_n: int = 10) -> pd.DataFrame:
    """Get users with highest average concurrent job counts."""
    if df.empty or metric not in df.columns:
        return pd.DataFrame()
    
    user_avg = df.groupby('user')[metric].agg(['mean', 'max', 'count']).reset_index()
    user_avg.columns = ['user', f'avg_{metric}', f'max_{metric}', 'time_windows']
    
    return user_avg.sort_values(f'avg_{metric}', ascending=False).head(top_n)

def print_concurrency_analysis(db_results: pd.DataFrame):
    """Print formatted concurrency analysis results."""
    print(f"\n{'='*80}")
    print(f"{'GPU JOB CONCURRENCY ANALYSIS':^80}")
    print(f"{'='*80}")
    
    if not db_results.empty:
        print(f"\n{'DATABASE ANALYSIS (Real-time snapshots)':^80}")
        print(f"{'-'*80}")
        
        # Overall stats
        total_windows = db_results['time_bucket'].nunique()
        unique_users = db_results['user'].nunique()
        max_concurrent = db_results['concurrent_jobs'].max()
        avg_concurrent = db_results['concurrent_jobs'].mean()
        
        print(f"Time windows analyzed: {total_windows}")
        print(f"Unique users: {unique_users}")
        print(f"Max concurrent jobs (single user): {max_concurrent}")
        print(f"Average concurrent jobs per user per window: {avg_concurrent:.1f}")
        
        # Top users by concurrent jobs
        top_users_db = get_top_concurrent_users(db_results, 'concurrent_jobs')
        if not top_users_db.empty:
            print(f"\nTop Users by Average Concurrent Jobs:")
            print(f"{'User':<20} {'Avg Concurrent':<15} {'Max Concurrent':<15} {'Time Windows':<12}")
            print(f"{'-'*62}")
            for _, row in top_users_db.iterrows():
                print(f"{row['user']:<20} {row['avg_concurrent_jobs']:<15.1f} "
                      f"{row['max_concurrent_jobs']:<15.0f} {row['time_windows']:<12.0f}")
    else:
        print("No concurrency data found.")

def main(
    db_path: str = typer.Option("gpu_state_2025-06.db", help="Path to GPU state database"),
    hours_back: int = typer.Option(24, help="Hours of data to analyze from database"),
    window_minutes: int = typer.Option(15, help="Time window size in minutes")
):
    """
    Analyze concurrent GPU job usage by users across time windows.
    
    This tool provides insights into which users have the most jobs running
    concurrently using real-time database snapshots.
    """
    
    print(f"Loading GPU state data from {db_path}...")
    try:
        gpu_state_df = load_gpu_state_data(db_path, hours_back)
        if not gpu_state_df.empty:
            print(f"Analyzing concurrency from database ({len(gpu_state_df)} records)...")
            db_results = analyze_concurrency_from_db(gpu_state_df, window_minutes)
        else:
            print("No GPU state data found in specified time range.")
            db_results = pd.DataFrame()
    except Exception as e:
        print(f"Error loading database: {e}")
        db_results = pd.DataFrame()
    
    # Print results
    print_concurrency_analysis(db_results)
    
    # Optional: Save detailed results
    if not db_results.empty:
        output_file = f"concurrency_analysis_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        db_results.to_csv(output_file, index=False)
        print(f"\nDetailed results saved to: {output_file}")

if __name__ == "__main__":
    typer.run(main)