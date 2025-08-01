#!/usr/bin/env python3
"""
GPU Utilities Module

Common utilities for GPU data filtering, counting, and processing.
Consolidates duplicate functions from across the codebase.
"""

import pandas as pd
import datetime
from typing import Optional, Dict
import yaml
from pathlib import Path

# Global variable to store host exclusion configuration
HOST_EXCLUSIONS = {}
FILTERED_HOSTS_INFO = []


def load_host_exclusions(exclusions_config: Optional[str] = None, yaml_file: Optional[str] = None) -> Dict[str, str]:
    """
    Load host exclusion configuration from YAML file or string.
    
    Args:
        exclusions_config: Optional string with exclusion configuration
        yaml_file: Optional path to YAML file with exclusions
        
    Returns:
        Dictionary mapping excluded host patterns to reasons
    """
    exclusions = {}
    
    if yaml_file and Path(yaml_file).exists():
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                if data and 'excluded_hosts' in data:
                    exclusions = data['excluded_hosts']
        except Exception as e:
            print(f"Warning: Could not load exclusions from {yaml_file}: {e}")
    
    if exclusions_config:
        try:
            data = yaml.safe_load(exclusions_config)
            if data and 'excluded_hosts' in data:
                exclusions.update(data['excluded_hosts'])
        except Exception as e:
            print(f"Warning: Could not parse exclusions config: {e}")
    
    return exclusions


def filter_df(df: pd.DataFrame, utilization: str = "", state: str = "", host: str = "") -> pd.DataFrame:
    """
    Filter DataFrame based on utilization type, state, and host.
    
    Args:
        df: Input DataFrame with GPU state data
        utilization: Filter by utilization type ("Priority", "Shared", "Backfill")
        state: Filter by GPU state ("Claimed", "Unclaimed")
        host: Filter by host name pattern
        
    Returns:
        Filtered DataFrame
    """
    # Always work with a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Apply host exclusions if configured
    if HOST_EXCLUSIONS:
        original_count = len(df)
        # Filter out excluded hosts
        for excluded_host in HOST_EXCLUSIONS.keys():
            df = df[~df['Machine'].str.contains(excluded_host, case=False, na=False)]
        
        filtered_count = len(df)
        if filtered_count < original_count:
            # Track that filtering occurred
            filtered_info = {
                'original_count': original_count,
                'filtered_count': filtered_count,
                'excluded_hosts': HOST_EXCLUSIONS
            }
            # Update global tracking (avoid duplicates)
            if filtered_info not in FILTERED_HOSTS_INFO:
                FILTERED_HOSTS_INFO.append(filtered_info)
    
    if utilization == "Backfill":
        df = df[(df['State'] == state if state != "" else True) & 
                (df['Name'].str.contains(host) if host != "" else True) & 
                (df['Name'].str.contains("backfill"))]
    elif utilization == "Shared":
        df = df[(df['PrioritizedProjects'] == "") & 
                (df['State'] == state if state != "" else True) & 
                (df['Name'].str.contains(host) if host != "" else True) & 
                (~df['Name'].str.contains("backfill"))]
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
        if state == "Claimed":  # Only care about claimed and prioritized
            df = df[(df['PrioritizedProjects'] != "") & 
                    (df['State'] == state if state != "" else True) & 
                    (df['Name'].str.contains(host) if host != "" else True) & 
                    (~df['Name'].str.contains("backfill"))] 
        elif state == "Unclaimed":  # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[((df['PrioritizedProjects'] != "") & 
                     (df['State'] == state if state != "" else True) & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (~df['Name'].str.contains("backfill"))) |
                    ((df['PrioritizedProjects'] != "") & 
                     (df['State'] == "Claimed") & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (df['Name'].str.contains("backfill")))]
        else:  # When state is empty, still need to filter for priority projects
            df = df[(df['PrioritizedProjects'] != "") & 
                    (df['Name'].str.contains(host) if host != "" else True) & 
                    (~df['Name'].str.contains("backfill"))]
    return df


def count_backfill(df: pd.DataFrame, state: str = "", host: str = "") -> int:
    """Count backfill GPUs."""
    df = filter_df(df, "Backfill", state, host)
    return df.shape[0]


def count_shared(df: pd.DataFrame, state: str = "", host: str = "") -> int:
    """Count shared GPUs."""
    df = filter_df(df, "Shared", state, host)
    return df.shape[0]


def count_prioritized(df: pd.DataFrame, state: str = "", host: str = "") -> int:
    """Count prioritized GPUs."""
    df = filter_df(df, "Priority", state, host)
    return df.shape[0]


def get_display_name(class_name: str) -> str:
    """Convert internal class names to user-friendly display names."""
    display_names = {
        "Priority": "Prioritized service",
        "Shared": "Open Capacity",
        "Backfill": "Backfill"
    }
    return display_names.get(class_name, class_name)


def get_required_databases(start_time: datetime.datetime, end_time: datetime.datetime, base_dir: str = ".") -> list:
    """
    Get list of database files needed to cover the specified time range.
    
    Args:
        start_time: Start of time range
        end_time: End of time range  
        base_dir: Directory containing database files
        
    Returns:
        List of database file paths
    """
    db_files = []
    
    # Generate list of months between start and end
    current = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    while current <= end_month:
        db_file = Path(base_dir) / f"gpu_state_{current.strftime('%Y-%m')}.db"
        if db_file.exists():
            db_files.append(str(db_file))
        
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return db_files