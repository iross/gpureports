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

# Global variable to cache hosted capacity list
_HOSTED_CAPACITY_HOSTS = None


def load_hosted_capacity_hosts(hosted_capacity_file: str = "hosted_capacity") -> set:
    """
    Load hosted capacity hosts from file.
    
    Args:
        hosted_capacity_file: Path to file containing hosted capacity host names
        
    Returns:
        Set of hosted capacity host names
    """
    global _HOSTED_CAPACITY_HOSTS
    
    if _HOSTED_CAPACITY_HOSTS is not None:
        return _HOSTED_CAPACITY_HOSTS
    
    hosted_capacity_hosts = set()
    hosted_capacity_path = Path(hosted_capacity_file)
    
    if hosted_capacity_path.exists():
        try:
            with open(hosted_capacity_path, 'r') as f:
                for line in f:
                    host = line.strip()
                    if host:  # Skip empty lines
                        hosted_capacity_hosts.add(host)
        except Exception as e:
            print(f"Warning: Could not load hosted capacity hosts from {hosted_capacity_file}: {e}")
    else:
        print(f"Warning: Hosted capacity file {hosted_capacity_file} not found")
    
    _HOSTED_CAPACITY_HOSTS = hosted_capacity_hosts
    return hosted_capacity_hosts


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
        # Apply same duplicate cleanup logic as Priority - shared GPUs can also appear in backfill slots
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
        if state == "Claimed":  # Only care about claimed shared GPUs
            df = df[(df['PrioritizedProjects'] == "") & 
                    (df['State'] == state if state != "" else True) & 
                    (df['Name'].str.contains(host) if host != "" else True) & 
                    (~df['Name'].str.contains("backfill"))]
        elif state == "Unclaimed":  # Care about unclaimed shared GPUs, but some might be claimed as backfill so count those.
            df = df[((df['PrioritizedProjects'] == "") & 
                     (df['State'] == state if state != "" else True) & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (~df['Name'].str.contains("backfill"))) |
                    ((df['PrioritizedProjects'] == "") & 
                     (df['State'] == "Claimed") & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (df['Name'].str.contains("backfill")))]
        else:  # When state is empty, still need to filter for shared machines (no priority projects)
            df = df[(df['PrioritizedProjects'] == "") & 
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


def classify_machine_category(machine: str, prioritized_projects: str) -> str:
    """
    Classify a machine into one of the new categories.
    
    Args:
        machine: Machine name/hostname
        prioritized_projects: PrioritizedProjects field value
        
    Returns:
        Category: "Hosted Capacity", "Researcher Owned", or "Open Capacity"
    """
    hosted_capacity_hosts = load_hosted_capacity_hosts()
    
    # Check if machine is in hosted capacity list
    if machine in hosted_capacity_hosts:
        return "Hosted Capacity"
    
    # Check if machine has non-empty PrioritizedProjects
    if prioritized_projects and prioritized_projects.strip():
        return "Researcher Owned"
    
    # Default to Open Capacity
    return "Open Capacity"


def filter_df_by_machine_category(df: pd.DataFrame, category: str) -> pd.DataFrame:
    """
    Filter DataFrame by machine category.
    
    Args:
        df: Input DataFrame with GPU state data
        category: Machine category ("Hosted Capacity", "Researcher Owned", "Open Capacity")
        
    Returns:
        Filtered DataFrame
    """
    df = df.copy()
    hosted_capacity_hosts = load_hosted_capacity_hosts()
    
    if category == "Hosted Capacity":
        df = df[df['Machine'].isin(hosted_capacity_hosts)]
    elif category == "Researcher Owned":
        # Researcher owned: has PrioritizedProjects AND not in hosted capacity list
        df = df[
            (df['PrioritizedProjects'] != "") & 
            (df['PrioritizedProjects'].notna()) & 
            (~df['Machine'].isin(hosted_capacity_hosts))
        ]
    elif category == "Open Capacity":
        # Open capacity: no PrioritizedProjects AND not in hosted capacity list
        df = df[
            ((df['PrioritizedProjects'] == "") | (df['PrioritizedProjects'].isna())) & 
            (~df['Machine'].isin(hosted_capacity_hosts))
        ]
    
    return df


def get_machines_by_category(df: pd.DataFrame) -> dict:
    """
    Get list of machines in each category.
    
    Args:
        df: DataFrame with Machine and PrioritizedProjects columns
        
    Returns:
        Dictionary mapping category names to lists of machine names
    """
    # Get unique machines with their PrioritizedProjects
    unique_machines = df.groupby('Machine')['PrioritizedProjects'].first().reset_index()
    
    categories = {
        "Hosted Capacity": [],
        "Researcher Owned": [],
        "Open Capacity": []
    }
    
    for _, row in unique_machines.iterrows():
        category = classify_machine_category(row['Machine'], row['PrioritizedProjects'])
        categories[category].append(row['Machine'])
    
    # Sort lists for consistent output
    for category in categories:
        categories[category].sort()
    
    return categories


def filter_df_enhanced(df: pd.DataFrame, utilization: str = "", state: str = "", host: str = "") -> pd.DataFrame:
    """
    Filter DataFrame with new classification categories.
    
    Args:
        df: Input DataFrame with GPU state data
        utilization: Filter by type ("Priority", "Shared", "Backfill-ResearcherOwned", "Backfill-HostedCapacity", "Backfill-OpenCapacity")
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
    
    hosted_capacity_hosts = load_hosted_capacity_hosts()
    
    if utilization == "Backfill-ResearcherOwned":
        # Backfill slots on researcher owned machines
        df = df[
            (df['State'] == state if state != "" else True) & 
            (df['Name'].str.contains(host) if host != "" else True) & 
            (df['Name'].str.contains("backfill")) &
            (df['PrioritizedProjects'] != "") & 
            (df['PrioritizedProjects'].notna()) & 
            (~df['Machine'].isin(hosted_capacity_hosts))
        ]
    elif utilization == "Backfill-HostedCapacity":
        # Backfill slots on hosted capacity machines
        df = df[
            (df['State'] == state if state != "" else True) & 
            (df['Name'].str.contains(host) if host != "" else True) & 
            (df['Name'].str.contains("backfill")) &
            (df['Machine'].isin(hosted_capacity_hosts))
        ]
    elif utilization == "Backfill-OpenCapacity":
        # Backfill slots on open capacity machines (reclassified as Backfill-OpenCapacity)
        df = df[
            (df['State'] == state if state != "" else True) & 
            (df['Name'].str.contains(host) if host != "" else True) & 
            (df['Name'].str.contains("backfill")) &
            ((df['PrioritizedProjects'] == "") | (df['PrioritizedProjects'].isna())) & 
            (~df['Machine'].isin(hosted_capacity_hosts))
        ]
    elif utilization == "Shared":
        # Apply same duplicate cleanup logic as Priority - shared GPUs can also appear in backfill slots
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
        if state == "Claimed":  # Only care about claimed shared GPUs
            df = df[(df['PrioritizedProjects'] == "") & 
                    (df['State'] == state if state != "" else True) & 
                    (df['Name'].str.contains(host) if host != "" else True) & 
                    (~df['Name'].str.contains("backfill"))]
        elif state == "Unclaimed":  # Care about unclaimed shared GPUs, but some might be claimed as backfill so count those.
            df = df[((df['PrioritizedProjects'] == "") & 
                     (df['State'] == state if state != "" else True) & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (~df['Name'].str.contains("backfill"))) |
                    ((df['PrioritizedProjects'] == "") & 
                     (df['State'] == "Claimed") & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (df['Name'].str.contains("backfill")))]
        else:  # When state is empty, still need to filter for shared machines (no priority projects)
            df = df[(df['PrioritizedProjects'] == "") & 
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


def count_backfill_researcher_owned(df: pd.DataFrame, state: str = "", host: str = "") -> int:
    """Count backfill GPUs on researcher owned machines."""
    df = filter_df_enhanced(df, "Backfill-ResearcherOwned", state, host)
    return df.shape[0]


def count_backfill_hosted_capacity(df: pd.DataFrame, state: str = "", host: str = "") -> int:
    """Count backfill GPUs on hosted capacity machines."""
    df = filter_df_enhanced(df, "Backfill-HostedCapacity", state, host)
    return df.shape[0]


def count_glidein(df: pd.DataFrame, state: str = "", host: str = "") -> int:
    """Count Backfill-OpenCapacity GPUs (formerly backfill on open capacity)."""
    df = filter_df_enhanced(df, "Backfill-OpenCapacity", state, host)
    return df.shape[0]


def get_display_name(class_name: str) -> str:
    """Convert internal class names to user-friendly display names."""
    display_names = {
        "Priority": "Prioritized service",
        "Shared": "Open Capacity",
        "Backfill": "Backfill",  # Legacy support
        "Backfill-ResearcherOwned": "Backfill (Researcher Owned)",
        "Backfill-HostedCapacity": "Backfill (Hosted Capacity)",
        "Backfill-OpenCapacity": "Backfill (Open Capacity)",
        "Hosted Capacity": "Hosted Capacity",
        "Researcher Owned": "Researcher Owned",
        "Open Capacity": "Open Capacity"
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


def get_most_recent_database(base_dir: str = ".") -> Optional[str]:
    """
    Find the most recent database file in the given directory.
    
    Args:
        base_dir: Directory to search for database files
        
    Returns:
        Path to the most recent database file, or None if none found
    """
    import glob
    from pathlib import Path
    
    # Find all database files matching the pattern
    pattern = str(Path(base_dir) / "gpu_state_*.db")
    db_files = glob.glob(pattern)
    
    if not db_files:
        return None
    
    # Sort by filename (which contains YYYY-MM date) to get the most recent
    db_files.sort()
    return db_files[-1]


def get_latest_timestamp_from_most_recent_db(base_dir: str = ".") -> Optional[datetime.datetime]:
    """
    Get the latest timestamp from the most recent database file.
    
    Args:
        base_dir: Directory containing database files
        
    Returns:
        Latest timestamp from the most recent database, or None if not found
    """
    import sqlite3
    import pandas as pd
    
    most_recent_db = get_most_recent_database(base_dir)
    if not most_recent_db:
        return None
    
    try:
        conn = sqlite3.connect(most_recent_db)
        df_temp = pd.read_sql_query("SELECT MAX(timestamp) as max_time FROM gpu_state", conn)
        conn.close()
        if len(df_temp) > 0 and df_temp['max_time'].iloc[0] is not None:
            return pd.to_datetime(df_temp['max_time'].iloc[0])
    except Exception:
        pass
    
    return None
