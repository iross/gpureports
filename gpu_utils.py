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
_CHTC_OWNED_HOSTS = None

# Shared constants for GPU slot classification
CLASS_ORDER = ["Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared", "Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]
UTILIZATION_TYPES = ["Priority", "Shared", "Backfill"]
BACKFILL_SLOT_TYPES = ["Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]


def load_chtc_owned_hosts(chtc_owned_file: str = "chtc_owned") -> set:
    """
    Load CHTC owned hosts from file.
    
    Args:
        chtc_owned_file: Path to file containing CHTC owned host names
        
    Returns:
        Set of CHTC owned host names
    """
    global _CHTC_OWNED_HOSTS
    
    if _CHTC_OWNED_HOSTS is not None:
        return _CHTC_OWNED_HOSTS
    
    chtc_owned_hosts = set()
    chtc_owned_path = Path(chtc_owned_file)
    
    if chtc_owned_path.exists():
        try:
            with open(chtc_owned_path, 'r') as f:
                for line in f:
                    host = line.strip()
                    if host:  # Skip empty lines
                        chtc_owned_hosts.add(host)
        except Exception as e:
            print(f"Warning: Could not load CHTC owned hosts from {chtc_owned_file}: {e}")
    else:
        print(f"Warning: CHTC owned file {chtc_owned_file} not found")
    
    _CHTC_OWNED_HOSTS = chtc_owned_hosts
    return chtc_owned_hosts


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
            # Only deduplicate within each timestamp, not across different timestamps
            df = df.drop_duplicates(subset=['timestamp', 'AssignedGPUs'], keep='first')
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
            # Only deduplicate within each timestamp, not across different timestamps
            df = df.drop_duplicates(subset=['timestamp', 'AssignedGPUs'], keep='first')
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
        Category: "CHTC Owned", "Researcher Owned", or "Open Capacity"
    """
    chtc_owned_hosts = load_chtc_owned_hosts()
    
    # Check if machine is in CHTC owned list
    if machine in chtc_owned_hosts:
        return "CHTC Owned"
    
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
        category: Machine category ("CHTC Owned", "Researcher Owned", "Open Capacity")
        
    Returns:
        Filtered DataFrame
    """
    df = df.copy()
    chtc_owned_hosts = load_chtc_owned_hosts()
    
    if category == "CHTC Owned":
        df = df[df['Machine'].isin(chtc_owned_hosts)]
    elif category == "Researcher Owned":
        # Researcher owned: has PrioritizedProjects AND not in CHTC owned list
        df = df[
            (df['PrioritizedProjects'] != "") & 
            (df['PrioritizedProjects'].notna()) & 
            (~df['Machine'].isin(chtc_owned_hosts))
        ]
    elif category == "Open Capacity":
        # Open capacity: no PrioritizedProjects AND not in CHTC owned list
        df = df[
            ((df['PrioritizedProjects'] == "") | (df['PrioritizedProjects'].isna())) & 
            (~df['Machine'].isin(chtc_owned_hosts))
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
        "CHTC Owned": [],
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
        utilization: Filter by type ("Priority-ResearcherOwned", "Priority-CHTCOwned", "Shared", "Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity")
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
    
    chtc_owned_hosts = load_chtc_owned_hosts()
    
    if utilization == "Priority-ResearcherOwned":
        # Priority slots on researcher owned machines (non-empty PrioritizedProjects AND not in hosted capacity)
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
            # Only deduplicate within each timestamp, not across different timestamps
            df = df.drop_duplicates(subset=['timestamp', 'AssignedGPUs'], keep='first')
            # Remove the temporary rank column
            df = df.drop(columns=['_rank'])
        if state == "Claimed":  # Only care about claimed and prioritized
            df = df[(df['PrioritizedProjects'] != "") & 
                    (~df['Machine'].isin(chtc_owned_hosts)) &
                    (df['State'] == state if state != "" else True) & 
                    (df['Name'].str.contains(host) if host != "" else True) & 
                    (~df['Name'].str.contains("backfill"))] 
        elif state == "Unclaimed":  # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[((df['PrioritizedProjects'] != "") & 
                     (~df['Machine'].isin(chtc_owned_hosts)) &
                     (df['State'] == state if state != "" else True) & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (~df['Name'].str.contains("backfill"))) |
                    ((df['PrioritizedProjects'] != "") & 
                     (~df['Machine'].isin(chtc_owned_hosts)) &
                     (df['State'] == "Claimed") & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (df['Name'].str.contains("backfill")))]
        else:  # When state is empty, still need to filter for priority projects
            df = df[(df['PrioritizedProjects'] != "") & 
                    (~df['Machine'].isin(chtc_owned_hosts)) &
                    (df['Name'].str.contains(host) if host != "" else True) & 
                    (~df['Name'].str.contains("backfill"))]
    elif utilization == "Priority-CHTCOwned":
        # Priority slots on hosted capacity machines (non-empty PrioritizedProjects AND in hosted capacity)
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
            # Only deduplicate within each timestamp, not across different timestamps
            df = df.drop_duplicates(subset=['timestamp', 'AssignedGPUs'], keep='first')
            # Remove the temporary rank column
            df = df.drop(columns=['_rank'])
        if state == "Claimed":  # Only care about claimed and prioritized
            df = df[(df['PrioritizedProjects'] != "") & 
                    (df['Machine'].isin(chtc_owned_hosts)) &
                    (df['State'] == state if state != "" else True) & 
                    (df['Name'].str.contains(host) if host != "" else True) & 
                    (~df['Name'].str.contains("backfill"))] 
        elif state == "Unclaimed":  # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[((df['PrioritizedProjects'] != "") & 
                     (df['Machine'].isin(chtc_owned_hosts)) &
                     (df['State'] == state if state != "" else True) & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (~df['Name'].str.contains("backfill"))) |
                    ((df['PrioritizedProjects'] != "") & 
                     (df['Machine'].isin(chtc_owned_hosts)) &
                     (df['State'] == "Claimed") & 
                     (df['Name'].str.contains(host) if host != "" else True) & 
                     (df['Name'].str.contains("backfill")))]
        else:  # When state is empty, still need to filter for priority projects
            df = df[(df['PrioritizedProjects'] != "") & 
                    (df['Machine'].isin(chtc_owned_hosts)) &
                    (df['Name'].str.contains(host) if host != "" else True) & 
                    (~df['Name'].str.contains("backfill"))]
    elif utilization == "Backfill-ResearcherOwned":
        # Backfill slots on researcher owned machines
        df = df[
            (df['State'] == state if state != "" else True) & 
            (df['Name'].str.contains(host) if host != "" else True) & 
            (df['Name'].str.contains("backfill")) &
            (df['PrioritizedProjects'] != "") & 
            (df['PrioritizedProjects'].notna()) & 
            (~df['Machine'].isin(chtc_owned_hosts))
        ]
    elif utilization == "Backfill-CHTCOwned":
        # Backfill slots on hosted capacity machines
        df = df[
            (df['State'] == state if state != "" else True) & 
            (df['Name'].str.contains(host) if host != "" else True) & 
            (df['Name'].str.contains("backfill")) &
            (df['Machine'].isin(chtc_owned_hosts))
        ]
    elif utilization == "Backfill-OpenCapacity":
        # Backfill slots on open capacity machines (reclassified as Backfill-OpenCapacity)
        df = df[
            (df['State'] == state if state != "" else True) & 
            (df['Name'].str.contains(host) if host != "" else True) & 
            (df['Name'].str.contains("backfill")) &
            ((df['PrioritizedProjects'] == "") | (df['PrioritizedProjects'].isna())) & 
            (~df['Machine'].isin(chtc_owned_hosts))
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
            # Only deduplicate within each timestamp, not across different timestamps
            df = df.drop_duplicates(subset=['timestamp', 'AssignedGPUs'], keep='first')
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
            # Only deduplicate within each timestamp, not across different timestamps
            df = df.drop_duplicates(subset=['timestamp', 'AssignedGPUs'], keep='first')
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


def count_backfill_chtc_owned(df: pd.DataFrame, state: str = "", host: str = "") -> int:
    """Count backfill GPUs on CHTC owned machines."""
    df = filter_df_enhanced(df, "Backfill-CHTCOwned", state, host)
    return df.shape[0]


def count_glidein(df: pd.DataFrame, state: str = "", host: str = "") -> int:
    """Count Backfill-OpenCapacity GPUs (formerly backfill on open capacity)."""
    df = filter_df_enhanced(df, "Backfill-OpenCapacity", state, host)
    return df.shape[0]


def get_display_name(class_name: str) -> str:
    """Convert internal class names to user-friendly display names."""
    display_names = {
        "Priority": "Prioritized service",  # Legacy support
        "Priority-ResearcherOwned": "Prioritized (Researcher Owned)",
        "Priority-CHTCOwned": "Prioritized (CHTC Owned)",
        "Shared": "Open Capacity",
        "Backfill": "Backfill",  # Legacy support
        "Backfill-ResearcherOwned": "Backfill (Researcher Owned)",
        "Backfill-CHTCOwned": "Backfill (CHTC Owned)",
        "Backfill-OpenCapacity": "Backfill (Open Capacity)",
        "CHTC Owned": "CHTC Owned",
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


def analyze_backfill_utilization_by_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze backfill usage patterns over time using consistent methodology.
    
    Args:
        df: DataFrame with GPU state data
        
    Returns:
        DataFrame with daily utilization statistics by slot type
    """
    # Create daily buckets for analysis
    df['date'] = df['timestamp'].dt.date
    df['15min_bucket'] = df['timestamp'].dt.floor('15min')
    
    usage_data = []
    
    # Analyze usage for each day and slot type
    for slot_type in BACKFILL_SLOT_TYPES:
        filtered_df = filter_df_enhanced(df, slot_type, "", "")
        if filtered_df.empty:
            continue
        
        daily_stats = []
        
        # Process each day separately to match usage_stats.py methodology
        for date in sorted(df['date'].unique()):
            day_df = filtered_df[filtered_df['date'] == date]
            if day_df.empty:
                continue
                
            # Get all 15-minute buckets for this day
            day_buckets = day_df['15min_bucket'].unique()
            
            total_assigned = 0
            total_claimed = 0
            bucket_count = 0
            
            for bucket in day_buckets:
                bucket_df = day_df[day_df['15min_bucket'] == bucket]
                if bucket_df.empty:
                    continue
                
                # Count unique GPUs in this bucket (all states)
                unique_gpus = bucket_df['AssignedGPUs'].nunique()
                
                # Count unique GPUs that are claimed
                claimed_gpus = bucket_df[bucket_df['State'] == 'Claimed']['AssignedGPUs'].nunique()
                
                total_assigned += unique_gpus
                total_claimed += claimed_gpus
                bucket_count += 1
            
            if bucket_count > 0:
                # Calculate average GPUs per bucket for this day
                avg_assigned = total_assigned / bucket_count
                avg_claimed = total_claimed / bucket_count
                utilization = (avg_claimed / avg_assigned * 100) if avg_assigned > 0 else 0
                
                daily_stats.append({
                    'date': date,
                    'slot_type': slot_type.replace('Backfill-', ''),
                    'AssignedGPUs': avg_assigned,
                    'State': avg_claimed,
                    'utilization': utilization
                })
        
        if daily_stats:
            usage_data.extend(daily_stats)
    
    if not usage_data:
        return pd.DataFrame()
        
    return pd.DataFrame(usage_data)
