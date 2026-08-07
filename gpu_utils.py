#!/usr/bin/env python3
"""
GPU Utilities Module (pandas)

Host-exclusion/CHTC-hosts config loading (shared with the canonical
stats_calculations.prepare_frames() pipeline) plus filter_df/filter_df_enhanced
and related pandas DataFrame filtering, still needed by pandas-backed callers
(scripts/host_report.py, stats_calculations.py's small-window pandas tail,
website_generator/, analysis/analyze_task7_troubleshoot.py). See
gpu_utils_polars.py for Parquet/SQLite file discovery.
"""

import re
from pathlib import Path

import pandas as pd
import yaml

# Global variable to store host exclusion configuration
HOST_EXCLUSIONS = {}
FILTERED_HOSTS_INFO = []

# Global variable to cache hosted capacity list
_CHTC_OWNED_HOSTS = None

# Shared constants for GPU slot classification
CLASS_ORDER = [
    "Priority-ResearcherOwned",
    "Priority-CHTCOwned",
    "Shared",
    "Backfill-ResearcherOwned",
    "Backfill-CHTCOwned",
]
UTILIZATION_TYPES = ["Priority", "Shared", "Backfill"]
BACKFILL_SLOT_TYPES = ["Backfill-ResearcherOwned", "Backfill-CHTCOwned"]


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
            with open(chtc_owned_path) as f:
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


def load_host_exclusions(exclusions_config: str | None = None, yaml_file: str | None = None) -> dict[str, str]:
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
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
                if data and "excluded_hosts" in data:
                    exclusions = data["excluded_hosts"]
        except Exception as e:
            print(f"Warning: Could not load exclusions from {yaml_file}: {e}")

    if exclusions_config:
        try:
            data = yaml.safe_load(exclusions_config)
            if data and "excluded_hosts" in data:
                exclusions.update(data["excluded_hosts"])
        except Exception as e:
            print(f"Warning: Could not parse exclusions config: {e}")

    return exclusions


def _apply_duplicate_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep one row per (timestamp, GPU) when a GPU appears on multiple slots.

    Prefer primary slots over backfill slots; within each type, prefer Claimed >
    Unclaimed > Drained, so a Drained primary slot is not displaced by a Drained
    backfill slot and then incorrectly excluded by the "not backfill" filters.
    One exception: a primary slot that is idle with PreventJobsReason set loses
    to a Claimed backfill slot, so a GPU still finishing a backfill job counts
    as Allocated in the backfill class rather than Prevented or Available.
    """
    duplicated_gpus = df[~df["AssignedGPUs"].isna()]["AssignedGPUs"].duplicated(keep=False)
    if not duplicated_gpus.any():
        return df

    is_primary = ~df["Name"].str.contains("backfill")
    is_backfill = df["Name"].str.contains("backfill")
    if "PreventJobsReason" in df.columns:
        prevented_idle = (
            df["PreventJobsReason"].notna()
            & (df["PreventJobsReason"].astype(str).str.strip() != "")
            & (df["State"] != "Claimed")
        )
    else:
        prevented_idle = pd.Series(False, index=df.index)

    df["_rank"] = 0  # Backfill Drained / other (lowest)
    df.loc[is_backfill & (df["State"] == "Unclaimed"), "_rank"] = 1  # Backfill Unclaimed
    df.loc[is_primary & prevented_idle, "_rank"] = 2  # Primary idle with PreventJobsReason
    df.loc[is_backfill & (df["State"] == "Claimed"), "_rank"] = 3  # Backfill Claimed
    df.loc[is_primary & ~prevented_idle, "_rank"] = 4  # Primary Drained / other
    df.loc[is_primary & ~prevented_idle & (df["State"] == "Unclaimed"), "_rank"] = 5  # Primary Unclaimed
    df.loc[is_primary & (df["State"] == "Claimed"), "_rank"] = 6  # Primary Claimed

    # Sort by AssignedGPUs and rank, then keep the highest-ranked row per GPU.
    # Only deduplicate within each timestamp, not across different timestamps.
    df = df.sort_values(["AssignedGPUs", "_rank"], ascending=[True, False])
    df = df.drop_duplicates(subset=["timestamp", "AssignedGPUs"], keep="first")
    return df.drop(columns=["_rank"])


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
        # Filter out excluded hosts in a single scan
        excluded_pattern = "|".join(re.escape(excluded_host) for excluded_host in HOST_EXCLUSIONS)
        df = df[~df["Machine"].str.contains(excluded_pattern, case=False, na=False)]

        filtered_count = len(df)
        if filtered_count < original_count:
            # Track that filtering occurred
            filtered_info = {
                "original_count": original_count,
                "filtered_count": filtered_count,
                "excluded_hosts": HOST_EXCLUSIONS,
            }
            # Update global tracking (avoid duplicates)
            if filtered_info not in FILTERED_HOSTS_INFO:
                FILTERED_HOSTS_INFO.append(filtered_info)

    if utilization == "Backfill":
        df = df[
            (df["State"] == state if state != "" else True)
            & (df["Name"].str.contains(host) if host != "" else True)
            & (df["Name"].str.contains("backfill"))
        ]
    elif utilization == "Shared":
        # Apply same duplicate cleanup logic as Priority - shared GPUs can also appear in backfill slots
        df = _apply_duplicate_cleanup(df)
        not_primary_excluded = ~df["Name"].str.contains("backfill") & ~df["Name"].str.contains("interactive")
        if state == "Claimed":  # Only care about claimed shared GPUs
            df = df[
                (df["PrioritizedProjects"] == "")
                & (df["State"] == state if state != "" else True)
                & (df["Name"].str.contains(host) if host != "" else True)
                & not_primary_excluded
            ]
        elif (
            state == "Unclaimed"
        ):  # Care about unclaimed shared GPUs, but some might be claimed as backfill so count those.
            df = df[
                (
                    (df["PrioritizedProjects"] == "")
                    & (df["State"] == state if state != "" else True)
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & not_primary_excluded
                )
                | (
                    (df["PrioritizedProjects"] == "")
                    & (df["State"] == "Claimed")
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (df["Name"].str.contains("backfill"))
                )
            ]
        else:  # When state is empty, still need to filter for shared machines (no priority projects)
            df = df[
                (df["PrioritizedProjects"] == "")
                & (df["Name"].str.contains(host) if host != "" else True)
                & not_primary_excluded
            ]
    elif utilization == "Priority":
        # Do some cleanup -- primary slots still have in-use GPUs listed as Assigned, so remove them if they're in use
        df = _apply_duplicate_cleanup(df)
        if state == "Claimed":  # Only care about claimed and prioritized
            df = df[
                (df["PrioritizedProjects"] != "")
                & (df["State"] == state if state != "" else True)
                & (df["Name"].str.contains(host) if host != "" else True)
                & (~df["Name"].str.contains("backfill"))
            ]
        elif (
            state == "Unclaimed"
        ):  # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[
                (
                    (df["PrioritizedProjects"] != "")
                    & (df["State"] == state if state != "" else True)
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (~df["Name"].str.contains("backfill"))
                )
                | (
                    (df["PrioritizedProjects"] != "")
                    & (df["State"] == "Claimed")
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (df["Name"].str.contains("backfill"))
                )
            ]
        else:  # When state is empty, still need to filter for priority projects
            df = df[
                (df["PrioritizedProjects"] != "")
                & (df["Name"].str.contains(host) if host != "" else True)
                & (~df["Name"].str.contains("backfill"))
            ]
    return df


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
        # Filter out excluded hosts in a single scan
        excluded_pattern = "|".join(re.escape(excluded_host) for excluded_host in HOST_EXCLUSIONS)
        df = df[~df["Machine"].str.contains(excluded_pattern, case=False, na=False)]

        filtered_count = len(df)
        if filtered_count < original_count:
            # Track that filtering occurred
            filtered_info = {
                "original_count": original_count,
                "filtered_count": filtered_count,
                "excluded_hosts": HOST_EXCLUSIONS,
            }
            # Update global tracking (avoid duplicates)
            if filtered_info not in FILTERED_HOSTS_INFO:
                FILTERED_HOSTS_INFO.append(filtered_info)

    chtc_owned_hosts = load_chtc_owned_hosts()

    if utilization == "Priority-ResearcherOwned":
        # Priority slots on researcher owned machines (non-empty PrioritizedProjects AND not in hosted capacity)
        # Do some cleanup -- primary slots still have in-use GPUs listed as Assigned, so remove them if they're in use
        df = _apply_duplicate_cleanup(df)
        if state == "Claimed":  # Only care about claimed and prioritized
            df = df[
                (df["PrioritizedProjects"] != "")
                & (~df["Machine"].isin(chtc_owned_hosts))
                & (df["State"] == state if state != "" else True)
                & (df["Name"].str.contains(host) if host != "" else True)
                & (~df["Name"].str.contains("backfill"))
            ]
        elif (
            state == "Unclaimed"
        ):  # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[
                (
                    (df["PrioritizedProjects"] != "")
                    & (~df["Machine"].isin(chtc_owned_hosts))
                    & (df["State"] == state if state != "" else True)
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (~df["Name"].str.contains("backfill"))
                )
                | (
                    (df["PrioritizedProjects"] != "")
                    & (~df["Machine"].isin(chtc_owned_hosts))
                    & (df["State"] == "Claimed")
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (df["Name"].str.contains("backfill"))
                )
            ]
        else:  # When state is empty, still need to filter for priority projects
            df = df[
                (df["PrioritizedProjects"] != "")
                & (~df["Machine"].isin(chtc_owned_hosts))
                & (df["Name"].str.contains(host) if host != "" else True)
                & (~df["Name"].str.contains("backfill"))
            ]
    elif utilization == "Priority-CHTCOwned":
        # Priority slots on hosted capacity machines (non-empty PrioritizedProjects AND in hosted capacity)
        # Do some cleanup -- primary slots still have in-use GPUs listed as Assigned, so remove them if they're in use
        df = _apply_duplicate_cleanup(df)
        if state == "Claimed":  # Only care about claimed and prioritized
            df = df[
                (df["PrioritizedProjects"] != "")
                & (df["Machine"].isin(chtc_owned_hosts))
                & (df["State"] == state if state != "" else True)
                & (df["Name"].str.contains(host) if host != "" else True)
                & (~df["Name"].str.contains("backfill"))
            ]
        elif (
            state == "Unclaimed"
        ):  # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[
                (
                    (df["PrioritizedProjects"] != "")
                    & (df["Machine"].isin(chtc_owned_hosts))
                    & (df["State"] == state if state != "" else True)
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (~df["Name"].str.contains("backfill"))
                )
                | (
                    (df["PrioritizedProjects"] != "")
                    & (df["Machine"].isin(chtc_owned_hosts))
                    & (df["State"] == "Claimed")
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (df["Name"].str.contains("backfill"))
                )
            ]
        else:  # When state is empty, still need to filter for priority projects
            df = df[
                (df["PrioritizedProjects"] != "")
                & (df["Machine"].isin(chtc_owned_hosts))
                & (df["Name"].str.contains(host) if host != "" else True)
                & (~df["Name"].str.contains("backfill"))
            ]
    elif utilization in ["Backfill-ResearcherOwned", "Backfill-CHTCOwned", "Backfill-OpenCapacity"]:
        # Classify backfill slots by machine's primary ownership, not the backfill slot's PrioritizedProjects
        # First identify researcher-owned machines (machines with any non-empty PrioritizedProjects in primary slots)
        primary_slots = df[~df["Name"].str.contains("backfill")].copy()
        researcher_machines = set(
            primary_slots[
                (primary_slots["PrioritizedProjects"] != "")
                & (primary_slots["PrioritizedProjects"].notna())
                & (~primary_slots["Machine"].isin(chtc_owned_hosts))
            ]["Machine"].unique()
        )

        # Filter to backfill slots only
        df = df[df["Name"].str.contains("backfill")].copy()
        if state:
            df = df[df["State"] == state]
        if host:
            df = df[df["Name"].str.contains(host)]

        # Classify based on machine ownership
        if utilization == "Backfill-ResearcherOwned":
            df = df[df["Machine"].isin(researcher_machines)]
        elif utilization == "Backfill-CHTCOwned":
            df = df[df["Machine"].isin(chtc_owned_hosts)]
        elif utilization == "Backfill-OpenCapacity":
            df = df[(~df["Machine"].isin(chtc_owned_hosts)) & (~df["Machine"].isin(researcher_machines))]
    elif utilization == "Shared":
        # Apply same duplicate cleanup logic as Priority - shared GPUs can also appear in backfill slots
        df = _apply_duplicate_cleanup(df)
        not_primary_excluded = ~df["Name"].str.contains("backfill") & ~df["Name"].str.contains("interactive")
        if state == "Claimed":  # Only care about claimed shared GPUs
            df = df[
                (df["PrioritizedProjects"] == "")
                & (df["State"] == state if state != "" else True)
                & (df["Name"].str.contains(host) if host != "" else True)
                & not_primary_excluded
            ]
        elif (
            state == "Unclaimed"
        ):  # Care about unclaimed shared GPUs, but some might be claimed as backfill so count those.
            df = df[
                (
                    (df["PrioritizedProjects"] == "")
                    & (df["State"] == state if state != "" else True)
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & not_primary_excluded
                )
                | (
                    (df["PrioritizedProjects"] == "")
                    & (df["State"] == "Claimed")
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (df["Name"].str.contains("backfill"))
                )
            ]
        else:  # When state is empty, still need to filter for shared machines (no priority projects)
            df = df[
                (df["PrioritizedProjects"] == "")
                & (df["Name"].str.contains(host) if host != "" else True)
                & not_primary_excluded
            ]
    elif utilization == "Priority":
        # Do some cleanup -- primary slots still have in-use GPUs listed as Assigned, so remove them if they're in use
        df = _apply_duplicate_cleanup(df)
        if state == "Claimed":  # Only care about claimed and prioritized
            df = df[
                (df["PrioritizedProjects"] != "")
                & (df["State"] == state if state != "" else True)
                & (df["Name"].str.contains(host) if host != "" else True)
                & (~df["Name"].str.contains("backfill"))
            ]
        elif (
            state == "Unclaimed"
        ):  # Care about unclaimed and prioritized, but some might be claimed as backfill so count those.
            df = df[
                (
                    (df["PrioritizedProjects"] != "")
                    & (df["State"] == state if state != "" else True)
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (~df["Name"].str.contains("backfill"))
                )
                | (
                    (df["PrioritizedProjects"] != "")
                    & (df["State"] == "Claimed")
                    & (df["Name"].str.contains(host) if host != "" else True)
                    & (df["Name"].str.contains("backfill"))
                )
            ]
        else:  # When state is empty, still need to filter for priority projects
            df = df[
                (df["PrioritizedProjects"] != "")
                & (df["Name"].str.contains(host) if host != "" else True)
                & (~df["Name"].str.contains("backfill"))
            ]
    return df


def get_gpu_performance_tier(device_name: str) -> str:
    """
    Classify GPU device into performance tier.

    Args:
        device_name: Full GPU device name (e.g., 'NVIDIA H100 80GB HBM3')

    Returns:
        Performance tier: 'Flagship' or 'Standard'
    """
    # Flagship tier: H100, H200, and 80GB A100
    flagship_patterns = [
        "H100",
        "H200",
        "A100-SXM4-80GB",
        "A100 80GB",
    ]

    for pattern in flagship_patterns:
        if pattern in device_name:
            return "Flagship"

    return "Standard"


def get_display_name(class_name: str) -> str:
    """Convert internal class names to user-friendly display names."""
    display_names = {
        "Priority": "Prioritized service",  # Legacy support
        "Priority-ResearcherOwned": "Researcher-Owned Hardware",
        "Priority-CHTCOwned": "Researcher-Reserved Capacity",
        "Shared": "Open Capacity",
        "Backfill": "Secondary (Backfill)",  # Legacy support
        "Backfill-ResearcherOwned": "Researcher-Owned Hardware",
        "Backfill-CHTCOwned": "Researcher-Reserved Capacity",
        "Backfill-OpenCapacity": "Secondary (Backfill) — Open Capacity",
        "CHTC Owned": "Researcher-Reserved Capacity",
        "Researcher Owned": "Researcher-Owned Hardware",
        "Open Capacity": "Open Capacity",
        # New tier-based display names for Open Capacity breakdown
        "Open Capacity (Flagship)": "Open Capacity (Flagship)",
        "Open Capacity (Standard)": "Open Capacity (Standard)",
    }
    return display_names.get(class_name, class_name)


def analyze_backfill_utilization_by_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze backfill usage patterns over time using consistent methodology.

    Args:
        df: DataFrame with GPU state data

    Returns:
        DataFrame with daily utilization statistics by slot type
    """
    # Create daily buckets for analysis
    df["date"] = df["timestamp"].dt.date
    df["15min_bucket"] = df["timestamp"].dt.floor("15min")

    usage_data = []

    # Analyze usage for each day and slot type
    for slot_type in BACKFILL_SLOT_TYPES:
        filtered_df = filter_df_enhanced(df, slot_type, "", "")
        if filtered_df.empty:
            continue

        daily_stats = []

        # Process each day separately to match usage_stats.py methodology
        for date in sorted(df["date"].unique()):
            day_df = filtered_df[filtered_df["date"] == date]
            if day_df.empty:
                continue

            # Get all 15-minute buckets for this day
            day_buckets = day_df["15min_bucket"].unique()

            total_assigned = 0
            total_claimed = 0
            bucket_count = 0

            for bucket in day_buckets:
                bucket_df = day_df[day_df["15min_bucket"] == bucket]
                if bucket_df.empty:
                    continue

                # Count unique GPUs in this bucket (all states)
                unique_gpus = bucket_df["AssignedGPUs"].nunique()

                # Count unique GPUs that are claimed
                claimed_gpus = bucket_df[bucket_df["State"] == "Claimed"]["AssignedGPUs"].nunique()

                total_assigned += unique_gpus
                total_claimed += claimed_gpus
                bucket_count += 1

            if bucket_count > 0:
                # Calculate average GPUs per bucket for this day
                avg_assigned = total_assigned / bucket_count
                avg_claimed = total_claimed / bucket_count
                utilization = (avg_claimed / avg_assigned * 100) if avg_assigned > 0 else 0

                daily_stats.append(
                    {
                        "date": date,
                        "slot_type": slot_type.replace("Backfill-", ""),
                        "AssignedGPUs": avg_assigned,
                        "State": avg_claimed,
                        "utilization": utilization,
                    }
                )

        if daily_stats:
            usage_data.extend(daily_stats)

    if not usage_data:
        return pd.DataFrame()

    return pd.DataFrame(usage_data)
