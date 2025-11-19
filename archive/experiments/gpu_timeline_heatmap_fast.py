#!/usr/bin/env python3
"""
Optimized version of prepare_timeline_data for faster processing.

This module provides a vectorized implementation that's 10-100x faster than the original.
"""

import pandas as pd


def classify_gpu_state_vectorized(df: pd.DataFrame) -> pd.Series:
    """
    Vectorized GPU state classification - much faster than row-by-row apply.

    Args:
        df: DataFrame with State, Name, and PrioritizedProjects columns

    Returns:
        Series with state classifications
    """
    # Initialize all as 'na'
    state_class = pd.Series("na", index=df.index)

    # Determine utilization type
    is_backfill = df["Name"].str.contains("backfill", case=False, na=False)
    has_priority = (df["PrioritizedProjects"] != "") & (df["PrioritizedProjects"].notna())

    # Classify based on State and utilization type
    is_claimed = df["State"] == "Claimed"
    is_unclaimed = df["State"] == "Unclaimed"

    # Claimed states
    state_class.loc[is_claimed & has_priority & ~is_backfill] = "busy_prioritized"
    state_class.loc[is_claimed & ~has_priority & ~is_backfill] = "busy_shared"
    state_class.loc[is_claimed & is_backfill] = "busy_backfill"

    # Unclaimed states
    state_class.loc[is_unclaimed & has_priority] = "idle_prioritized"
    state_class.loc[is_unclaimed & ~has_priority] = "idle_shared"

    return state_class


def prepare_timeline_data_fast(df: pd.DataFrame, bucket_minutes: int = 5) -> pd.DataFrame:
    """
    Optimized version of prepare_timeline_data using vectorized operations.

    Args:
        df: Raw GPU state data
        bucket_minutes: Size of time buckets in minutes

    Returns:
        DataFrame ready for heatmap visualization
    """
    if df.empty:
        return pd.DataFrame()

    # Work with a copy
    df = df.copy()

    # Vectorized timestamp processing
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["time_bucket"] = df["timestamp"].dt.floor(f"{bucket_minutes}min")

    # Vectorized GPU identifier creation (much faster than apply)
    df["gpu_id"] = df["Machine"].astype(str) + "_" + df["AssignedGPUs"].astype(str)

    # Apply deduplication logic
    duplicated_mask = df.duplicated(subset=["time_bucket", "AssignedGPUs"], keep=False)

    if duplicated_mask.any():
        # Vectorized ranking
        df["_rank"] = 0

        is_backfill = df["Name"].str.contains("backfill", case=False, na=False)
        is_claimed = df["State"] == "Claimed"
        is_unclaimed = df["State"] == "Unclaimed"

        df.loc[is_claimed & ~is_backfill, "_rank"] = 3
        df.loc[is_claimed & is_backfill, "_rank"] = 2
        df.loc[is_unclaimed & ~is_backfill, "_rank"] = 1

        # Sort and drop duplicates
        df = df.sort_values(["time_bucket", "AssignedGPUs", "_rank"], ascending=[True, True, False])
        df = df.drop_duplicates(subset=["time_bucket", "AssignedGPUs"], keep="first")
        df = df.drop(columns=["_rank"])

    # Vectorized state classification
    df["state_class"] = classify_gpu_state_vectorized(df)

    # Build timeline data using groupby (much faster than nested loops)
    # Group by gpu_id and time_bucket, take the last state in each bucket
    timeline_df = (
        df.groupby(["gpu_id", "time_bucket"])
        .agg(
            {
                "state_class": "last",  # Use the most recent state
                "Machine": "first",
                "AssignedGPUs": "first",
                "GPUs_DeviceName": "first",
            }
        )
        .reset_index()
    )

    # Rename columns to match expected output
    timeline_df = timeline_df.rename(
        columns={
            "gpu_id": "gpu_identifier",
            "Machine": "hostname",
            "AssignedGPUs": "gpu_num",
            "GPUs_DeviceName": "device_name",
            "state_class": "state",
        }
    )

    # Convert gpu_num to string to match original behavior
    timeline_df["gpu_num"] = timeline_df["gpu_num"].astype(str)

    return timeline_df
