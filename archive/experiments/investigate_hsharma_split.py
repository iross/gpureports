#!/usr/bin/env python3
"""
Investigate hsharma split between Open Capacity and Researcher Owned
"""

import sqlite3
import pandas as pd
from datetime import timedelta

DB_PATH = "gpu_state_2026-01.db"
MACHINE = "hsharma-chtcgpu4000.chtc.wisc.edu"

# Connect to database
conn = sqlite3.connect(DB_PATH)

# Get data for the last 24 hours
end_time_query = "SELECT MAX(timestamp) FROM gpu_state"
end_time_df = pd.read_sql_query(end_time_query, conn)
end_time = pd.to_datetime(end_time_df.iloc[0, 0])
start_time = end_time - timedelta(hours=24)

query = f"""
SELECT * FROM gpu_state
WHERE Machine = '{MACHINE}'
AND timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
AND timestamp <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
"""

df = pd.read_sql_query(query, conn)
conn.close()

df["timestamp"] = pd.to_datetime(df["timestamp"])

print(f"Analyzing {MACHINE}")
print(f"Total records: {len(df)}")
print(f"Unique timestamps: {df['timestamp'].nunique()}")

# Split by PrioritizedProjects
open_capacity_df = df[
    (df["PrioritizedProjects"] == "") | (df["PrioritizedProjects"].isna())
]
researcher_owned_df = df[
    (df["PrioritizedProjects"] != "") & (df["PrioritizedProjects"].notna())
]

print(f"\nOpen Capacity records (PrioritizedProjects=''): {len(open_capacity_df)}")
print(f"  Unique timestamps: {open_capacity_df['timestamp'].nunique()}")
print(f"  Unique GPUs: {open_capacity_df['AssignedGPUs'].nunique()}")

print(f"\nResearcher Owned records (PrioritizedProjects!= ''): {len(researcher_owned_df)}")
print(f"  Unique timestamps: {researcher_owned_df['timestamp'].nunique()}")
print(f"  Unique GPUs: {researcher_owned_df['AssignedGPUs'].nunique()}")

# Analyze each category separately
for category_name, category_df in [("Open Capacity", open_capacity_df), ("Researcher Owned", researcher_owned_df)]:
    if len(category_df) == 0:
        continue

    print(f"\n{'='*140}")
    print(f"{category_name.upper()} ANALYSIS")
    print('='*140)

    # Separate primary and backfill
    primary_df = category_df[~category_df["Name"].str.contains("backfill", case=False, na=False)].copy()
    backfill_df = category_df[category_df["Name"].str.contains("backfill", case=False, na=False)].copy()

    partitionable_pattern = r'^slot\d+@'
    primary_df["is_partitionable"] = primary_df["Name"].str.match(partitionable_pattern, case=False, na=False)

    total_primary_claimed = 0
    total_primary_available = 0
    total_backfill_available = 0
    total_unused_when_possible = 0
    total_backfill_when_possible = 0
    num_intervals_with_unused = 0
    num_intervals = 0

    all_timestamps = sorted(set(primary_df["timestamp"].unique()) | set(backfill_df["timestamp"].unique()))

    for timestamp in all_timestamps:
        primary_snapshot = primary_df[primary_df["timestamp"] == timestamp]
        backfill_snapshot = backfill_df[backfill_df["timestamp"] == timestamp]

        dynamic_df = primary_snapshot[~primary_snapshot["is_partitionable"]]
        primary_claimed = len(dynamic_df[dynamic_df["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())
        primary_available = len(primary_snapshot["AssignedGPUs"].dropna().unique())
        backfill_available = len(backfill_snapshot["AssignedGPUs"].dropna().unique())

        total_primary_claimed += primary_claimed
        total_primary_available += primary_available
        total_backfill_available += backfill_available
        num_intervals += 1

        unused_at_timestamp = primary_available - primary_claimed
        if unused_at_timestamp > 0:
            total_unused_when_possible += unused_at_timestamp
            total_backfill_when_possible += backfill_available
            num_intervals_with_unused += 1

    avg_primary_available = total_primary_available / num_intervals if num_intervals > 0 else 0
    avg_primary_claimed = total_primary_claimed / num_intervals if num_intervals > 0 else 0
    avg_backfill_available = total_backfill_available / num_intervals if num_intervals > 0 else 0

    avg_unused_when_possible = total_unused_when_possible / num_intervals_with_unused if num_intervals_with_unused > 0 else 0
    avg_backfill_when_possible = total_backfill_when_possible / num_intervals_with_unused if num_intervals_with_unused > 0 else 0
    efficiency = (avg_backfill_when_possible / avg_unused_when_possible * 100) if avg_unused_when_possible > 0 else 0

    print(f"Number of timestamps: {num_intervals}")
    print(f"Number with unused GPUs: {num_intervals_with_unused}")
    print(f"Avg Primary Available: {avg_primary_available:.2f}")
    print(f"Avg Primary Claimed: {avg_primary_claimed:.2f}")
    print(f"Avg Backfill Available (all timestamps): {avg_backfill_available:.2f}")
    print(f"Avg Unused When Possible: {avg_unused_when_possible:.2f}")
    print(f"Avg Backfill When Possible: {avg_backfill_when_possible:.2f}")
    print(f"Slot Creation Efficiency: {efficiency:.1f}%")
