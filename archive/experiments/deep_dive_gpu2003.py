#!/usr/bin/env python3
"""
Deep dive into gpu2003 to understand why slot creation efficiency can exceed 100%
"""

import sqlite3
import pandas as pd
from datetime import timedelta

DB_PATH = "gpu_state_2026-01.db"
MACHINE = "gpu2003.chtc.wisc.edu"

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

# Separate primary and backfill slots
primary_df = df[~df["Name"].str.contains("backfill", case=False, na=False)].copy()
backfill_df = df[df["Name"].str.contains("backfill", case=False, na=False)].copy()

# Identify partitionable slots vs dynamic slots
partitionable_pattern = r'^slot\d+@'
primary_df["is_partitionable"] = primary_df["Name"].str.match(partitionable_pattern, case=False, na=False)

print(f"Analyzing {MACHINE} from {start_time} to {end_time}")
print(f"Total records: {len(df)}")
print(f"Unique timestamps: {df['timestamp'].nunique()}")
print("\n" + "="*140)

# Calculate per-timestamp statistics AND overall averages (matching gpu_type_summary logic)
total_primary_claimed = 0
total_primary_available = 0
num_primary_intervals = 0

total_backfill_available = 0
total_backfill_claimed = 0
num_backfill_intervals = 0

# For new efficiency calculation
total_unused_primary_when_possible = 0
total_backfill_when_possible = 0
num_intervals_with_unused = 0

timestamp_details = []

# Calculate primary statistics
for timestamp in sorted(primary_df["timestamp"].unique()):
    primary_snapshot = primary_df[primary_df["timestamp"] == timestamp]

    # Count claimed GPUs only from dynamic slots (not partitionable)
    dynamic_df = primary_snapshot[~primary_snapshot["is_partitionable"]]
    primary_claimed = len(dynamic_df[dynamic_df["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())

    # Count total GPUs from all slots
    primary_available = len(primary_snapshot["AssignedGPUs"].dropna().unique())

    # Accumulate for averaging
    total_primary_claimed += primary_claimed
    total_primary_available += primary_available
    num_primary_intervals += 1

# Calculate backfill statistics (matching gpu_type_summary: only count timestamps where backfill exists)
for timestamp in sorted(backfill_df["timestamp"].unique()):
    backfill_snapshot = backfill_df[backfill_df["timestamp"] == timestamp]

    backfill_claimed = len(backfill_snapshot[backfill_snapshot["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())
    backfill_available = len(backfill_snapshot["AssignedGPUs"].dropna().unique())

    total_backfill_claimed += backfill_claimed
    total_backfill_available += backfill_available
    num_backfill_intervals += 1

# Now collect per-timestamp details for all timestamps
for timestamp in sorted(df["timestamp"].unique()):
    primary_snapshot = primary_df[primary_df["timestamp"] == timestamp]
    backfill_snapshot = backfill_df[backfill_df["timestamp"] == timestamp]

    # Count claimed GPUs only from dynamic slots (not partitionable)
    dynamic_df = primary_snapshot[~primary_snapshot["is_partitionable"]]
    primary_claimed = len(dynamic_df[dynamic_df["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())

    # Count total GPUs from all slots
    primary_available = len(primary_snapshot["AssignedGPUs"].dropna().unique())

    # Count backfill available
    backfill_available = len(backfill_snapshot["AssignedGPUs"].dropna().unique())

    # Per-timestamp efficiency
    unused_primary_gpus = primary_available - primary_claimed
    timestamp_efficiency = (backfill_available / unused_primary_gpus * 100) if unused_primary_gpus > 0 else 0

    # For new efficiency calculation: only count when unused > 0
    if unused_primary_gpus > 0:
        total_unused_primary_when_possible += unused_primary_gpus
        total_backfill_when_possible += backfill_available
        num_intervals_with_unused += 1

    timestamp_details.append({
        "timestamp": timestamp,
        "primary_available": primary_available,
        "primary_claimed": primary_claimed,
        "unused_primary": unused_primary_gpus,
        "backfill_available": backfill_available,
        "timestamp_efficiency": timestamp_efficiency
    })

# Calculate overall averages (gpu_type_summary method)
avg_primary_claimed = total_primary_claimed / num_primary_intervals if num_primary_intervals > 0 else 0
avg_primary_available = total_primary_available / num_primary_intervals if num_primary_intervals > 0 else 0
avg_backfill_claimed = total_backfill_claimed / num_backfill_intervals if num_backfill_intervals > 0 else 0
avg_backfill_available = total_backfill_available / num_backfill_intervals if num_backfill_intervals > 0 else 0

# OLD calculation (flawed):
avg_unused_primary = avg_primary_available - avg_primary_claimed
old_efficiency = (avg_backfill_available / avg_unused_primary * 100) if avg_unused_primary > 0 else 0

# NEW calculation (only over timestamps where backfill could exist):
avg_unused_when_possible = total_unused_primary_when_possible / num_intervals_with_unused if num_intervals_with_unused > 0 else 0
avg_backfill_when_possible = total_backfill_when_possible / num_intervals_with_unused if num_intervals_with_unused > 0 else 0
overall_efficiency = (avg_backfill_when_possible / avg_unused_when_possible * 100) if avg_unused_when_possible > 0 else 0

print("\nOVERALL STATISTICS:")
print("="*140)
print(f"Number of primary intervals:  {num_primary_intervals}")
print(f"Number of backfill intervals: {num_backfill_intervals}")
print(f"Number of intervals with unused GPUs: {num_intervals_with_unused}")
print(f"Average Primary Available:  {avg_primary_available:.2f}")
print(f"Average Primary Claimed:    {avg_primary_claimed:.2f}")
print(f"\nOLD CALCULATION (flawed):")
print(f"  Average Unused Primary (all timestamps):     {avg_unused_primary:.2f}")
print(f"  Average Backfill Available (only when exists): {avg_backfill_available:.2f}")
print(f"  OLD EFFICIENCY:   {old_efficiency:.1f}%  <-- WRONG!")
print(f"\nNEW CALCULATION (fixed):")
print(f"  Average Unused Primary (when > 0):    {avg_unused_when_possible:.2f}")
print(f"  Average Backfill Available (when > 0): {avg_backfill_when_possible:.2f}")
print(f"  NEW EFFICIENCY:   {overall_efficiency:.1f}%  <-- CORRECT!")

print("\n" + "="*140)
print("\nEXPLANATION OF THE BUG:")
print("-" * 140)
print("The OLD calculation averaged backfill over 85 timestamps (where backfill exists)")
print("but averaged unused_primary over ALL 288 timestamps (including 203 with no unused GPUs).")
print("\nThis created an artificial >100% efficiency because the denominators were different.")
print("\nThe NEW calculation averages BOTH metrics over the same 85 timestamps where unused > 0,")
print("giving the correct 100% efficiency.")

# Show some example timestamps
print("\n" + "="*140)
print("\nSample of timestamps showing the pattern:")
print("-"*140)

details_df = pd.DataFrame(timestamp_details)

# Show timestamps where backfill > unused primary (which contribute to >100% average)
problematic = details_df[details_df["backfill_available"] > details_df["unused_primary"]]
print(f"\nTimestamps where backfill_available > unused_primary: {len(problematic)}/{len(details_df)}")

if len(problematic) > 0:
    print("\nFirst 10 examples:")
    for idx, row in problematic.head(10).iterrows():
        print(f"{row['timestamp']} | Primary Avail: {row['primary_available']:.0f} | "
              f"Claimed: {row['primary_claimed']:.0f} | Unused: {row['unused_primary']:.0f} | "
              f"Backfill: {row['backfill_available']:.0f} | Ratio: {row['backfill_available']/row['unused_primary']*100 if row['unused_primary'] > 0 else 0:.1f}%")

# Show distribution of backfill counts
print("\n" + "="*140)
print("\nDistribution of backfill_available counts:")
print(details_df["backfill_available"].value_counts().sort_index())

print("\n" + "="*140)
print("\nDistribution of unused_primary counts:")
print(details_df["unused_primary"].value_counts().sort_index())
