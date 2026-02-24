#!/usr/bin/env python3
"""
Investigate hsharma-chtcgpu4000 to understand >100% efficiency
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
print(f"PrioritizedProjects values: {df['PrioritizedProjects'].unique()}")
print(f"\nUnique GPUs: {sorted(df['AssignedGPUs'].dropna().unique())}")

# Separate primary and backfill slots
primary_df = df[~df["Name"].str.contains("backfill", case=False, na=False)].copy()
backfill_df = df[df["Name"].str.contains("backfill", case=False, na=False)].copy()

print(f"\nPrimary slot records: {len(primary_df)}")
print(f"Backfill slot records: {len(backfill_df)}")

# Identify partitionable slots
partitionable_pattern = r'^slot\d+@'
primary_df["is_partitionable"] = primary_df["Name"].str.match(partitionable_pattern, case=False, na=False)

print(f"\nUnique slot names (primary): {sorted(primary_df['Name'].unique())}")
print(f"Unique slot names (backfill): {sorted(backfill_df['Name'].unique())}")

# Check a few timestamps to see the pattern
print("\n" + "="*140)
print("Sample timestamps showing >100% efficiency pattern:")
print("="*140)

for timestamp in sorted(df["timestamp"].unique())[:10]:
    primary_snapshot = primary_df[primary_df["timestamp"] == timestamp]
    backfill_snapshot = backfill_df[backfill_df["timestamp"] == timestamp]

    # Count claimed GPUs only from dynamic slots (not partitionable)
    dynamic_df = primary_snapshot[~primary_snapshot["is_partitionable"]]
    primary_claimed = len(dynamic_df[dynamic_df["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())

    # Count total GPUs from all slots
    primary_available = len(primary_snapshot["AssignedGPUs"].dropna().unique())

    # Count backfill
    backfill_available = len(backfill_snapshot["AssignedGPUs"].dropna().unique())

    unused = primary_available - primary_claimed
    eff = (backfill_available / unused * 100) if unused > 0 else 0

    print(f"\n{timestamp}")
    print(f"  Primary slots: {len(primary_snapshot)} records")
    print(f"  Primary GPUs available: {primary_available}")
    print(f"  Primary GPUs claimed: {primary_claimed}")
    print(f"  Unused primary GPUs: {unused}")
    print(f"  Backfill slots: {len(backfill_snapshot)} records")
    print(f"  Backfill GPUs available: {backfill_available}")
    print(f"  Efficiency: {eff:.1f}%")

    if eff > 100:
        print(f"  >>> EFFICIENCY > 100%! <<<")
        print(f"  Primary slots detail:")
        for _, row in primary_snapshot.iterrows():
            print(f"    {row['Name']}: {row['State']}, GPU: {row['AssignedGPUs']}, Partitionable: {row['is_partitionable']}")
        print(f"  Backfill slots detail:")
        for _, row in backfill_snapshot.iterrows():
            print(f"    {row['Name']}: {row['State']}, GPU: {row['AssignedGPUs']}")
