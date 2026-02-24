#!/usr/bin/env python3
"""
Analyze gpu2003 slot creation efficiency to find timestamps where it exceeds 100%
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
print("\n" + "="*120)

# Analyze each timestamp
results = []

for timestamp in sorted(df["timestamp"].unique()):
    # Get primary data at this timestamp
    primary_snapshot = primary_df[primary_df["timestamp"] == timestamp]
    backfill_snapshot = backfill_df[backfill_df["timestamp"] == timestamp]

    # Count claimed GPUs only from dynamic slots (not partitionable)
    dynamic_df = primary_snapshot[~primary_snapshot["is_partitionable"]]
    primary_claimed = len(dynamic_df[dynamic_df["State"] == "Claimed"]["AssignedGPUs"].dropna().unique())

    # Count total GPUs from all slots
    primary_available = len(primary_snapshot["AssignedGPUs"].dropna().unique())

    # Count backfill available
    backfill_available = len(backfill_snapshot["AssignedGPUs"].dropna().unique())

    # Calculate slot creation efficiency
    unused_primary_gpus = primary_available - primary_claimed
    slot_creation_efficiency = (backfill_available / unused_primary_gpus * 100) if unused_primary_gpus > 0 else 0

    results.append({
        "timestamp": timestamp,
        "primary_available": primary_available,
        "primary_claimed": primary_claimed,
        "unused_primary": unused_primary_gpus,
        "backfill_available": backfill_available,
        "efficiency_pct": slot_creation_efficiency
    })

# Convert to DataFrame for analysis
results_df = pd.DataFrame(results)

if len(results_df) == 0:
    print(f"\nNo data found for {MACHINE}")
    exit(1)

# Find timestamps where efficiency > 100%
over_100 = results_df[results_df["efficiency_pct"] > 100].sort_values("efficiency_pct", ascending=False)

print(f"\nTimestamps where slot creation efficiency > 100%:")
print("="*120)

if len(over_100) > 0:
    print(f"\nFound {len(over_100)} timestamps with >100% efficiency:\n")
    for idx, row in over_100.iterrows():
        print(f"Timestamp: {row['timestamp']}")
        print(f"  Primary Available: {row['primary_available']:.0f}")
        print(f"  Primary Claimed:   {row['primary_claimed']:.0f}")
        print(f"  Unused Primary:    {row['unused_primary']:.0f}")
        print(f"  Backfill Available: {row['backfill_available']:.0f}")
        print(f"  Efficiency:        {row['efficiency_pct']:.1f}%")
        print()
else:
    print("\nNo timestamps found with efficiency > 100%")

# Show summary statistics
print("\n" + "="*120)
print("\nSummary Statistics:")
print(f"  Min Efficiency:  {results_df['efficiency_pct'].min():.1f}%")
print(f"  Max Efficiency:  {results_df['efficiency_pct'].max():.1f}%")
print(f"  Mean Efficiency: {results_df['efficiency_pct'].mean():.1f}%")
print(f"  Median Efficiency: {results_df['efficiency_pct'].median():.1f}%")

# Show top 10 highest efficiency timestamps
print("\n" + "="*120)
print("\nTop 10 Highest Efficiency Timestamps:")
print("-"*120)
top10 = results_df.nlargest(10, "efficiency_pct")
for idx, row in top10.iterrows():
    print(f"{row['timestamp']} | Eff: {row['efficiency_pct']:6.1f}% | "
          f"Primary: {row['primary_available']:.0f} | Claimed: {row['primary_claimed']:.0f} | "
          f"Unused: {row['unused_primary']:.0f} | Backfill: {row['backfill_available']:.0f}")
