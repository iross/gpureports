#!/usr/bin/env python3
"""
Check which machines appear in Backfill (Open Capacity) over the last 24 hours
"""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from gpu_utils import filter_df_enhanced, get_most_recent_database, load_chtc_owned_hosts

# Load CHTC owned hosts for reference
chtc_owned = load_chtc_owned_hosts()

# Get the most recent database
db_file = get_most_recent_database()
if not db_file:
    print("No database file found!")
    exit(1)

print(f"Using database: {db_file}")

# Connect and query
conn = sqlite3.connect(db_file)

# Get the latest timestamp
latest_query = "SELECT MAX(timestamp) as max_time FROM gpu_state"
df_time = pd.read_sql_query(latest_query, conn)
latest_time = pd.to_datetime(df_time["max_time"].iloc[0])

print(f"Latest timestamp in database: {latest_time}")

# Calculate 24 hours ago
time_24h_ago = latest_time - timedelta(hours=24)
print(f"Querying from: {time_24h_ago}")

# Query for last 24 hours
query = f"""
SELECT * FROM gpu_state
WHERE timestamp >= '{time_24h_ago.strftime('%Y-%m-%d %H:%M:%S')}'
"""

df = pd.read_sql_query(query, conn)
conn.close()

df["timestamp"] = pd.to_datetime(df["timestamp"])

print(f"\nTotal records in last 24 hours: {len(df)}")
print(f"Columns in dataframe: {df.columns.tolist()}")

# Filter for Backfill (Open Capacity)
backfill_open_df = filter_df_enhanced(df, "Backfill-OpenCapacity", "", "")

if backfill_open_df.empty:
    print("\nNo Backfill (Open Capacity) slots found in the last 24 hours!")
else:
    # Get unique machines
    unique_machines = backfill_open_df["Machine"].unique()

    print(f"\n{'='*80}")
    print(f"Machines in Backfill (Open Capacity) over last 24 hours:")
    print(f"{'='*80}")
    print(f"\nTotal unique machines: {len(unique_machines)}\n")

    for machine in sorted(unique_machines):
        # Get info about this machine
        machine_df = backfill_open_df[backfill_open_df["Machine"] == machine]

        # Get GPU model info (take most common)
        gpu_model = "Unknown"
        if "GPUs_DeviceName" in machine_df.columns:
            gpu_models = machine_df["GPUs_DeviceName"].value_counts()
            gpu_model = gpu_models.index[0] if len(gpu_models) > 0 else "Unknown"

        # Get unique AssignedGPUs count (number of GPUs on this machine seen in backfill)
        num_gpus = machine_df["AssignedGPUs"].nunique()

        # Check PrioritizedProjects
        prioritized = machine_df["PrioritizedProjects"].iloc[0] if len(machine_df) > 0 else ""

        print(f"  {machine}")
        print(f"    GPU Model: {gpu_model}")
        print(f"    Backfill GPUs seen: {num_gpus}")
        print(f"    PrioritizedProjects: '{prioritized}'")
        print(f"    In CHTC owned list: {machine in chtc_owned}")
        print()

    # Summary statistics
    print(f"{'='*80}")
    print("Summary Statistics:")
    print(f"{'='*80}")

    # Count total backfill slots
    total_slots = len(backfill_open_df)
    claimed_slots = len(backfill_open_df[backfill_open_df["State"] == "Claimed"])
    unclaimed_slots = len(backfill_open_df[backfill_open_df["State"] == "Unclaimed"])

    print(f"Total backfill slot observations: {total_slots}")
    print(f"  Claimed: {claimed_slots}")
    print(f"  Unclaimed: {unclaimed_slots}")
    print(f"Unique machines: {len(unique_machines)}")

    # Analyze who is using these machines when claimed
    print(f"\n{'='*80}")
    print("Users Running on Backfill (Open Capacity) - Last 24 Hours:")
    print(f"{'='*80}\n")

    claimed_df = backfill_open_df[backfill_open_df["State"] == "Claimed"]

    if not claimed_df.empty:
        # Get user usage statistics
        user_counts = claimed_df["RemoteOwner"].value_counts()

        print(f"Total claimed slot observations: {len(claimed_df)}")
        print(f"Unique users: {len(user_counts)}\n")

        print("User breakdown (by number of claimed slot observations):")
        for user, count in user_counts.items():
            percentage = (count / len(claimed_df)) * 100
            print(f"  {user}: {count} observations ({percentage:.1f}%)")

        # Show which machines each user ran on
        print(f"\n{'='*80}")
        print("Machines Used by Each User:")
        print(f"{'='*80}\n")

        for user in user_counts.index:
            user_df = claimed_df[claimed_df["RemoteOwner"] == user]
            machines_used = user_df["Machine"].unique()
            print(f"{user}:")
            for machine in sorted(machines_used):
                machine_count = len(user_df[user_df["Machine"] == machine])
                print(f"  - {machine} ({machine_count} observations)")
            print()
    else:
        print("No claimed slots found in the dataset.")
