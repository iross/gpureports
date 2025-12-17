#!/usr/bin/env python3

import pandas as pd

import usage_stats
from usage_stats import filter_df, get_time_filtered_data, load_host_exclusions

# Set up exactly like usage_stats.py does
usage_stats.HOST_EXCLUSIONS = load_host_exclusions(None, "masked_hosts.yaml")
usage_stats.FILTERED_HOSTS_INFO = []

df = get_time_filtered_data("gpu_state_2025-07.db", 24, None)
df["15min_bucket"] = df["timestamp"].dt.floor("15min")

print("Debugging H200 Priority classification:")
print("=" * 60)

# Check a sample bucket to see what gets classified as Priority
bucket = pd.to_datetime("2025-07-28 10:00:00")  # Use our troubleshoot time
bucket_df = df[df["15min_bucket"] == bucket]

# Filter by device type (H200)
device_df = bucket_df[bucket_df["GPUs_DeviceName"] == "NVIDIA H200"]

print(f"H200 records in bucket {bucket}:")
print(f"Total H200 records: {len(device_df)}")

# Apply the Priority filter and see what we get
priority_df = filter_df(device_df, "Priority", "", "")

print(f"\nPriority H200 records: {len(priority_df)}")
print(f'Unique Priority H200 GPUs: {priority_df["AssignedGPUs"].dropna().nunique()}')

# Break down by machine
print("\nBreakdown by machine:")
for machine in priority_df["Machine"].unique():
    machine_df = priority_df[priority_df["Machine"] == machine]
    unique_gpus = machine_df["AssignedGPUs"].dropna().nunique()
    print(f"  {machine}: {len(machine_df)} records, {unique_gpus} unique GPUs")

    # Show the priority projects
    priority_projects = machine_df["PrioritizedProjects"].unique()
    print(f"    Priority Projects: {priority_projects}")

    # Show some sample records
    print(f'    Sample GPU IDs: {sorted(machine_df["AssignedGPUs"].dropna().unique())[:3]}')

    # Check states
    states = machine_df["State"].value_counts()
    print(f"    States: {dict(states)}")
    print()

# Check if gpu4004 GPUs are somehow getting priority projects assigned
gpu4004_h200s = device_df[device_df["Machine"] == "gpu4004.chtc.wisc.edu"]
print(f"gpu4004 H200 records in this bucket: {len(gpu4004_h200s)}")
print("Unique PrioritizedProjects values for gpu4004 H200s:")
priority_values = gpu4004_h200s["PrioritizedProjects"].unique()
for val in priority_values:
    count = len(gpu4004_h200s[gpu4004_h200s["PrioritizedProjects"] == val])
    print(f'  "{val}": {count} records')
