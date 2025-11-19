#!/usr/bin/env python3
import usage_stats
from usage_stats import filter_df, get_time_filtered_data, load_host_exclusions

usage_stats.HOST_EXCLUSIONS = load_host_exclusions(None, "masked_hosts.yaml")
usage_stats.FILTERED_HOSTS_INFO = []

df = get_time_filtered_data("gpu_state_2025-07.db", 24, None)
df["15min_bucket"] = df["timestamp"].dt.floor("15min")

# Check the specific GPU: GPU-d5703dd5 on dmorgan2000
bucket = df["15min_bucket"].iloc[0]
bucket_df = df[df["15min_bucket"] == bucket]

# Find all records for this GPU
gpu_records = bucket_df[bucket_df["AssignedGPUs"] == "GPU-d5703dd5"]

print("All records for GPU-d5703dd5:")
for _, record in gpu_records.iterrows():
    print(f'  Slot: {record["Name"]}')
    print(f'  State: {record["State"]}')
    print(f'  PrioritizedProjects: "{record["PrioritizedProjects"]}"')
    print(f'  Machine: {record["Machine"]}')
    print()

# Test the Priority filter on this GPU's records
priority_df = filter_df(gpu_records, "Priority", "", "")
print(f"Priority filter results: {len(priority_df)} records")

# Test individual components of the Priority filter
has_priority_project = gpu_records["PrioritizedProjects"] != ""
not_backfill_slot = ~gpu_records["Name"].str.contains("backfill")

print(f'Records with PrioritizedProjects != "": {has_priority_project.sum()}')
print(f'Records NOT containing "backfill" in name: {not_backfill_slot.sum()}')
print(f"Records meeting both conditions: {(has_priority_project & not_backfill_slot).sum()}")

# Also check the Backfill filter
backfill_df = filter_df(gpu_records, "Backfill", "", "")
print(f"Backfill filter results: {len(backfill_df)} records")

# Test backfill filter conditions
contains_backfill = gpu_records["Name"].str.contains("backfill")
print(f'Records containing "backfill" in name: {contains_backfill.sum()}')
