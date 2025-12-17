#!/usr/bin/env python3
from collections import defaultdict

import gpu_utils
from gpu_utils import filter_df, load_host_exclusions
from usage_stats import get_time_filtered_data

# Set up exactly like usage_stats.py does
gpu_utils.HOST_EXCLUSIONS = load_host_exclusions(None, "masked_hosts.yaml")
gpu_utils.FILTERED_HOSTS_INFO = []

df = get_time_filtered_data("gpu_state_2025-07.db", 24, None)
df["15min_bucket"] = df["timestamp"].dt.floor("15min")

print("Investigating backfill-only GPUs:")
print("=" * 60)

# Check multiple buckets to get a comprehensive view
sample_buckets = sorted(df["15min_bucket"].unique())[:10]  # First 10 buckets

all_backfill_only = set()
backfill_only_by_machine = defaultdict(set)
backfill_only_details = []

for bucket in sample_buckets:
    bucket_df = df[df["15min_bucket"] == bucket]

    if bucket_df.empty:
        continue

    # Get GPUs from each category
    priority_gpus = set(filter_df(bucket_df, "Priority", "", "")["AssignedGPUs"].dropna().unique())
    shared_gpus = set(filter_df(bucket_df, "Shared", "", "")["AssignedGPUs"].dropna().unique())
    backfill_gpus = set(filter_df(bucket_df, "Backfill", "", "")["AssignedGPUs"].dropna().unique())

    # Find backfill-only GPUs
    priority_and_shared = priority_gpus.union(shared_gpus)
    backfill_only = backfill_gpus - priority_and_shared

    if backfill_only:
        print(f"\n{bucket} - {len(backfill_only)} backfill-only GPUs:")

        # Get details for these backfill-only GPUs
        backfill_df = filter_df(bucket_df, "Backfill", "", "")
        backfill_only_df = backfill_df[backfill_df["AssignedGPUs"].isin(backfill_only)]

        # Group by machine
        for machine in backfill_only_df["Machine"].unique():
            machine_df = backfill_only_df[backfill_only_df["Machine"] == machine]
            machine_gpus = set(machine_df["AssignedGPUs"].dropna().unique())

            print(f"  {machine}: {len(machine_gpus)} GPUs")

            # Store for overall tracking
            all_backfill_only.update(machine_gpus)
            backfill_only_by_machine[machine].update(machine_gpus)

            # Show details for a few GPUs
            for gpu_id in sorted(machine_gpus)[:3]:
                gpu_records = machine_df[machine_df["AssignedGPUs"] == gpu_id]
                if len(gpu_records) > 0:
                    sample_record = gpu_records.iloc[0]
                    device = sample_record["GPUs_DeviceName"] or "Unknown"
                    priority_proj = sample_record["PrioritizedProjects"] or "None"
                    state = sample_record["State"]
                    slot_name = sample_record["Name"]

                    print(f'    {gpu_id}: {device}, Priority="{priority_proj}", State={state}, Slot={slot_name}')

print("\n" + "=" * 60)
print("SUMMARY - Machines with backfill-only GPUs:")
print("=" * 60)

for machine, gpus in sorted(backfill_only_by_machine.items()):
    print(f"\n{machine}: {len(gpus)} unique backfill-only GPUs")

    # Get details for all GPUs on this machine
    machine_records = df[df["Machine"] == machine]

    # Show a sample of GPU details
    sample_gpus = sorted(gpus)[:5]  # First 5 GPUs
    for gpu_id in sample_gpus:
        gpu_records = machine_records[machine_records["AssignedGPUs"] == gpu_id]
        if len(gpu_records) > 0:
            sample_record = gpu_records.iloc[0]
            device = sample_record["GPUs_DeviceName"] or "Unknown"
            priority_proj = sample_record["PrioritizedProjects"] or "None"

            print(f'  {gpu_id}: {device}, Priority="{priority_proj}"')

    if len(gpus) > 5:
        print(f"  ... and {len(gpus) - 5} more GPUs")

print(f"\nTotal unique backfill-only GPUs found: {len(all_backfill_only)}")
print(f"Machines with backfill-only GPUs: {len(backfill_only_by_machine)}")
