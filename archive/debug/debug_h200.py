#!/usr/bin/env python3
import usage_stats
from usage_stats import filter_df, get_time_filtered_data, load_host_exclusions

# Set up exactly like usage_stats.py does
usage_stats.HOST_EXCLUSIONS = load_host_exclusions(None, "masked_hosts.yaml")
usage_stats.FILTERED_HOSTS_INFO = []

df = get_time_filtered_data("gpu_state_2025-07.db", 24, None)
df["15min_bucket"] = df["timestamp"].dt.floor("15min")

print("Debugging H200 Shared calculation:")
print("=" * 50)

total_claimed_gpus = 0
total_available_gpus = 0
intervals_with_h200_data = 0
intervals_with_any_gpus = 0

for bucket in sorted(df["15min_bucket"].unique()):
    bucket_df = df[df["15min_bucket"] == bucket]
    intervals_with_any_gpus += 1

    # Filter by device type (H200)
    device_df = bucket_df[bucket_df["GPUs_DeviceName"] == "NVIDIA H200"]

    if device_df.empty:
        print(f"{bucket}: NO H200 data - skipped")
        continue

    intervals_with_h200_data += 1

    # Count unique GPUs for Shared
    claimed_gpus = len(filter_df(device_df, "Shared", "Claimed", "")["AssignedGPUs"].dropna().unique())
    unclaimed_gpus = len(filter_df(device_df, "Shared", "Unclaimed", "")["AssignedGPUs"].dropna().unique())

    total_gpus_this_interval = claimed_gpus + unclaimed_gpus
    total_claimed_gpus += claimed_gpus
    total_available_gpus += total_gpus_this_interval

    if total_gpus_this_interval != 8:
        print(
            f"{bucket}: {total_gpus_this_interval} total H200 shared GPUs (claimed: {claimed_gpus}, unclaimed: {unclaimed_gpus})"
        )

print("\nSummary:")
print(f"Total intervals: {intervals_with_any_gpus}")
print(f"Intervals with H200 data: {intervals_with_h200_data}")
print(f"Total claimed GPUs (sum): {total_claimed_gpus}")
print(f"Total available GPUs (sum): {total_available_gpus}")
print(f"Average claimed: {total_claimed_gpus / intervals_with_h200_data}")
print(f"Average available: {total_available_gpus / intervals_with_h200_data}")
