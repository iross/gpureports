#!/usr/bin/env python3
import pandas as pd
import sqlite3
from usage_stats import filter_df, get_time_filtered_data, load_host_exclusions
import usage_stats

# Set up exactly like usage_stats.py does
usage_stats.HOST_EXCLUSIONS = load_host_exclusions(None, 'masked_hosts.yaml')
usage_stats.FILTERED_HOSTS_INFO = []

df = get_time_filtered_data('gpu_state_2025-07.db', 24, None)
df['15min_bucket'] = df['timestamp'].dt.floor('15min')

print('Detailed analysis of 15-GPU anomaly:')
print('=' * 60)

# Check the bucket that showed 15 GPUs
bucket = pd.to_datetime('2025-07-27 15:15:00')
bucket_df = df[df['15min_bucket'] == bucket]

# Filter by device type (H200)
device_df = bucket_df[bucket_df['GPUs_DeviceName'] == 'NVIDIA H200']

print(f'H200 records in bucket {bucket}:')
print(f'Total H200 records: {len(device_df)}')
print(f'Unique H200 GPUs: {device_df["AssignedGPUs"].nunique()}')

# Apply the filter for Shared and see what we get
shared_claimed_df = filter_df(device_df, 'Shared', 'Claimed', '')
shared_unclaimed_df = filter_df(device_df, 'Shared', 'Unclaimed', '')

print(f'\nShared Claimed H200s:')
print(f'  Records: {len(shared_claimed_df)}')
print(f'  Unique GPUs: {shared_claimed_df["AssignedGPUs"].dropna().nunique()}')
if len(shared_claimed_df) > 0:
    print(f'  GPU IDs: {sorted(shared_claimed_df["AssignedGPUs"].dropna().unique())}')

print(f'\nShared Unclaimed H200s:')
print(f'  Records: {len(shared_unclaimed_df)}')
print(f'  Unique GPUs: {shared_unclaimed_df["AssignedGPUs"].dropna().nunique()}')
if len(shared_unclaimed_df) > 0:
    print(f'  GPU IDs: {sorted(shared_unclaimed_df["AssignedGPUs"].dropna().unique())}')

# The issue might be that we're counting records vs unique GPUs
claimed_gpus = len(shared_claimed_df['AssignedGPUs'].dropna().unique())
unclaimed_gpus = len(shared_unclaimed_df['AssignedGPUs'].dropna().unique())

print(f'\nCalculation used by usage_stats.py:')
print(f'  Claimed unique GPUs: {claimed_gpus}')
print(f'  Unclaimed unique GPUs: {unclaimed_gpus}')
print(f'  Total: {claimed_gpus + unclaimed_gpus}')

# Check if there are duplicate GPU IDs being counted
all_shared_df = filter_df(device_df, 'Shared', '', '')
print(f'\nAll Shared H200 records: {len(all_shared_df)}')
print(f'Unique GPUs in all shared records: {all_shared_df["AssignedGPUs"].dropna().nunique()}')

# Check for the mystery 7 extra GPUs - maybe they're being double-counted somewhere
print(f'\nBreakdown by machine and state:')
for machine in all_shared_df['Machine'].unique():
    machine_df = all_shared_df[all_shared_df['Machine'] == machine]
    for state in machine_df['State'].unique():
        state_df = machine_df[machine_df['State'] == state]
        unique_gpus = state_df['AssignedGPUs'].dropna().nunique()
        print(f'  {machine} - {state}: {len(state_df)} records, {unique_gpus} unique GPUs')
        if unique_gpus > 0:
            print(f'    GPU IDs: {sorted(state_df["AssignedGPUs"].dropna().unique())}')