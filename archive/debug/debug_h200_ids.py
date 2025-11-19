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

print('H200 GPU IDs during anomalous time slices:')
print('=' * 60)

# Known GPU IDs from each machine
gpu4004_h200s = {'GPU-0c28c2e6', 'GPU-18a8cb87', 'GPU-48e4e4f6', 'GPU-65404a9b', 
                 'GPU-7990e801', 'GPU-843cdddd', 'GPU-9ba15165', 'GPU-9e8c2c5b'}
ssilwal_h200s = {'GPU-0f265617', 'GPU-5cdb85f0', 'GPU-9b91e22c', 'GPU-b5971c9a'}

# Check time slices with > 8 shared H200s
anomalous_buckets = [
    '2025-07-27 15:15:00',
    '2025-07-27 15:45:00', 
    '2025-07-28 04:15:00',
    '2025-07-28 07:30:00',
    '2025-07-28 08:30:00',
    '2025-07-28 09:00:00',
    '2025-07-28 09:30:00',
    '2025-07-28 09:45:00',
    '2025-07-28 10:00:00',
    '2025-07-28 12:00:00',
    '2025-07-28 12:15:00',
    '2025-07-28 12:30:00'
]

for bucket_str in anomalous_buckets:
    bucket = pd.to_datetime(bucket_str)
    bucket_df = df[df['15min_bucket'] == bucket]
    
    # Filter by device type (H200)
    device_df = bucket_df[bucket_df['GPUs_DeviceName'] == 'NVIDIA H200']
    
    if device_df.empty:
        continue
    
    # Get shared H200s
    shared_df = filter_df(device_df, 'Shared', '', '')
    shared_gpu_ids = set(shared_df['AssignedGPUs'].dropna().unique())
    
    print(f'\n{bucket_str} - {len(shared_gpu_ids)} Shared H200 GPUs:')
    
    # Check which machine each GPU belongs to
    gpu4004_in_shared = shared_gpu_ids.intersection(gpu4004_h200s)
    ssilwal_in_shared = shared_gpu_ids.intersection(ssilwal_h200s)
    unknown_in_shared = shared_gpu_ids - gpu4004_h200s - ssilwal_h200s
    
    print(f'  From gpu4004 (normally shared): {len(gpu4004_in_shared)} GPUs')
    if gpu4004_in_shared:
        print(f'    {sorted(gpu4004_in_shared)}')
    
    print(f'  From ssilwalgpu4000 (normally priority): {len(ssilwal_in_shared)} GPUs')
    if ssilwal_in_shared:
        print(f'    {sorted(ssilwal_in_shared)}')
        # Check their priority projects in this bucket
        ssilwal_shared_data = shared_df[shared_df['AssignedGPUs'].isin(ssilwal_in_shared)]
        for _, row in ssilwal_shared_data.iterrows():
            print(f'      {row["AssignedGPUs"]}: PriorityProjects="{row["PrioritizedProjects"]}"')
    
    if unknown_in_shared:
        print(f'  Unknown GPUs: {len(unknown_in_shared)} GPUs')
        print(f'    {sorted(unknown_in_shared)}')

# Also check a normal time slice for comparison
print(f'\n' + '=' * 60)
print('COMPARISON - Normal time slice with exactly 8 shared H200s:')

normal_bucket = pd.to_datetime('2025-07-28 11:15:00')  # Should be normal
bucket_df = df[df['15min_bucket'] == normal_bucket]
device_df = bucket_df[bucket_df['GPUs_DeviceName'] == 'NVIDIA H200']
shared_df = filter_df(device_df, 'Shared', '', '')
shared_gpu_ids = set(shared_df['AssignedGPUs'].dropna().unique())

gpu4004_in_shared = shared_gpu_ids.intersection(gpu4004_h200s)
ssilwal_in_shared = shared_gpu_ids.intersection(ssilwal_h200s)

print(f'\n2025-07-28 11:15:00 - {len(shared_gpu_ids)} Shared H200 GPUs:')
print(f'  From gpu4004 (normally shared): {len(gpu4004_in_shared)} GPUs')
print(f'  From ssilwalgpu4000 (normally priority): {len(ssilwal_in_shared)} GPUs')