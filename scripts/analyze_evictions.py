#!/usr/bin/env python3
"""
Analyze GPU backfill eviction logs to identify machines and GPUs assigned to evicted jobs,
and match them with jobs that caused the evictions using GPU job data.

Common eviction codes found in logs:
- 1011: Unknown eviction reason
- 1013: Preempted by higher priority job
- 1009: System maintenance or node draining
"""

import os
import re
from pathlib import Path
import csv
from datetime import datetime, timedelta
import pandas as pd

def parse_log_file(file_path):
    """Parse a single log file and extract eviction information."""
    with open(file_path, 'r') as f:
        content = f.read()

    # Extract job ID from filename
    job_id = Path(file_path).stem.split('_')[0]

    # Find all eviction events in the log with timestamps
    eviction_matches = list(re.finditer(
        r'004 \(.*\) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) Job was evicted\. Code (\d+) Subcode \d+',
        content
    ))
    if not eviction_matches:
        return None  # Skip jobs that weren't evicted

    # Find all execution starts (001 events) with their context
    # Split content by lines to process each execution start individually
    lines = content.split('\n')
    execution_starts = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Look for job execution start
        exec_match = re.search(r'001 \(.*\) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) Job executing on host: .*alias=([^\s&]+)', line)
        if exec_match:
            start_time = exec_match.group(1)
            host = exec_match.group(2)
            
            # Look ahead for SlotName, AvailableGPUs, and DeviceName in the next ~20 lines
            slot_name = "Unknown"
            gpu_id = "Unknown" 
            gpu_type = "Unknown"
            
            for j in range(i+1, min(i+25, len(lines))):
                if 'SlotName:' in lines[j]:
                    slot_match = re.search(r'SlotName: ([^\s]+)', lines[j])
                    if slot_match:
                        slot_name = slot_match.group(1)
                
                if 'AvailableGPUs = { GPUs_GPU_' in lines[j]:
                    gpu_match = re.search(r'AvailableGPUs = { GPUs_GPU_(\w+) }', lines[j])
                    if gpu_match:
                        gpu_id = gpu_match.group(1)
                
                if f'GPUs_GPU_{gpu_id} = [' in lines[j] and 'DeviceName = "' in lines[j]:
                    device_match = re.search(r'DeviceName = "([^"]+)"', lines[j])
                    if device_match:
                        gpu_type = device_match.group(1)
                        break
            
            # Create a match-like object for compatibility
            class FakeMatch:
                def __init__(self, time, host, slot, gpu_id, gpu_type):
                    self._groups = (time, host, slot, gpu_id, gpu_type)
                def group(self, n):
                    return self._groups[n-1]
            
            execution_starts.append(FakeMatch(start_time, host, slot_name, gpu_id, gpu_type))
        
        i += 1

    # Find the final termination time if it exists
    termination_match = re.search(
        r'005 \(.*\) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) Job terminated',
        content
    )
    final_end_time = datetime.strptime(termination_match.group(1), '%Y-%m-%d %H:%M:%S') if termination_match else None

    results = []
    
    # For each eviction, find the corresponding execution context
    for eviction_match in eviction_matches:
        eviction_time = datetime.strptime(eviction_match.group(1), '%Y-%m-%d %H:%M:%S')
        eviction_code = eviction_match.group(2)
        
        # Find the most recent execution start before this eviction
        relevant_execution = None
        for exec_match in execution_starts:
            exec_start_time = datetime.strptime(exec_match.group(1), '%Y-%m-%d %H:%M:%S')
            if exec_start_time <= eviction_time:
                if relevant_execution is None or exec_start_time > datetime.strptime(relevant_execution.group(1), '%Y-%m-%d %H:%M:%S'):
                    relevant_execution = exec_match
        
        if relevant_execution:
            start_time = datetime.strptime(relevant_execution.group(1), '%Y-%m-%d %H:%M:%S')
            host = relevant_execution.group(2)
            slot_name = relevant_execution.group(3)
            gpu_id = relevant_execution.group(4)
            gpu_type = relevant_execution.group(5)
        else:
            # Fallback to first execution if we can't find a match
            if execution_starts:
                start_time = datetime.strptime(execution_starts[0].group(1), '%Y-%m-%d %H:%M:%S')
                host = execution_starts[0].group(2)
                slot_name = execution_starts[0].group(3)
                gpu_id = execution_starts[0].group(4)
                gpu_type = execution_starts[0].group(5)
            else:
                start_time = None
                host = "Unknown"
                slot_name = "Unknown"
                gpu_id = "Unknown"
                gpu_type = "Unknown"

        # Calculate runtime for this specific execution
        runtime = (eviction_time - start_time).total_seconds() if start_time else 0
        
        # Determine end time (eviction time for evicted runs, final termination for last successful run)
        end_time = eviction_time

        results.append({
            'job_id': job_id,
            'host': host,
            'slot_name': slot_name,
            'gpu_id': gpu_id,
            'gpu_type': gpu_type,
            'eviction_code': eviction_code,
            'eviction_time': eviction_time,
            'start_time': start_time,
            'end_time': end_time,
            'runtime_seconds': runtime,
            'log_file': str(file_path)
        })

    # Note: We don't include successful completions (eviction_code='0') in evictions report
    # since this is specifically for analyzing evictions, not normal job completions

    return results

def load_gpu_jobs(csv_file):
    """Load GPU jobs data from CSV file."""
    try:
        df = pd.read_csv(csv_file)
        # Convert timestamp columns to datetime
        if 'JobStartDate' in df.columns:
            df['JobStartDate'] = pd.to_datetime(df['JobStartDate'], unit='s', errors='coerce')
        if 'CompletionDate' in df.columns:
            df['CompletionDate'] = pd.to_datetime(df['CompletionDate'], unit='s', errors='coerce')
        if 'QDate' in df.columns:
            df['QDate'] = pd.to_datetime(df['QDate'], unit='s', errors='coerce')
        return df
    except Exception as e:
        print(f"Error loading GPU jobs CSV: {e}")
        return pd.DataFrame()

def extract_gpu_id_from_assigned(assigned_gpus_str):
    """Extract GPU ID from AssignedGPUs string format."""
    if pd.isna(assigned_gpus_str) or not assigned_gpus_str:
        return None
    
    # Handle different formats of GPU assignment
    # Format: "GPU-427daf99" or similar
    match = re.search(r'GPU-([a-f0-9]+)', str(assigned_gpus_str))
    if match:
        return match.group(1)
    return None

def find_evicting_jobs(evictions, gpu_jobs_df, time_window_minutes=30):
    """
    Find jobs that likely caused the evictions by matching:
    - Time: jobs that were queued (QDate) around the eviction time
    - Machine: same StartdName (host)
    - Slot: same StartdSlot
    - GPU: same or related GPU assignment
    """
    results = []
    
    for eviction in evictions:
        eviction_time = eviction['eviction_time']
        host = eviction['host']
        slot_name = eviction['slot_name']
        evicted_gpu_id = eviction['gpu_id']
        
        # Create time window around eviction
        time_start = eviction_time - timedelta(minutes=time_window_minutes)
        
        # Filter jobs by time window using QDate (queue date)
        if 'QDate' in gpu_jobs_df.columns:
            # QDate must be BEFORE eviction time (jobs can't cause evictions if queued after)
            time_mask = (
                (gpu_jobs_df['QDate'] >= time_start) & 
                (gpu_jobs_df['QDate'] <= eviction_time)
            )
            time_column = 'QDate'
        else:
            # Fall back to JobStartDate if QDate not available
            time_end = eviction_time + timedelta(minutes=time_window_minutes)
            time_mask = (
                (gpu_jobs_df['JobStartDate'] >= time_start) & 
                (gpu_jobs_df['JobStartDate'] <= time_end)
            )
            time_column = 'JobStartDate'
            
        candidate_jobs = gpu_jobs_df[time_mask].copy()
        
        if candidate_jobs.empty:
            # No jobs found in time window
            eviction_result = eviction.copy()
            eviction_result.update({
                'evicting_job_id': None,
                'evicting_job_start': None,
                'evicting_job_user': None,
                'evicting_job_project': None,
                'match_confidence': 'no_candidates',
                'match_reason': f'No jobs queued within {time_window_minutes} minutes of eviction'
            })
            results.append(eviction_result)
            continue
        
        # Try to match by host name
        host_matches = candidate_jobs[candidate_jobs['StartdName'].str.contains(host, case=False, na=False)]
        
        if host_matches.empty:
            # Try broader hostname matching (removing domain suffixes)
            host_base = host.split('.')[0]
            host_matches = candidate_jobs[candidate_jobs['StartdName'].str.contains(host_base, case=False, na=False)]
        
        # Try to match by slot if we have slot information
        if not host_matches.empty and slot_name != "Unknown":
            slot_matches = host_matches[host_matches['StartdSlot'].str.contains(slot_name.split('@')[0], case=False, na=False)]
            if not slot_matches.empty:
                host_matches = slot_matches
        
        # Try to match by GPU if we have GPU information
        best_matches = host_matches.copy()
        if not best_matches.empty and evicted_gpu_id != "Unknown":
            # Extract GPU IDs from AssignedGPUs column
            best_matches['extracted_gpu_id'] = best_matches['AssignedGPUs'].apply(extract_gpu_id_from_assigned)
            gpu_matches = best_matches[best_matches['extracted_gpu_id'] == evicted_gpu_id]
            if not gpu_matches.empty:
                best_matches = gpu_matches
        
        # Select the best match (closest in time to eviction)
        if not best_matches.empty:
            best_matches['time_diff'] = abs((best_matches[time_column] - eviction_time).dt.total_seconds())
            best_match = best_matches.loc[best_matches['time_diff'].idxmin()]
            
            # Determine match confidence
            confidence = 'high'
            match_reasons = []
            
            if evicted_gpu_id != "Unknown" and best_match.get('extracted_gpu_id') == evicted_gpu_id:
                match_reasons.append('exact_gpu_match')
            elif evicted_gpu_id != "Unknown":
                confidence = 'medium'
                match_reasons.append('host_time_match')
            else:
                confidence = 'low'
                match_reasons.append('host_time_only')
            
            if slot_name != "Unknown" and slot_name.split('@')[0] in str(best_match.get('StartdSlot', '')):
                match_reasons.append('slot_match')
            
            time_diff_minutes = best_match['time_diff'] / 60
            if time_diff_minutes <= 5:
                match_reasons.append('close_time_match')
            elif time_diff_minutes > 15:
                confidence = 'low'
            
            eviction_result = eviction.copy()
            eviction_result.update({
                'evicting_job_id': f"{best_match.get('ClusterId', 'Unknown')}.{best_match.get('ProcId', 'Unknown')}",
                'evicting_job_queue_date': best_match[time_column],
                'evicting_job_start_date': best_match.get('JobStartDate', 'Unknown'),
                'evicting_job_user': best_match.get('User', 'Unknown'),
                'evicting_job_project': best_match.get('ProjectName', 'Unknown'),
                'evicting_job_assigned_gpu': best_match.get('AssignedGPUs', 'Unknown'),
                'match_confidence': confidence,
                'match_reason': ','.join(match_reasons),
                'time_diff_minutes': time_diff_minutes
            })
        else:
            # No good matches found
            eviction_result = eviction.copy()
            eviction_result.update({
                'evicting_job_id': None,
                'evicting_job_queue_date': None,
                'evicting_job_start_date': None,
                'evicting_job_user': None,
                'evicting_job_project': None,
                'match_confidence': 'no_match',
                'match_reason': f'No matching jobs found on host {host}'
            })
        
        results.append(eviction_result)
    
    return results

def analyze_evictions(log_dir, output_file, gpu_jobs_csv=None, host_filter=None):
    """Analyze all log files in directory and write results to CSV."""
    evictions = []
    log_files = list(Path(log_dir).rglob('*.log'))

    print(f"Processing {len(log_files)} log files from {log_dir}")

    for log_file in log_files:
        results = parse_log_file(log_file)
        if results:
            evictions.extend(results)

    print(f"Found {len(evictions)} evicted jobs")

    # Apply host filter if specified
    if host_filter:
        original_count = len(evictions)
        evictions = [e for e in evictions if host_filter.lower() in e['host'].lower()]
        filtered_count = len(evictions)
        print(f"Host filter '{host_filter}' applied: {filtered_count}/{original_count} evictions kept")

    # If GPU jobs CSV is provided, match evictions with evicting jobs
    if gpu_jobs_csv and Path(gpu_jobs_csv).exists():
        print(f"Loading GPU jobs data from {gpu_jobs_csv}")
        gpu_jobs_df = load_gpu_jobs(gpu_jobs_csv)
        
        if not gpu_jobs_df.empty:
            print(f"Loaded {len(gpu_jobs_df)} GPU jobs. Matching with evictions...")
            evictions = find_evicting_jobs(evictions, gpu_jobs_df)
            
            # Count matches by confidence
            confidence_counts = {}
            for eviction in evictions:
                conf = eviction.get('match_confidence', 'unknown')
                confidence_counts[conf] = confidence_counts.get(conf, 0) + 1
            
            print("Match confidence distribution:")
            for conf, count in confidence_counts.items():
                print(f"  {conf}: {count}")
        else:
            print("Warning: GPU jobs CSV is empty or could not be loaded")

    # Write results to CSV with reorganized columns for better readability
    if gpu_jobs_csv and evictions and 'evicting_job_id' in evictions[0]:
        # Full output with evicting job information - put timing columns together
        fieldnames = [
            'job_id', 'host', 'slot_name', 'gpu_id', 'gpu_type', 'eviction_code',
            'eviction_time', 'evicting_job_queue_date', 'time_diff_minutes',
            'evicting_job_id', 'evicting_job_user', 'evicting_job_project',
            'evicting_job_start_date', 'evicting_job_assigned_gpu', 
            'match_confidence', 'match_reason',
            'start_time', 'end_time', 'runtime_seconds', 'log_file'
        ]
    else:
        # Basic output without evicting job information
        fieldnames = [
            'job_id', 'host', 'slot_name', 'gpu_id', 'gpu_type', 'eviction_code',
            'eviction_time', 'start_time', 'end_time', 'runtime_seconds', 'log_file'
        ]
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(evictions)

    print(f"Results written to {output_file}")

def main():
    """Main function with command line argument handling."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze GPU backfill eviction logs')
    parser.add_argument('--log-dir', default='gpu-backfill-evictions',
                       help='Directory containing HTCondor log files')
    parser.add_argument('--output', default='evictions_report.csv',
                       help='Output CSV file path')
    parser.add_argument('--gpu-jobs-csv', 
                       help='CSV file containing GPU jobs data for matching')
    parser.add_argument('--time-window', type=int, default=30,
                       help='Time window in minutes for matching jobs (default: 30)')
    parser.add_argument('--host-filter', 
                       help='Only analyze evictions from hosts containing this string (e.g., "txie-dsigpu4000" or "ssilwalgpu4000")')
    
    args = parser.parse_args()
    
    analyze_evictions(args.log_dir, args.output, args.gpu_jobs_csv, args.host_filter)

if __name__ == '__main__':
    main()
