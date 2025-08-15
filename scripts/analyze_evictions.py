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
import sys
from pathlib import Path
import csv
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Add parent directory to path to import device name mappings
sys.path.append(str(Path(__file__).parent.parent))
from device_name_mappings import get_human_readable_device_name

def create_eviction_heatmaps(evictions, all_job_configs, output_file):
    """Create heatmap plots showing n_gpus vs sleep_time vs average eviction_count by capability."""
    # Step 1: Count evictions per job_id
    job_eviction_counts = {}
    for eviction in evictions:
        job_id = eviction['job_id']
        job_eviction_counts[job_id] = job_eviction_counts.get(job_id, 0) + 1
    
    # Step 2: Create job data using all job configurations (including those with 0 evictions)
    job_data = []
    for job_id, config in all_job_configs.items():
        try:
            requested_gpus = int(config.get('requested_gpus', 0)) if config.get('requested_gpus') != 'unknown' else None
            sleep_time = int(config.get('sleep_time', 0)) if config.get('sleep_time') != 'unknown' else None
            capability = config.get('capability', 'unknown')
            
            if requested_gpus is not None and sleep_time is not None and capability != 'unknown':
                eviction_count = job_eviction_counts.get(job_id, 0)  # 0 if no evictions
                job_data.append({
                    'job_id': job_id,
                    'requested_gpus': requested_gpus,
                    'sleep_time': sleep_time,
                    'capability': capability,
                    'eviction_count': eviction_count
                })
        except (ValueError, TypeError):
            continue  # Skip jobs with invalid numeric data
    
    if not job_data:
        print("No valid job configurations with numeric parameters found for plotting")
        return
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(job_data)
    
    # Step 3: Group by configuration and calculate average eviction count
    aggregated = df.groupby(['requested_gpus', 'sleep_time', 'capability']).agg({
        'eviction_count': ['mean', 'count']
    }).reset_index()
    
    # Flatten column names
    aggregated.columns = ['requested_gpus', 'sleep_time', 'capability', 'avg_eviction_count', 'job_count']
    
    # Get unique capabilities
    capabilities = sorted(aggregated['capability'].unique())
    
    if not capabilities:
        print("No capabilities found for plotting")
        return
    
    # Create subplots - arrange in a grid
    n_caps = len(capabilities)
    if n_caps == 1:
        fig, axes = plt.subplots(1, 1, figsize=(8, 6))
        axes = [axes]
    elif n_caps == 2:
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    elif n_caps <= 4:
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        axes = axes.flatten()
    else:
        # For more than 4, use a grid that accommodates all
        cols = min(3, n_caps)
        rows = (n_caps + cols - 1) // cols
        fig, axes = plt.subplots(rows, cols, figsize=(5*cols, 5*rows))
        if rows == 1:
            axes = [axes] if n_caps == 1 else axes
        else:
            axes = axes.flatten()
    
    # Calculate global min/max for consistent color scale
    global_min = aggregated['avg_eviction_count'].min()
    global_max = aggregated['avg_eviction_count'].max()
    
    # Store the first heatmap for colorbar creation
    first_heatmap = None
    
    # Create a heatmap for each capability
    for i, capability in enumerate(capabilities):
        cap_data = aggregated[aggregated['capability'] == capability]
        
        if cap_data.empty:
            axes[i].text(0.5, 0.5, f'No data for\ncapability {capability}', 
                        transform=axes[i].transAxes, ha='center', va='center')
            axes[i].set_title(f'Capability: {capability}')
            continue
        
        # Create pivot table for heatmap
        pivot_table = cap_data.pivot_table(
            index='sleep_time', 
            columns='requested_gpus', 
            values='avg_eviction_count', 
            fill_value=0
        )
        
        # Create heatmap with global color scale (no individual colorbars)
        heatmap = sns.heatmap(
            pivot_table, 
            ax=axes[i], 
            annot=True, 
            fmt='.1f',  # Format as float with 1 decimal place
            cmap='YlOrRd',
            vmin=global_min,  # Set global minimum for color scale
            vmax=global_max,  # Set global maximum for color scale
            cbar=False,  # No individual colorbars
            square=False
        )
        
        # Store first heatmap for colorbar creation
        if first_heatmap is None:
            first_heatmap = heatmap
        
        axes[i].set_title(f'Capability: {capability}')
        axes[i].set_xlabel('Number of GPUs Requested')
        axes[i].set_ylabel('Sleep Time')
        
        # Rotate x-axis labels for better readability
        axes[i].tick_params(axis='x', rotation=45)
    
    # Hide any unused subplots
    for i in range(len(capabilities), len(axes)):
        axes[i].set_visible(False)
    
    # Apply tight_layout first to arrange subplots nicely
    plt.tight_layout()
    
    # Add a single colorbar for the entire figure
    if first_heatmap is not None:
        # Manually adjust subplot positions to make room for colorbar
        plt.subplots_adjust(right=0.82)
        # Create colorbar on the right side of the figure
        cbar = fig.colorbar(first_heatmap.collections[0], ax=axes, shrink=0.8, aspect=30)
        cbar.set_label('Avg Eviction Count', rotation=270, labelpad=20)
    
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Heatmap plots saved to {output_file}")
    
    # Print summary statistics
    print(f"\nPlot Summary:")
    print(f"Total jobs analyzed: {len(job_data)}")
    print(f"Total configurations: {len(aggregated)}")
    print(f"Capabilities plotted: {capabilities}")
    for capability in capabilities:
        cap_jobs = len([j for j in job_data if j['capability'] == capability])
        cap_configs = len(aggregated[aggregated['capability'] == capability])
        print(f"  {capability}: {cap_jobs} jobs across {cap_configs} configurations")

def create_simple_summary(evictions, output_file):
    """Create a simple summary with jobId, eviction count, device type, and filename components."""
    # Aggregate evictions by job ID
    job_summary = {}
    
    for eviction in evictions:
        job_id = eviction['job_id']
        gpu_type = eviction.get('gpu_type', 'Unknown')
        
        # Convert to short device name
        short_device_name = get_human_readable_device_name(gpu_type)
        
        if job_id not in job_summary:
            job_summary[job_id] = {
                'eviction_count': 0,
                'devices': set(),
                'requested_gpus': eviction.get('requested_gpus', 'unknown'),
                'sleep_time': eviction.get('sleep_time', 'unknown'),
                'capability': eviction.get('capability', 'unknown'),
                'cluster_id': eviction.get('cluster_id', 'unknown'),
                'proc_id': eviction.get('proc_id', 'unknown')
            }
        
        job_summary[job_id]['eviction_count'] += 1
        job_summary[job_id]['devices'].add(short_device_name)
    
    # Create output data
    summary_data = []
    for job_id, data in job_summary.items():
        # Filter out "Unknown" device types if we have any known ones
        devices = data['devices'].copy()
        if len(devices) > 1 and 'Unknown' in devices:
            devices.discard('Unknown')
        
        # Join device types if job ran on multiple device types
        device_types = ', '.join(sorted(devices)) if devices else 'Unknown'
        summary_data.append({
            'job_id': job_id,
            'eviction_count': data['eviction_count'],
            'device_type': device_types,
            'requested_gpus': data['requested_gpus'],
            'sleep_time': data['sleep_time'],
            'capability': data['capability'],
            'cluster_id': data['cluster_id'],
            'proc_id': data['proc_id']
        })
    
    # Sort by eviction count (descending) then by job_id
    summary_data.sort(key=lambda x: (-x['eviction_count'], x['job_id']))
    
    # Write to CSV
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['job_id', 'eviction_count', 'device_type', 'requested_gpus', 'sleep_time', 'capability', 'cluster_id', 'proc_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_data)
    
    print(f"Simple summary with {len(summary_data)} jobs written to {output_file}")
    
    # Print some basic stats
    total_evictions = sum(data['eviction_count'] for data in summary_data)
    print(f"Total evictions: {total_evictions}")
    if summary_data:
        max_evictions = max(data['eviction_count'] for data in summary_data)
        print(f"Max evictions for single job: {max_evictions}")

def parse_log_file(file_path):
    """Parse a single log file and extract eviction information."""
    with open(file_path, 'r') as f:
        content = f.read()

    # Extract filename components: (requested_gpus)_(sleep_time)_(capability)_(cluster_id)_(proc_id)
    filename_stem = Path(file_path).stem
    filename_parts = filename_stem.split('_')
    
    # Initialize with defaults
    requested_gpus = "unknown"
    sleep_time = "unknown"
    capability = "unknown"
    cluster_id = "unknown"
    proc_id = "unknown"
    
    # Parse based on number of components
    if len(filename_parts) == 5:
        # Full format: requested_gpus_sleep_time_capability_cluster_id_proc_id
        requested_gpus, sleep_time, capability, cluster_id, proc_id = filename_parts
    elif len(filename_parts) == 2:
        # Current format: cluster_id_proc_id
        cluster_id, proc_id = filename_parts
    else:
        # Try to extract what we can
        if len(filename_parts) >= 2:
            cluster_id = filename_parts[-2]
            proc_id = filename_parts[-1]
    
    # Extract full job ID from log content (cluster.process.subproc format)
    job_id_match = re.search(r'\((\d+\.\d+\.\d+)\)', content)
    if job_id_match:
        full_job_id = job_id_match.group(1)
        # Convert to cluster.process format (drop the subproc part)
        cluster, process, subproc = full_job_id.split('.')
        job_id = f"{cluster}.{process}"
        # Update cluster_id and proc_id from log content if they were unknown
        if cluster_id == "unknown":
            cluster_id = cluster
        if proc_id == "unknown":
            proc_id = process
    else:
        # Fallback to filename-based extraction if no match found
        job_id = f"{cluster_id}.{proc_id}" if cluster_id != "unknown" and proc_id != "unknown" else cluster_id

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
                
                if 'AvailableGPUs = {' in lines[j]:
                    # Handle both single and multiple GPU cases
                    gpu_matches = re.findall(r'GPUs_GPU_(\w+)', lines[j])
                    if gpu_matches:
                        gpu_id = gpu_matches[0]  # Use first GPU for device name lookup
                
                # Look for the GPU specification line that contains DeviceName
                if f'GPUs_GPU_{gpu_id} = [' in lines[j]:
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
            'log_file': str(file_path),
            'requested_gpus': requested_gpus,
            'sleep_time': sleep_time,
            'capability': capability,
            'cluster_id': cluster_id,
            'proc_id': proc_id
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

def analyze_evictions(log_dir, output_file, gpu_jobs_csv=None, host_filter=None, simple_summary=False, plot=False, plot_output='eviction_heatmaps.png'):
    """Analyze all log files in directory and write results to CSV."""
    evictions = []
    log_files = list(Path(log_dir).rglob('*.log'))

    print(f"Processing {len(log_files)} log files from {log_dir}")

    # Collect all job configurations (including those with 0 evictions)
    all_job_configs = {}
    
    for log_file in log_files:
        results = parse_log_file(log_file)
        if results:
            evictions.extend(results)
            # Store the job configuration from the first eviction record for this job
            for result in results:
                job_id = result['job_id']
                if job_id not in all_job_configs:
                    all_job_configs[job_id] = {
                        'requested_gpus': result.get('requested_gpus', 'unknown'),
                        'sleep_time': result.get('sleep_time', 'unknown'),
                        'capability': result.get('capability', 'unknown')
                    }
        else:
            # Even if no evictions, try to extract job configuration from filename
            try:
                filename_stem = Path(log_file).stem
                filename_parts = filename_stem.split('_')
                
                if len(filename_parts) == 5:
                    # Full format: requested_gpus_sleep_time_capability_cluster_id_proc_id
                    requested_gpus, sleep_time, capability, cluster_id, proc_id = filename_parts
                    job_id = f"{cluster_id}.{proc_id}"
                elif len(filename_parts) == 2:
                    # Current format: cluster_id_proc_id - no additional config info
                    cluster_id, proc_id = filename_parts
                    job_id = f"{cluster_id}.{proc_id}"
                    requested_gpus = sleep_time = capability = 'unknown'
                else:
                    continue
                
                if job_id not in all_job_configs:
                    all_job_configs[job_id] = {
                        'requested_gpus': requested_gpus,
                        'sleep_time': sleep_time,
                        'capability': capability
                    }
            except (ValueError, IndexError):
                continue  # Skip files with unparseable filenames

    print(f"Found {len(evictions)} evicted jobs from {len(all_job_configs)} total jobs")

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

    # If simple summary is requested, create aggregated data
    if simple_summary:
        create_simple_summary(evictions, output_file)
        if not plot:  # Only return if plots are not also requested
            return
    
    # If plots are requested, create heatmaps
    if plot:
        create_eviction_heatmaps(evictions, all_job_configs, plot_output)
        if simple_summary:  # If both summary and plots were requested, we're done
            return

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
    parser.add_argument('--simple-summary', action='store_true',
                       help='Output a simple summary: jobId, eviction_count, device_type')
    parser.add_argument('--plot', action='store_true',
                       help='Generate heatmap plots showing n_gpus vs sleep_time vs eviction_count by capability')
    parser.add_argument('--plot-output', default='eviction_heatmaps.png',
                       help='Output file for heatmap plots (default: eviction_heatmaps.png)')
    
    args = parser.parse_args()
    
    analyze_evictions(args.log_dir, args.output, args.gpu_jobs_csv, args.host_filter, args.simple_summary, args.plot, args.plot_output)

if __name__ == '__main__':
    main()
