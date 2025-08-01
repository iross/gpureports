#!/usr/bin/env python3
"""
Task 7 Troubleshoot Database Analyzer

This script analyzes the task7_troubleshoot.db database to understand why
backfill GPUs outnumbered prioritized GPUs at a specific timestamp.
Uses the same categorization logic as usage_stats.py.
"""

import pandas as pd
import sqlite3
import sys
from typing import Dict, List
from collections import defaultdict

# Import the exact filter function and host exclusion logic from usage_stats.py
from usage_stats import filter_df, load_host_exclusions, HOST_EXCLUSIONS, FILTERED_HOSTS_INFO

def should_include_device(device_name: str, include_all_devices: bool = False) -> bool:
    """Check if a device should be included based on usage_stats.py filtering logic."""
    if include_all_devices:
        return True
    if device_name is None:
        return True
    # Skip old/uncommon GPU types for cleaner output (same logic as usage_stats.py)
    old_gpu_types = ["GTX 1080", "P100", "Quadro", "A30", "A40"]
    return not any(old_gpu in device_name for old_gpu in old_gpu_types)

def analyze_gpu_categories(db_path: str, include_all_devices: bool = False) -> Dict:
    """Analyze GPU categories and return detailed breakdown."""
    # Set up host exclusions like usage_stats.py does
    global HOST_EXCLUSIONS, FILTERED_HOSTS_INFO
    import usage_stats
    usage_stats.HOST_EXCLUSIONS = load_host_exclusions(None, "masked_hosts.yaml")
    usage_stats.FILTERED_HOSTS_INFO = []
    
    # Connect to database and load data
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM gpu_state", conn)
    conn.close()
    
    # Convert timestamp to pandas datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Get the timestamp for reporting
    timestamp = df['timestamp'].iloc[0] if len(df) > 0 else None
    
    results = {
        'timestamp': timestamp,
        'total_records': len(df),
        'categories': {}
    }
    
    # Analyze each category
    for category in ['Priority', 'Shared', 'Backfill']:
        results['categories'][category] = {}
        
        # Get data for each state
        for state in ['Claimed', 'Unclaimed']:
            filtered_df = filter_df(df, category, state, "")
            
            # Apply device filtering (exclude older GPUs unless requested)
            if not include_all_devices:
                filtered_df = filtered_df[filtered_df.apply(
                    lambda row: should_include_device(row['GPUs_DeviceName'], include_all_devices), 
                    axis=1
                )]
            
            results['categories'][category][state] = {
                'count': len(filtered_df),
                'unique_gpus': filtered_df['AssignedGPUs'].dropna().nunique(),
                'gpus': []
            }
            
            # Get detailed GPU information
            for _, row in filtered_df.iterrows():
                gpu_info = {
                    'slot_name': row['Name'],
                    'assigned_gpu': row['AssignedGPUs'],
                    'machine': row['Machine'],
                    'device_name': row['GPUs_DeviceName'],
                    'priority_projects': row['PrioritizedProjects'],
                    'remote_owner': row['RemoteOwner'],
                    'global_job_id': row['GlobalJobId'],
                    'gpu_usage': row['GPUsAverageUsage']
                }
                results['categories'][category][state]['gpus'].append(gpu_info)
    
    return results

def print_analysis_results(results: Dict):
    """Print detailed analysis results."""
    print("=" * 80)
    print("TASK 7 TROUBLESHOOT DATABASE ANALYSIS")
    print("=" * 80)
    print(f"Timestamp: {results['timestamp']}")
    print(f"Total Records: {results['total_records']}")
    print("=" * 80)
    
    # Summary table
    print("\nSUMMARY:")
    print("-" * 80)
    print(f"{'Category':<12} {'Claimed':<10} {'Unclaimed':<12} {'Total':<10} {'Unique GPUs':<15}")
    print("-" * 80)
    
    category_totals = {}
    for category in ['Priority', 'Shared', 'Backfill']:
        cat_data = results['categories'][category]
        claimed_count = cat_data['Claimed']['count']
        unclaimed_count = cat_data['Unclaimed']['count']
        total_count = claimed_count + unclaimed_count
        
        # Count unique GPUs across both states
        all_gpus = set()
        for state in ['Claimed', 'Unclaimed']:
            gpus = [gpu['assigned_gpu'] for gpu in cat_data[state]['gpus'] if gpu['assigned_gpu']]
            all_gpus.update(gpus)
        unique_gpu_count = len(all_gpus)
        
        category_totals[category] = {
            'claimed': claimed_count,
            'unclaimed': unclaimed_count,
            'total': total_count,
            'unique_gpus': unique_gpu_count
        }
        
        print(f"{category:<12} {claimed_count:<10} {unclaimed_count:<12} {total_count:<10} {unique_gpu_count:<15}")
    
    print("-" * 80)
    
    # Detailed breakdown by category
    for category in ['Priority', 'Shared', 'Backfill']:
        cat_data = results['categories'][category]
        cat_totals = category_totals[category]
        
        if cat_totals['total'] == 0:
            continue
            
        print(f"\n{category.upper()} GPUs - {cat_totals['total']} slots ({cat_totals['unique_gpus']} unique GPUs)")
        print("=" * 80)
        
        for state in ['Claimed', 'Unclaimed']:
            state_data = cat_data[state]
            if state_data['count'] == 0:
                continue
                
            print(f"\n{state} {category} GPUs: {state_data['count']} slots ({state_data['unique_gpus']} unique)")
            print("-" * 80)
            
            # Group by machine for better organization
            by_machine = defaultdict(list)
            for gpu in state_data['gpus']:
                by_machine[gpu['machine']].append(gpu)
            
            for machine in sorted(by_machine.keys()):
                machine_gpus = by_machine[machine]
                print(f"\n  Machine: {machine} ({len(machine_gpus)} slots)")
                
                for gpu in machine_gpus:
                    slot_name = gpu['slot_name'] or 'N/A'
                    assigned_gpu = gpu['assigned_gpu'] or 'N/A'
                    device_name = gpu['device_name'] or 'N/A'
                    priority_projects = gpu['priority_projects'] or 'None'
                    remote_owner = gpu['remote_owner'] or 'None'
                    job_id = gpu['global_job_id'] or 'None'
                    gpu_usage = f"{float(gpu['gpu_usage'])*100:.1f}%" if gpu['gpu_usage'] is not None and gpu['gpu_usage'] != '' else 'N/A'
                    
                    print(f"    Slot: {slot_name}")
                    print(f"      GPU ID: {assigned_gpu}")
                    print(f"      Device: {device_name}")
                    print(f"      Priority Projects: {priority_projects}")
                    if state == 'Claimed':
                        print(f"      Job Owner: {remote_owner}")
                        print(f"      Job ID: {job_id}")
                        print(f"      Usage: {gpu_usage}")
                    print()

def print_anomaly_analysis(results: Dict):
    """Print specific analysis of the backfill > priority anomaly."""
    print("\n" + "=" * 80)
    print("ANOMALY ANALYSIS: Why Backfill > Priority?")
    print("=" * 80)
    
    priority_unclaimed = results['categories']['Priority']['Unclaimed']['count']
    backfill_unclaimed = results['categories']['Backfill']['Unclaimed']['count']
    
    print(f"Priority Available (Unclaimed): {priority_unclaimed}")
    print(f"Backfill Available (Unclaimed): {backfill_unclaimed}")
    print(f"Difference: {backfill_unclaimed - priority_unclaimed} more backfill GPUs")
    
    # Analyze the backfill GPUs to understand their origin
    backfill_gpus = results['categories']['Backfill']['Unclaimed']['gpus']
    
    # Group backfill GPUs by priority project
    backfill_by_project = defaultdict(list)
    for gpu in backfill_gpus:
        project = gpu['priority_projects'] or 'No Priority Project'
        backfill_by_project[project].append(gpu)
    
    print(f"\nBackfill GPUs by Priority Project:")
    print("-" * 50)
    for project, gpus in sorted(backfill_by_project.items()):
        print(f"  {project}: {len(gpus)} GPUs")
    
    # Check if there are more backfill slots than primary priority slots
    priority_slots = set()
    backfill_slots = set()
    
    for state in ['Claimed', 'Unclaimed']:
        for gpu in results['categories']['Priority'][state]['gpus']:
            if gpu['assigned_gpu']:
                priority_slots.add(gpu['assigned_gpu'])
        for gpu in results['categories']['Backfill'][state]['gpus']:
            if gpu['assigned_gpu']:
                backfill_slots.add(gpu['assigned_gpu'])
    
    print(f"\nGPU Pool Analysis:")
    print("-" * 50)
    print(f"  Unique Priority GPU IDs: {len(priority_slots)}")
    print(f"  Unique Backfill GPU IDs: {len(backfill_slots)}")
    
    # Check for overlap (GPUs that appear in both categories)
    overlap = priority_slots.intersection(backfill_slots)
    backfill_only = backfill_slots - priority_slots
    
    if overlap:
        print(f"  GPUs appearing in BOTH categories: {len(overlap)}")
        print("    This indicates the same physical GPUs have both priority and backfill slots")
    else:
        print("  No GPU overlap between categories")
        print("    This suggests separate physical GPU pools for priority vs backfill")
    
    # Show GPUs that are ONLY in backfill
    if backfill_only:
        print(f"\n  GPUs ONLY in Backfill (not in Priority): {len(backfill_only)}")
        print("    These are the 'extra' backfill GPUs causing the anomaly")
        
        # Get detailed info about backfill-only GPUs
        backfill_only_details = []
        for gpu in backfill_gpus:
            if gpu['assigned_gpu'] in backfill_only:
                backfill_only_details.append(gpu)
        
        if backfill_only_details:
            print(f"\nBACKFILL-ONLY GPUs ({len(backfill_only_details)} GPUs):")
            print("=" * 80)
            
            # Group by machine for better organization
            by_machine = defaultdict(list)
            for gpu in backfill_only_details:
                by_machine[gpu['machine']].append(gpu)
            
            for machine in sorted(by_machine.keys()):
                machine_gpus = by_machine[machine]
                print(f"\n  Machine: {machine} ({len(machine_gpus)} GPUs)")
                
                for gpu in machine_gpus:
                    slot_name = gpu['slot_name'] or 'N/A'
                    assigned_gpu = gpu['assigned_gpu'] or 'N/A'
                    device_name = gpu['device_name'] or 'N/A'
                    priority_projects = gpu['priority_projects'] or 'No Priority Project'
                    
                    print(f"    Slot: {slot_name}")
                    print(f"      GPU ID: {assigned_gpu}")
                    print(f"      Device: {device_name}")
                    print(f"      Priority Projects: {priority_projects}")
                    print()
    else:
        print(f"  No GPUs are exclusively in Backfill category")

def main():
    """Main function."""
    db_path = "task7_troubleshoot.db"
    
    # Parse command line arguments for device filtering
    include_all_devices = False
    if len(sys.argv) > 1 and sys.argv[1] == "--include-all-devices":
        include_all_devices = True
    
    try:
        print("Analyzing task7_troubleshoot.db...")
        if not include_all_devices:
            print("(Filtering out older GPU models: GTX 1080, P100, Quadro, A30, A40)")
            print("(Use --include-all-devices to include all GPU types)")
        results = analyze_gpu_categories(db_path, include_all_devices)
        print_analysis_results(results)
        print_anomaly_analysis(results)
        
    except FileNotFoundError:
        print(f"Error: Database file '{db_path}' not found.")
        print("Make sure you're running this script from the correct directory.")
        sys.exit(1)
    except Exception as e:
        print(f"Error analyzing database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()