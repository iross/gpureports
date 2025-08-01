"""
IAR testing out claude-3.7 sonnet
"""
import pandas as pd
import numpy as np
from datetime import datetime

def analyze_machine_job_gaps(csv_file, machine_name, min_gap_hours=1):
    """
    Analyze gaps in job execution for a specific machine.
    
    Parameters:
    -----------
    csv_file : str
        Path to the CSV file with job data
    machine_name : str
        Name of the machine to analyze
    min_gap_hours : float
        Minimum gap duration in hours to consider (default: 1 hour)
        
    Returns:
    --------
    dict
        Dictionary with analysis results
    """
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Filter for the specific machine
    machine_jobs = df[df['StartdName'] == machine_name].copy()
    
    print(f"Total jobs for {machine_name}: {len(machine_jobs)}")
    
    if len(machine_jobs) == 0:
        return {"error": f"No jobs found for machine {machine_name}"}
    
    # Convert to numeric to ensure proper handling of timestamps
    for col in ['JobStartDate', 'CompletionDate']:
        machine_jobs[col] = pd.to_numeric(machine_jobs[col], errors='coerce')
    
    # Drop rows with missing start or completion dates
    valid_jobs = machine_jobs.dropna(subset=['JobStartDate', 'CompletionDate'])
    
    # Sort jobs by start date
    valid_jobs = valid_jobs.sort_values('JobStartDate')
    
    # Create a timeline of events
    timeline = []
    
    for _, job in valid_jobs.iterrows():
        timeline.append({
            'time': job['JobStartDate'],
            'type': 'start',
            'job_id': job.name  # Use the DataFrame index as job_id
        })
        
        timeline.append({
            'time': job['CompletionDate'],
            'type': 'end',
            'job_id': job.name
        })
    
    # Sort timeline events by time
    timeline = sorted(timeline, key=lambda x: x['time'])
    
    # Find gaps in the timeline
    active_jobs = 0
    last_time = None
    gaps = []
    
    for event in timeline:
        # If we had 0 active jobs and this isn't the first event
        if active_jobs == 0 and last_time is not None:
            gap_duration = event['time'] - last_time
            # Convert to hours and check minimum gap threshold
            gap_duration_hours = gap_duration / 3600
            
            if gap_duration_hours > min_gap_hours:
                gaps.append({
                    'start': last_time,
                    'end': event['time'],
                    'duration_seconds': gap_duration,
                    'duration_hours': gap_duration_hours,
                    'start_date': datetime.fromtimestamp(last_time).isoformat(),
                    'end_date': datetime.fromtimestamp(event['time']).isoformat()
                })
        
        # Update active job count
        if event['type'] == 'start':
            active_jobs += 1
        else:
            active_jobs -= 1
        
        last_time = event['time']
    
    # Calculate gap statistics
    if len(gaps) > 0:
        total_gap_time = sum(gap['duration_seconds'] for gap in gaps)
        avg_gap_duration = total_gap_time / len(gaps)
        min_gap_duration = min(gap['duration_seconds'] for gap in gaps)
        max_gap_duration = max(gap['duration_seconds'] for gap in gaps)
        
        # Sort gaps by duration (longest first)
        gaps.sort(key=lambda x: x['duration_seconds'], reverse=True)
        
        # Calculate overall timeline
        first_job_start = valid_jobs['JobStartDate'].min()
        last_job_end = valid_jobs['CompletionDate'].max()
        total_time_range = last_job_end - first_job_start
        
        # Calculate monthly statistics
        valid_jobs['month'] = pd.to_datetime(valid_jobs['JobStartDate'], unit='s').dt.strftime('%Y-%m')
        valid_jobs['runtime'] = valid_jobs['CompletionDate'] - valid_jobs['JobStartDate']
        
        monthly_stats = valid_jobs.groupby('month').agg(
            job_count=('JobStartDate', 'count'),
            total_runtime=('runtime', 'sum')
        ).reset_index()
        
        monthly_stats['total_runtime_hours'] = monthly_stats['total_runtime'] / 3600
        monthly_stats['utilization'] = monthly_stats['total_runtime'] / (30 * 24 * 3600) * 100  # Approx 30 days per month
        
        return {
            "total_jobs": len(machine_jobs),
            "valid_jobs": len(valid_jobs),
            "gaps": {
                "count": len(gaps),
                "details": gaps[:10],  # Top 10 longest gaps
                "total_gap_time_hours": total_gap_time / 3600,
                "avg_gap_duration_hours": avg_gap_duration / 3600,
                "min_gap_duration_hours": min_gap_duration / 3600,
                "max_gap_duration_hours": max_gap_duration / 3600
            },
            "timeline": {
                "first_job_start": datetime.fromtimestamp(first_job_start).isoformat(),
                "last_job_end": datetime.fromtimestamp(last_job_end).isoformat(),
                "total_days": total_time_range / (24 * 3600),
                "idle_percentage": (total_gap_time / total_time_range) * 100
            },
            "monthly_stats": monthly_stats.to_dict('records')
        }
    else:
        return {
            "total_jobs": len(machine_jobs),
            "valid_jobs": len(valid_jobs),
            "gaps": {
                "count": 0,
                "message": "No significant gaps found"
            }
        }

def print_gap_analysis(results):
    """
    Print the gap analysis results in a readable format
    
    Parameters:
    -----------
    results : dict
        Results from analyze_machine_job_gaps function
    """
    if "error" in results:
        print(results["error"])
        return
    
    print(f"\nTotal jobs: {results['total_jobs']}")
    print(f"Jobs with valid start/end dates: {results['valid_jobs']}")
    
    gaps = results['gaps']
    if gaps['count'] == 0:
        print("\nNo significant gaps found. The machine was continuously running jobs.")
    else:
        print(f"\nFound {gaps['count']} gaps in job execution:")
        print("\nTop longest gaps:")
        for i, gap in enumerate(gaps['details'], 1):
            print(f"Gap #{i}: From {gap['start_date']} to {gap['end_date']} ({gap['duration_hours']:.2f} hours)")
        
        print("\nGap statistics:")
        print(f"Total time with no jobs: {gaps['total_gap_time_hours']:.2f} hours")
        print(f"Average gap duration: {gaps['avg_gap_duration_hours']:.2f} hours")
        print(f"Shortest gap: {gaps['min_gap_duration_hours']:.2f} hours")
        print(f"Longest gap: {gaps['max_gap_duration_hours']:.2f} hours")
    
    if 'timeline' in results:
        timeline = results['timeline']
        print("\nOverall timeline:")
        print(f"First job started: {timeline['first_job_start']}")
        print(f"Last job ended: {timeline['last_job_end']}")
        print(f"Total time range: {timeline['total_days']:.2f} days")
        print(f"Percentage of time idle: {timeline['idle_percentage']:.2f}%")
    
    if 'monthly_stats' in results:
        print("\nMonthly utilization analysis:")
        for month in results['monthly_stats']:
            print(f"{month['month']}: {month['job_count']} jobs, "
                  f"{month['total_runtime_hours']:.2f} hours runtime, "
                  f"~{month['utilization']:.2f}% utilization")

# Usage example
if __name__ == "__main__":
    csv_file = "gpu_jobs.csv"
    machine_name = "jcaicedogpu0000.chtc.wisc.edu"
    
    results = analyze_machine_job_gaps(csv_file, machine_name)
    print_gap_analysis(results)
    
    # To analyze multiple machines
    '''
    machine_names = [
        "jcaicedogpu0001.chtc.wisc.edu",
        "jcaicedogpu0002.chtc.wisc.edu",
        # Add more machines here
    ]
    
    for machine in machine_names:
        print(f"\n{'=' * 50}\nAnalyzing {machine}\n{'=' * 50}")
        results = analyze_machine_job_gaps(csv_file, machine)
        print_gap_analysis(results)
    '''