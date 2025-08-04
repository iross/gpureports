#!/usr/bin/env python3
"""
Script to plot wait times from GPU jobs data based on RequestGPUs values.
Uses the 'waittime' column as the wait duration metric.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def load_and_clean_data(csv_file):
    """Load CSV data and clean it for analysis."""
    df = pd.read_csv(csv_file)
    
    # Filter out rows with missing waittime data
    df_clean = df.dropna(subset=['initialwaitduration'])
    
    # Convert waittime to hours (assuming it's in fractional days)
    df_clean['waittime_hours'] = df_clean['initialwaitduration']/3600
    
    # Filter out negative wait times (data anomalies)
    df_clean = df_clean[df_clean['waittime_hours'] >= 0]
    
    print(f"Total rows in dataset: {len(df)}")
    print(f"Rows with valid waittime data: {len(df_clean)}")
    print(f"RequestGPUs values: {sorted(df_clean['RequestGpus'].unique())}")
    
    return df_clean

def create_wait_time_plots(df):
    """Create various plots showing wait time patterns."""
    
    # Set up the plotting style
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('GPU Job Wait Times Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Box plot of wait times by RequestGPUs
    ax1 = axes[0, 0]
    df.boxplot(column='waittime_hours', by='RequestGpus', ax=ax1)
    ax1.set_title('Wait Time Distribution by RequestGPUs')
    ax1.set_xlabel('Requested GPUs')
    ax1.set_ylabel('Wait Time (hours)')
    ax1.set_yscale('log')
    
    # Plot 2: Scatter plot of wait times vs RequestGPUs
    ax2 = axes[0, 1]
    gpu_counts = df['RequestGpus'].unique()
    for gpu_count in sorted(gpu_counts):
        subset = df[df['RequestGpus'] == gpu_count]
        ax2.scatter(subset['RequestGpus'], subset['waittime_hours'], 
                   alpha=0.6, label=f'{gpu_count} GPUs', s=20)
    
    ax2.set_xlabel('Requested GPUs')
    ax2.set_ylabel('Wait Time (hours)')
    ax2.set_title('Wait Time vs Requested GPUs (Scatter)')
    ax2.set_yscale('log')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Average wait time by RequestGPUs
    ax3 = axes[1, 0]
    avg_wait_by_gpu = df.groupby('RequestGpus')['waittime_hours'].agg(['mean', 'median', 'count']).reset_index()
    
    x_pos = range(len(avg_wait_by_gpu))
    bars = ax3.bar(x_pos, avg_wait_by_gpu['mean'], alpha=0.7, color='skyblue', label='Mean')
    ax3.plot(x_pos, avg_wait_by_gpu['median'], 'ro-', label='Median', linewidth=2)
    
    ax3.set_xlabel('Requested GPUs')
    ax3.set_ylabel('Average Wait Time (hours)')
    ax3.set_title('Average Wait Time by RequestGPUs')
    ax3.set_xticks(x_pos)
    ax3.set_xticklabels(avg_wait_by_gpu['RequestGpus'])
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Add count annotations on bars
    for i, (bar, count) in enumerate(zip(bars, avg_wait_by_gpu['count'])):
        ax3.annotate(f'n={count}', xy=(bar.get_x() + bar.get_width()/2, bar.get_height()),
                    xytext=(0, 3), textcoords='offset points', ha='center', fontsize=8)
    
    # Plot 4: Histogram of wait times
    ax4 = axes[1, 1]
    for gpu_count in sorted(gpu_counts):
        subset = df[df['RequestGpus'] == gpu_count]
        ax4.hist(subset['waittime_hours'], bins=30, alpha=0.6, 
                label=f'{gpu_count} GPUs', density=True)
    
    ax4.set_xlabel('Wait Time (hours)')
    ax4.set_ylabel('Density')
    ax4.set_title('Wait Time Distribution by RequestGPUs')
    ax4.set_xscale('log')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def print_summary_stats(df):
    """Print summary statistics for wait times."""
    print("\n=== WAIT TIME SUMMARY STATISTICS ===")
    
    # Overall statistics by RequestGPUs
    print("\n--- ALL JOBS ---")
    summary = df.groupby('RequestGpus')['waittime_hours'].agg([
        'count', 'mean', 'median', 'std', 'min', 'max'
    ]).round(2)
    print(summary)
    
    # Statistics by Prioritized status
    print("\n--- PRIORITIZED JOBS (Prioritized = True) ---")
    prioritized_df = df[df['Prioritized'] == True]
    if len(prioritized_df) > 0:
        prioritized_summary = prioritized_df.groupby('RequestGpus')['waittime_hours'].agg([
            'count', 'mean', 'median', 'std', 'min', 'max'
        ]).round(2)
        print(prioritized_summary)
    else:
        print("No prioritized jobs found")
    
    print("\n--- NON-PRIORITIZED JOBS (Prioritized = False) ---")
    non_prioritized_df = df[df['Prioritized'] == False]
    if len(non_prioritized_df) > 0:
        non_prioritized_summary = non_prioritized_df.groupby('RequestGpus')['waittime_hours'].agg([
            'count', 'mean', 'median', 'std', 'min', 'max'
        ]).round(2)
        print(non_prioritized_summary)
    else:
        print("No non-prioritized jobs found")
    
    # Overall comparison
    print("\n--- OVERALL COMPARISON ---")
    print(f"Total jobs with wait time data: {len(df)}")
    print(f"Prioritized jobs: {len(prioritized_df)} ({len(prioritized_df)/len(df)*100:.1f}%)")
    print(f"Non-prioritized jobs: {len(non_prioritized_df)} ({len(non_prioritized_df)/len(df)*100:.1f}%)")
    
    print(f"\nOverall mean wait time: {df['waittime_hours'].mean():.2f} hours")
    print(f"Overall median wait time: {df['waittime_hours'].median():.2f} hours")
    
    if len(prioritized_df) > 0:
        print(f"Prioritized mean wait time: {prioritized_df['waittime_hours'].mean():.2f} hours")
        print(f"Prioritized median wait time: {prioritized_df['waittime_hours'].median():.2f} hours")
    
    if len(non_prioritized_df) > 0:
        print(f"Non-prioritized mean wait time: {non_prioritized_df['waittime_hours'].mean():.2f} hours")
        print(f"Non-prioritized median wait time: {non_prioritized_df['waittime_hours'].median():.2f} hours")

def create_histograms(df):
    """Create histogram plots for wait times by prioritized status and GPU count."""
    
    # Separate data by prioritized status
    prioritized_df = df[df['Prioritized']]
    non_prioritized_df = df[~df['Prioritized']]
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle('Wait Time Histograms by Priority Status and GPU Count', fontsize=16, fontweight='bold')
    
    # Plot 1: Prioritized jobs
    if len(prioritized_df) > 0:
        gpu_counts_prioritized = sorted(prioritized_df['RequestGpus'].unique())
        colors1 = plt.cm.Set1(range(len(gpu_counts_prioritized)))
        
        for i, gpu_count in enumerate(gpu_counts_prioritized):
            subset = prioritized_df[prioritized_df['RequestGpus'] == gpu_count]
            if len(subset) > 0:
                # Use 1-hour width bins: 0-1, 1-2, ..., 22-23, 23-24, then 24+ overflow
                bins = np.arange(0, 26, 1)  # Creates bins [0,1), [1,2), ..., [23,24), [24,25), [25,26)
                
                # Clip values ≥25 to 25 so they fall into the [24,25) overflow bin
                clipped_data = np.clip(subset['waittime_hours'], 0, 25)
                
                print(f"Prioritized {gpu_count} GPUs:")
                print(f"  Data range: {subset['waittime_hours'].min():.2f} - {subset['waittime_hours'].max():.2f}")
                print(f"  Values ≥24h: {len(subset[subset['waittime_hours'] >= 24])}")
                print(f"  Bins shape: {bins.shape}, first few: {bins[:5]}, last few: {bins[-5:]}")
                
                ax1.hist(clipped_data, bins=bins, alpha=0.7, 
                        label=f'{gpu_count} GPUs (n={len(subset)})', 
                        color=colors1[i], density=False)
        
        ax1.set_xlabel('Wait Time (hours)')
        ax1.set_ylabel('Count')
        ax1.set_title(f'Prioritized Jobs (n={len(prioritized_df)})')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(0, 26)
        ax1.set_xticks([0, 4, 8, 12, 16, 20, 24])
        ax1.set_xticklabels(['0', '4', '8', '12', '16', '20', '24'])
        # Add light shade to indicate overflow area
        ax1.axvspan(24, 26, alpha=0.2, color='gray', zorder=0)
    else:
        ax1.text(0.5, 0.5, 'No prioritized jobs found', 
                ha='center', va='center', transform=ax1.transAxes, fontsize=14)
        ax1.set_title('Prioritized Jobs (n=0)')
    
    # Plot 2: Non-prioritized jobs
    if len(non_prioritized_df) > 0:
        gpu_counts_non_prioritized = sorted(non_prioritized_df['RequestGpus'].unique())
        colors2 = plt.cm.Set2(range(len(gpu_counts_non_prioritized)))
        
        for i, gpu_count in enumerate(gpu_counts_non_prioritized):
            subset = non_prioritized_df[non_prioritized_df['RequestGpus'] == gpu_count]
            if len(subset) > 0:
                # Use 1-hour width bins: 0-1, 1-2, ..., 22-23, 23-24, then 24+ overflow
                bins = np.arange(0, 26, 1)  # Creates bins [0,1), [1,2), ..., [23,24), [24,25), [25,26)
                
                # Clip values ≥25 to 25 so they fall into the [24,25) overflow bin
                clipped_data = np.clip(subset['waittime_hours'], 0, 25)
                
                print(f"Non-prioritized {gpu_count} GPUs:")
                print(f"  Data range: {subset['waittime_hours'].min():.2f} - {subset['waittime_hours'].max():.2f}")
                print(f"  Values ≥24h: {len(subset[subset['waittime_hours'] >= 24])}")
                print(f"  Bins shape: {bins.shape}, first few: {bins[:5]}, last few: {bins[-5:]}")
                
                ax2.hist(clipped_data, bins=bins, alpha=0.7, 
                        label=f'{gpu_count} GPUs (n={len(subset)})', 
                        color=colors2[i], density=False)
        
        ax2.set_xlabel('Wait Time (hours)')
        ax2.set_ylabel('Count')
        ax2.set_title(f'Non-Prioritized Jobs (n={len(non_prioritized_df)})')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(0, 26)
        ax2.set_xticks([0, 4, 8, 12, 16, 20, 24])
        ax2.set_xticklabels(['0', '4', '8', '12', '16', '20', '24'])
        # Add light shade to indicate overflow area
        ax2.axvspan(24, 26, alpha=0.2, color='gray', zorder=0)
    else:
        ax2.text(0.5, 0.5, 'No non-prioritized jobs found', 
                ha='center', va='center', transform=ax2.transAxes, fontsize=14)
        ax2.set_title('Non-Prioritized Jobs (n=0)')
    
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('wait_time_histograms.png', dpi=300, bbox_inches='tight')
    print(f"\nHistogram plot saved as 'wait_time_histograms.png'")
    
    # Show the plot
    plt.show()

def main():
    """Main execution function."""
    csv_file = 'gpu_jobs_2025-06-15-2025-07-15.csv'
    
    # Load and clean data
    df = load_and_clean_data(csv_file)
    
    if len(df) == 0:
        print("No valid data found for analysis.")
        return
    
    # Print summary statistics
    print_summary_stats(df)
    
    # Create histograms
    create_histograms(df)

if __name__ == "__main__":
    main()