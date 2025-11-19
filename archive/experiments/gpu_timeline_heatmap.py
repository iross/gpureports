#!/usr/bin/env python3
"""
GPU Timeline Heatmap Visualization

Creates detailed GPU timeline heatmaps that visualize the availability state of individual GPUs over time.
Shows each GPU's state in 5-minute intervals with color-coded states for easy pattern identification.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import typer
import sqlite3
import datetime
from typing import Optional, List, Dict, Tuple
import re
from pathlib import Path
import warnings

# Import shared utilities
from gpu_utils import (
    load_host_exclusions, get_required_databases,
    HOST_EXCLUSIONS, FILTERED_HOSTS_INFO
)
import gpu_utils

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)

# Define color scheme for GPU states
STATE_COLORS = {
    'idle_prioritized': '#ff4444',      # Red - Idle, prioritized
    'idle_shared': '#ff8800',           # Orange - Idle, open capacity
    'busy_prioritized': '#44ff44',      # Green - Busy, prioritized
    'busy_shared': '#00cc99',           # Teal - Busy, open capacity
    'busy_backfill': '#4488ff',         # Blue - Busy, backfill
    'na': '#cccccc'                     # Gray - No information/GPU dropout
}

STATE_LABELS = {
    'idle_prioritized': 'Idle, prioritized',
    'idle_shared': 'Idle, open capacity',
    'busy_prioritized': 'Busy, prioritized',
    'busy_shared': 'Busy, open capacity',
    'busy_backfill': 'Busy, backfill',
    'na': 'N/A (GPU dropout)'
}


def classify_gpu_state(row: pd.Series) -> str:
    """
    Classify a GPU's state into one of the 6 categories.
    
    Args:
        row: DataFrame row containing GPU state information
        
    Returns:
        String classification of the GPU state
    """
    state = row.get('State', '').lower()
    
    # Determine utilization type based on Name and PrioritizedProjects fields
    name = row.get('Name', '')
    prioritized = row.get('PrioritizedProjects', '') != ''
    
    # Determine utilization type
    if 'backfill' in str(name).lower():
        utilization_type = 'backfill'
    elif prioritized:
        utilization_type = 'priority'
    else:
        utilization_type = 'shared'
    
    if state == 'claimed':
        if utilization_type == 'priority':
            return 'busy_prioritized'
        elif utilization_type == 'shared':
            return 'busy_shared'
        elif utilization_type == 'backfill':
            return 'busy_backfill'
    elif state == 'unclaimed':
        if utilization_type == 'priority':
            return 'idle_prioritized'
        elif utilization_type == 'shared':
            return 'idle_shared'
        elif utilization_type == 'backfill':
            return 'idle_shared'  # Unclaimed backfill slots are considered idle shared capacity
    
    return 'na'


def get_time_filtered_data_multi_db(
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    base_dir: str = "."
) -> pd.DataFrame:
    """
    Get GPU state data filtered by time range, automatically handling multiple database files.
    
    Args:
        start_time: Start time for the range
        end_time: End time for the range
        base_dir: Directory containing database files
        
    Returns:
        DataFrame filtered to the specified time range from all relevant databases
    """
    # Discover required database files
    db_paths = get_required_databases(start_time, end_time, base_dir)
    
    if not db_paths:
        raise FileNotFoundError(f"No database files found for time range {start_time} to {end_time}")
    
    # Load and combine data
    return get_multi_db_data(db_paths, start_time, end_time)


def get_multi_db_data(db_paths: list, start_time: datetime.datetime, end_time: datetime.datetime) -> pd.DataFrame:
    """
    Load and merge data from multiple database files.
    
    Args:
        db_paths: List of database file paths
        start_time: Start time for filtering
        end_time: End time for filtering
        
    Returns:
        Combined DataFrame with data from all databases, filtered by time range
    """
    if not db_paths:
        return pd.DataFrame()
    
    all_dataframes = []
    
    # Add a small buffer to start_time to handle microsecond precision issues
    buffered_start = start_time - datetime.timedelta(seconds=1)
    
    for db_path in db_paths:
        try:
            conn = sqlite3.connect(db_path)
            query = """
            SELECT * FROM gpu_state
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn, params=[
                buffered_start.strftime('%Y-%m-%d %H:%M:%S.%f'), 
                end_time.strftime('%Y-%m-%d %H:%M:%S.%f')
            ])
            conn.close()
            
            if len(df) > 0:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                # Apply precise time filtering
                df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                if len(df) > 0:
                    all_dataframes.append(df)
                    
        except Exception as e:
            print(f"Warning: Could not load data from {db_path}: {e}")
            continue
    
    if not all_dataframes:
        return pd.DataFrame()
    
    # Combine all dataframes
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
    
    # Final time filtering
    combined_df = combined_df[
        (combined_df['timestamp'] >= start_time) &
        (combined_df['timestamp'] <= end_time)
    ]
    
    return combined_df


def create_gpu_identifier(row: pd.Series) -> str:
    """
    Create a unique identifier for each GPU based on hostname and GPU ID.
    
    Args:
        row: DataFrame row
        
    Returns:
        Unique GPU identifier string
    """
    hostname = row.get('Machine', 'unknown')
    gpu_id = str(row.get('AssignedGPUs', '0'))
    return f"{hostname}_{gpu_id}"


def prepare_timeline_data(
    df: pd.DataFrame,
    bucket_minutes: int = 5
) -> pd.DataFrame:
    """
    Prepare data for timeline visualization by creating time buckets and classifying states.
    
    Args:
        df: Raw GPU state data
        bucket_minutes: Size of time buckets in minutes
        
    Returns:
        DataFrame ready for heatmap visualization
    """
    if df.empty:
        return pd.DataFrame()
    
    # Create time buckets
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['time_bucket'] = df['timestamp'].dt.floor(f'{bucket_minutes}min')
    
    # Create GPU identifiers
    df['gpu_id'] = df.apply(create_gpu_identifier, axis=1)
    
    # Apply deduplication logic similar to usage_stats.py
    # When same GPU appears multiple times at same timestamp, prefer higher priority record
    duplicated_gpus = df.groupby(['time_bucket', 'AssignedGPUs']).size() > 1
    if duplicated_gpus.any():
        # Create ranking system: prefer claimed over unclaimed, and primary slots over backfill
        df['_rank'] = 0  # Default rank for Unclaimed + backfill
        df.loc[(df['State'] == 'Claimed') & (~df['Name'].str.contains("backfill")), '_rank'] = 3
        df.loc[(df['State'] == 'Claimed') & (df['Name'].str.contains("backfill")), '_rank'] = 2  
        df.loc[(df['State'] == 'Unclaimed') & (~df['Name'].str.contains("backfill")), '_rank'] = 1
        
        # Sort by time_bucket, AssignedGPUs, and rank (keeping highest rank first)
        df = df.sort_values(['time_bucket', 'AssignedGPUs', '_rank'], ascending=[True, True, False])
        
        # Drop duplicates within each time bucket, keeping the first (highest rank) occurrence
        df = df.drop_duplicates(subset=['time_bucket', 'AssignedGPUs'], keep='first')
        
        # Remove the temporary rank column
        df = df.drop(columns=['_rank'])
    
    # Classify GPU states
    df['state_class'] = df.apply(classify_gpu_state, axis=1)
    
    # Get unique combinations of GPU and time bucket (only where data exists)
    timeline_data = []
    
    for gpu_identifier in df['gpu_id'].unique():
        gpu_df = df[df['gpu_id'] == gpu_identifier]
        
        # Only iterate over time buckets where this GPU has data
        for time_bucket in gpu_df['time_bucket'].unique():
            bucket_df = gpu_df[gpu_df['time_bucket'] == time_bucket]
            
            # Use the most recent state within this bucket
            latest_state = bucket_df.iloc[-1]['state_class']
            
            # Get additional info for labeling
            hostname = bucket_df.iloc[0]['Machine']
            gpu_num = str(bucket_df.iloc[0]['AssignedGPUs'])
            device_name = bucket_df.iloc[0].get('GPUs_DeviceName', 'Unknown')
            
            timeline_data.append({
                'gpu_identifier': gpu_identifier,
                'hostname': hostname,
                'gpu_num': gpu_num,
                'device_name': device_name,
                'time_bucket': time_bucket,
                'state': latest_state
            })
    
    return pd.DataFrame(timeline_data)


def filter_gpus(
    df: pd.DataFrame,
    gpu_ids: Optional[List[str]] = None,
    host: Optional[str] = None,
    hostname_pattern: Optional[str] = None,
    gpu_model_pattern: Optional[str] = None
) -> pd.DataFrame:
    """
    Filter GPUs based on various criteria.
    
    Args:
        df: Timeline data DataFrame
        gpu_ids: List of specific GPU IDs to include
        host: Exact hostname match
        hostname_pattern: Regex pattern for hostname filtering
        gpu_model_pattern: Regex pattern for GPU model filtering
        
    Returns:
        Filtered DataFrame
    """
    filtered_df = df.copy()
    
    if gpu_ids:
        # Filter by specific GPU IDs - support both full GPU-xxxxx format and just the hex part
        def matches_gpu_id(gpu_identifier, target_ids):
            # Extract the GPU part from the identifier (after the hostname_)
            if '_' in gpu_identifier:
                gpu_part = gpu_identifier.split('_', 1)[1]  # Get everything after first underscore
            else:
                gpu_part = gpu_identifier
            
            for target_id in target_ids:
                # Support matching with or without GPU- prefix
                if target_id.lower() in gpu_part.lower():
                    return True
                if f"gpu-{target_id.lower()}" in gpu_part.lower():
                    return True
            return False
        
        filtered_df = filtered_df[filtered_df['gpu_identifier'].apply(lambda x: matches_gpu_id(x, gpu_ids))]
    
    if host:
        # Filter by exact hostname match
        filtered_df = filtered_df[filtered_df['hostname'] == host]
    
    if hostname_pattern:
        # Filter by hostname pattern
        pattern = re.compile(hostname_pattern, re.IGNORECASE)
        filtered_df = filtered_df[
            filtered_df['hostname'].apply(lambda x: bool(pattern.search(str(x))))
        ]
    
    if gpu_model_pattern:
        # Filter by GPU model pattern
        pattern = re.compile(gpu_model_pattern, re.IGNORECASE)
        filtered_df = filtered_df[
            filtered_df['device_name'].apply(lambda x: bool(pattern.search(str(x))))
        ]
    
    return filtered_df


def create_heatmap(
    timeline_df: pd.DataFrame,
    output_path: str,
    title: str = "GPU Timeline Heatmap",
    figsize: Tuple[int, int] = (16, 10)
) -> None:
    """
    Create the GPU timeline heatmap visualization.
    
    Args:
        timeline_df: Prepared timeline data
        output_path: Path to save the PNG file
        title: Title for the plot
        figsize: Figure size (width, height)
    """
    if timeline_df.empty:
        print("No data to visualize")
        return
    
    # Pivot data for heatmap
    pivot_df = timeline_df.pivot(
        index='gpu_identifier',
        columns='time_bucket',
        values='state'
    )
    
    # Map states to numeric values for coloring
    state_mapping = {
        'idle_prioritized': 0,
        'idle_shared': 1,
        'busy_prioritized': 2,
        'busy_shared': 3,
        'busy_backfill': 4,
        'na': 5
    }
    
    numeric_df = pivot_df.replace(state_mapping).infer_objects(copy=False)
    
    # Create figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Create custom colormap from our state colors
    colors = [STATE_COLORS[k] for k in state_mapping.keys()]
    cmap = plt.matplotlib.colors.ListedColormap(colors)
    
    # Create heatmap with explicit value range to ensure proper color mapping
    sns.heatmap(
        numeric_df,
        cmap=cmap,
        vmin=0,  # Minimum value for color mapping
        vmax=5,  # Maximum value for color mapping  
        cbar=False,
        linewidths=0.5,
        linecolor='white',
        ax=ax
    )
    
    # Customize plot
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('GPU Identifier', fontsize=12)
    
    # Format x-axis with labels every 30 minutes
    x_labels = [ts.strftime('%m-%d %H:%M') for ts in pivot_df.columns]
    
    # Find indices for 30-minute intervals
    tick_indices = []
    tick_labels = []
    
    for i, ts in enumerate(pivot_df.columns):
        # Show label if minutes are 00 or 30
        if ts.minute in [0, 30]:
            tick_indices.append(i)
            tick_labels.append(x_labels[i])
    
    # If no 30-minute markers found (short time range), fall back to showing every 6th label
    if not tick_indices:
        tick_indices = range(0, len(x_labels), max(1, len(x_labels) // 6))
        tick_labels = [x_labels[i] for i in tick_indices]
    
    ax.set_xticks(tick_indices)
    ax.set_xticklabels(tick_labels, rotation=45, ha='right')
    
    # Format y-axis - extract just the GPU ID part
    y_labels = []
    for label in pivot_df.index:
        # Extract GPU ID from identifier like "hostname_GPU-5d6e65db" -> "GPU-5d6e65db"
        if '_' in label:
            gpu_id = label.split('_', 1)[1]  # Get everything after first underscore
        else:
            gpu_id = label
        y_labels.append(gpu_id)
    
    ax.set_yticks(range(len(y_labels)))
    ax.set_yticklabels(y_labels, fontsize=8)
    
    # Create custom legend
    legend_elements = []
    for state_key, label in STATE_LABELS.items():
        legend_elements.append(
            plt.Rectangle((0, 0), 1, 1, fc=STATE_COLORS[state_key], label=label)
        )
    
    ax.legend(
        handles=legend_elements,
        loc='upper right',
        bbox_to_anchor=(1.15, 1),
        title='GPU State',
        fontsize=9
    )
    
    # Adjust layout
    plt.tight_layout()
    plt.subplots_adjust(right=0.85)
    
    # Save figure
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Heatmap saved to: {output_path}")


def create_html_heatmap(
    timeline_df: pd.DataFrame,
    output_path: str,
    title: str = "GPU Timeline Heatmap"
) -> None:
    """
    Create an interactive HTML heatmap visualization.
    
    Args:
        timeline_df: Prepared timeline data
        output_path: Path to save the HTML file
        title: Title for the heatmap
    """
    if timeline_df.empty:
        print("No data to visualize")
        return
    
    # Pivot data for heatmap
    pivot_df = timeline_df.pivot(
        index='gpu_identifier',
        columns='time_bucket',
        values='state'
    )
    
    # Create HTML content
    html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            text-align: center;
            margin-bottom: 30px;
        }}
        .heatmap {{
            overflow-x: auto;
            margin: 20px 0;
        }}
        .heatmap table {{
            border-collapse: collapse;
            font-size: 12px;
            min-width: 100%;
        }}
        .heatmap th, .heatmap td {{
            border: 1px solid #ddd;
            text-align: center;
            position: relative;
        }}
        .heatmap th {{
            background-color: #f8f9fa;
            padding: 8px 4px;
            font-weight: bold;
            white-space: nowrap;
        }}
        .heatmap td {{
            width: 20px;
            height: 25px;
            cursor: pointer;
        }}
        .heatmap .gpu-label {{
            text-align: left;
            padding: 8px;
            font-weight: bold;
            min-width: 120px;
            background-color: #f8f9fa;
        }}
        .legend {{
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            margin: 20px 0;
            justify-content: center;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .legend-color {{
            width: 20px;
            height: 15px;
            border: 1px solid #333;
        }}
        .tooltip {{
            position: absolute;
            background-color: #333;
            color: white;
            padding: 8px;
            border-radius: 4px;
            font-size: 12px;
            z-index: 1000;
            pointer-events: none;
            white-space: nowrap;
            opacity: 0;
            transition: opacity 0.2s;
        }}
        .metadata {{
            margin-top: 20px;
            padding: 15px;
            background-color: #f8f9fa;
            border-radius: 4px;
            font-size: 14px;
        }}
        @media (max-width: 768px) {{
            body {{
                margin: 10px;
            }}
            .container {{
                padding: 10px;
            }}
            .heatmap table {{
                font-size: 10px;
            }}
            .heatmap td {{
                width: 15px;
                height: 20px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        
        <div class="legend">
            <div class="legend-item">
                <div class="legend-color" style="background-color: {STATE_COLORS['idle_prioritized']};"></div>
                <span>{STATE_LABELS['idle_prioritized']}</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: {STATE_COLORS['idle_shared']};"></div>
                <span>{STATE_LABELS['idle_shared']}</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: {STATE_COLORS['busy_prioritized']};"></div>
                <span>{STATE_LABELS['busy_prioritized']}</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: {STATE_COLORS['busy_shared']};"></div>
                <span>{STATE_LABELS['busy_shared']}</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: {STATE_COLORS['busy_backfill']};"></div>
                <span>{STATE_LABELS['busy_backfill']}</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background-color: {STATE_COLORS['na']};"></div>
                <span>{STATE_LABELS['na']}</span>
            </div>
        </div>
        
        <div class="heatmap">
            <table>
                <thead>
                    <tr>
                        <th class="gpu-label">GPU</th>"""
    
    # Add time headers (every 30 minutes)
    time_columns = sorted(pivot_df.columns)
    for i, ts in enumerate(time_columns):
        if ts.minute in [0, 30]:  # Show labels every 30 minutes
            html_content += f'<th>{ts.strftime("%m-%d %H:%M")}</th>'
        else:
            html_content += f'<th></th>'
    
    html_content += """
                    </tr>
                </thead>
                <tbody>"""
    
    # Add GPU rows
    for gpu_identifier in sorted(pivot_df.index):
        # Extract just the GPU ID for display
        gpu_display = gpu_identifier.split('_', 1)[1] if '_' in gpu_identifier else gpu_identifier
        
        html_content += f"""
                    <tr>
                        <td class="gpu-label">{gpu_display}</td>"""
        
        for ts in time_columns:
            state = pivot_df.loc[gpu_identifier, ts] if pd.notna(pivot_df.loc[gpu_identifier, ts]) else 'na'
            color = STATE_COLORS.get(state, STATE_COLORS['na'])
            label = STATE_LABELS.get(state, 'Unknown')
            
            # Get hostname from original data for tooltip
            hostname = timeline_df[timeline_df['gpu_identifier'] == gpu_identifier]['hostname'].iloc[0] if not timeline_df[timeline_df['gpu_identifier'] == gpu_identifier].empty else 'Unknown'
            
            html_content += f"""<td style="background-color: {color};" 
                            data-gpu="{gpu_display}" 
                            data-hostname="{hostname}"
                            data-time="{ts.strftime('%Y-%m-%d %H:%M')}" 
                            data-state="{label}"></td>"""
        
        html_content += """
                    </tr>"""
    
    # Add JavaScript for interactivity and metadata
    unique_gpus = len(pivot_df.index)
    time_range = f"{time_columns[0].strftime('%Y-%m-%d %H:%M')} to {time_columns[-1].strftime('%Y-%m-%d %H:%M')}"
    
    html_content += f"""
                </tbody>
            </table>
        </div>
        
        <div class="metadata">
            <strong>Dataset Information:</strong><br>
            • Unique GPUs: {unique_gpus}<br>
            • Time Range: {time_range}<br>
            • Time Buckets: {len(time_columns)}<br>
            • Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
        
        <div class="tooltip" id="tooltip"></div>
    </div>
    
    <script>
        // Add hover functionality
        const cells = document.querySelectorAll('.heatmap td:not(.gpu-label)');
        const tooltip = document.getElementById('tooltip');
        
        cells.forEach(cell => {{
            cell.addEventListener('mouseenter', function(e) {{
                const gpu = e.target.dataset.gpu;
                const hostname = e.target.dataset.hostname;
                const time = e.target.dataset.time;
                const state = e.target.dataset.state;
                
                tooltip.innerHTML = `
                    <strong>${{gpu}}</strong><br>
                    Host: ${{hostname}}<br>
                    Time: ${{time}}<br>
                    State: ${{state}}
                `;
                tooltip.style.opacity = '1';
            }});
            
            cell.addEventListener('mousemove', function(e) {{
                tooltip.style.left = e.pageX + 10 + 'px';
                tooltip.style.top = e.pageY + 10 + 'px';
            }});
            
            cell.addEventListener('mouseleave', function() {{
                tooltip.style.opacity = '0';
            }});
        }});
    </script>
</body>
</html>"""
    
    # Write HTML file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"HTML heatmap saved to: {output_path}")


def get_time_filtered_data(
    db_path: str,
    hours_back: int = 24,
    end_time: Optional[datetime.datetime] = None
) -> pd.DataFrame:
    """
    Get GPU state data filtered by time range.
    Automatically handles month boundaries by loading data from multiple database files.
    
    Args:
        db_path: Path to SQLite database (used to determine base directory for multi-db queries)
        hours_back: Number of hours to look back from end_time
        end_time: End time for the range (defaults to latest timestamp in primary DB)
        
    Returns:
        DataFrame filtered to the specified time range
    """
    from pathlib import Path
    
    # Get base directory from the provided db_path
    db_path_obj = Path(db_path)
    base_dir = str(db_path_obj.parent) if db_path_obj.parent != Path('.') else "."
    
    # If end_time is not provided, use the latest timestamp from the database
    if end_time is None:
        try:
            conn = sqlite3.connect(db_path)
            df_temp = pd.read_sql_query("SELECT MAX(timestamp) as max_time FROM gpu_state", conn)
            conn.close()
            if len(df_temp) > 0 and df_temp['max_time'].iloc[0] is not None:
                end_time = pd.to_datetime(df_temp['max_time'].iloc[0])
            else:
                end_time = datetime.datetime.now()
        except Exception:
            # Fallback to current time if there's any issue with the database
            end_time = datetime.datetime.now()
    
    # Calculate start time
    start_time = end_time - datetime.timedelta(hours=hours_back)
    
    # Check if the time range spans multiple months
    start_month = (start_time.year, start_time.month)
    end_month = (end_time.year, end_time.month)
    
    if start_month == end_month:
        # Single month - use traditional approach
        try:
            conn = sqlite3.connect(db_path)
            df = pd.read_sql_query("SELECT * FROM gpu_state", conn)
            conn.close()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Filter by time range
            filtered_df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
            return filtered_df
        except Exception as e:
            # If single-db approach fails, fall back to multi-db approach
            print(f"Warning: Single database query failed, trying multi-database approach: {e}")
    
    # Multi-month query - use the multi-database functionality
    try:
        return get_time_filtered_data_multi_db(start_time, end_time, base_dir)
    except Exception as e:
        # Final fallback: try just the specified database file
        print(f"Warning: Multi-database query failed, falling back to single database: {e}")
        try:
            conn = sqlite3.connect(db_path)
            query = """
            SELECT * FROM gpu_state
            WHERE timestamp BETWEEN ? AND ?
            ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn, params=[
                start_time.strftime('%Y-%m-%d %H:%M:%S.%f'), 
                end_time.strftime('%Y-%m-%d %H:%M:%S.%f')
            ])
            conn.close()
            if len(df) > 0:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        except Exception as final_e:
            print(f"Error: All database query methods failed: {final_e}")
            return pd.DataFrame()


def main(
    db_path: str = typer.Option("gpu_state_2025-08.db", help="Path to SQLite database"),
    hours_back: int = typer.Option(24, help="Number of hours to analyze (default: 24)"),
    bucket_minutes: int = typer.Option(5, help="Time bucket size in minutes (default: 5)"),
    output_dir: str = typer.Option(".", help="Output directory for saved images"),
    gpu_ids: Optional[str] = typer.Option(None, help="Comma-separated list of specific GPU IDs to include (supports hex IDs like '5d6e65db' or full format 'GPU-5d6e65db')"),
    host: Optional[str] = typer.Option(None, help="Exact hostname match (e.g., 'txie-dsigpu4000.chtc.wisc.edu')"),
    hostname_pattern: Optional[str] = typer.Option(None, help="Regex pattern for hostname filtering"),
    gpu_model_pattern: Optional[str] = typer.Option(None, help="Regex pattern for GPU model filtering"),
    end_time: Optional[str] = typer.Option(None, help="End time for analysis (YYYY-MM-DD HH:MM:SS), defaults to latest in DB"),
    title: Optional[str] = typer.Option(None, help="Custom title for the heatmap"),
    width: int = typer.Option(16, help="Figure width in inches"),
    height: int = typer.Option(6, help="Figure height in inches"),
    output_format: str = typer.Option("png", help="Output format: 'png' or 'html'"),
    list_gpus: bool = typer.Option(False, help="List available GPU IDs and exit (useful for finding GPU IDs to filter)")
):
    """
    Create GPU timeline heatmap visualizations.
    
    This tool generates detailed heatmaps showing GPU state changes over time,
    with 5-minute time buckets and color-coded states for easy pattern identification.
    """
    # Parse end_time if provided
    parsed_end_time = None
    if end_time:
        try:
            parsed_end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f"Error: Invalid end_time format. Use YYYY-MM-DD HH:MM:S")
            return
    
    # Load data
    print(f"Loading GPU data from {db_path} for the last {hours_back} hours...")
    df = get_time_filtered_data(db_path, hours_back, parsed_end_time)
    
    if df.empty:
        print("No data found for the specified time range")
        return
    
    print(f"Loaded {len(df)} records")
    
    # Filter out hosts based on exclusions
    df = gpu_utils.filter_df(df)
    print(f"After host filtering: {len(df)} records")
    
    # If list_gpus is requested, show available GPUs and exit
    if list_gpus:
        print("\nAvailable GPU IDs in the dataset:")
        unique_gpus = df['AssignedGPUs'].unique()
        for i, gpu_id in enumerate(sorted(unique_gpus), 1):
            # Extract just the hex part
            hex_part = gpu_id.replace('GPU-', '') if gpu_id.startswith('GPU-') else gpu_id
            print(f"{i:3d}. {hex_part} (full: {gpu_id})")
        print(f"\nTotal: {len(unique_gpus)} unique GPUs")
        print("\nUsage: --gpu-ids \"5d6e65db,4daa763f\" or --gpu-ids \"GPU-5d6e65db,GPU-4daa763f\"")
        return
    
    # Prepare timeline data
    print("Preparing timeline data...")
    timeline_df = prepare_timeline_data(df, bucket_minutes)
    
    if timeline_df.empty:
        print("No timeline data to visualize")
        return
    
    # Apply GPU filtering
    gpu_id_list = None
    if gpu_ids:
        gpu_id_list = [gpu_id.strip() for gpu_id in gpu_ids.split(',')]
    
    filtered_df = filter_gpus(
        timeline_df,
        gpu_ids=gpu_id_list,
        host=host,
        hostname_pattern=hostname_pattern,
        gpu_model_pattern=gpu_model_pattern
    )
    
    if filtered_df.empty:
        print("No data remaining after filtering")
        return
    
    print(f"Creating heatmap with {len(filtered_df['gpu_identifier'].unique())} unique GPUs")
    
    # Create output path
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Validate output format
    if output_format.lower() not in ['png', 'html']:
        print(f"Error: Invalid output format '{output_format}'. Use 'png' or 'html'")
        return
    
    # Generate filename based on parameters and format
    time_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_extension = output_format.lower()
    filename = f"gpu_timeline_heatmap_{time_suffix}.{file_extension}"
    full_output_path = output_path / filename
    
    # Generate title
    if title is None:
        if host:
            title = f"GPU Timeline Heatmap - {host}"
        elif hostname_pattern:
            title = f"GPU Timeline Heatmap - Hosts matching '{hostname_pattern}'"
        elif gpu_id_list:
            title = f"GPU Timeline Heatmap - Selected GPUs"
        else:
            title = "GPU Timeline Heatmap"
        
        # Add time range to title
        start_time = filtered_df['time_bucket'].min()
        end_time = filtered_df['time_bucket'].max()
        title += f"\n{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}"
    
    # Create heatmap based on output format
    if output_format.lower() == 'png':
        create_heatmap(
            filtered_df,
            str(full_output_path),
            title=title,
            figsize=(width, height)
        )
    else:  # html
        create_html_heatmap(
            filtered_df,
            str(full_output_path),
            title=title
        )
    
    # Print summary
    unique_gpus = len(filtered_df['gpu_identifier'].unique())
    unique_times = len(filtered_df['time_bucket'].unique())
    print(f"Generated heatmap with {unique_gpus} GPUs across {unique_times} time buckets")
    print(f"Time range: {filtered_df['time_bucket'].min()} to {filtered_df['time_bucket'].max()}")


if __name__ == "__main__":
    typer.run(main)
