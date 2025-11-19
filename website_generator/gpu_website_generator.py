#!/usr/bin/env python3
"""
GPU Monitoring Website Generator

Creates a comprehensive multi-page website with host-by-host GPU timeline breakdowns,
summary statistics, and navigation. Uses gpu_timeline_heatmap module for individual
host visualizations and builds a complete web dashboard.
"""

import datetime
import subprocess

# Import shared utilities
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import typer

sys.path.append("..")
from gpu_timeline_heatmap import get_time_filtered_data

import gpu_utils

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore", category=UserWarning)


def discover_hosts(df: pd.DataFrame, min_gpus: int = 1) -> list[dict]:
    """
    Discover all hosts in the dataset and collect their statistics.

    Args:
        df: GPU state DataFrame
        min_gpus: Minimum number of GPUs required for a host to be included

    Returns:
        List of host dictionaries with statistics
    """
    hosts = []

    for hostname in sorted(df["Machine"].unique()):
        host_df = df[df["Machine"] == hostname]

        # Get unique GPUs for this host
        unique_gpus = host_df["AssignedGPUs"].unique()
        gpu_count = len(unique_gpus)

        if gpu_count < min_gpus:
            continue

        # Get GPU device types
        device_types = host_df["GPUs_DeviceName"].unique()
        primary_device = device_types[0] if len(device_types) > 0 else "Unknown"

        # Calculate basic statistics
        total_records = len(host_df)
        claimed_records = len(host_df[host_df["State"] == "Claimed"])
        utilization_pct = (claimed_records / total_records * 100) if total_records > 0 else 0

        hosts.append(
            {
                "hostname": hostname,
                "gpu_count": gpu_count,
                "primary_device": primary_device,
                "total_records": total_records,
                "utilization_pct": utilization_pct,
                "unique_gpus": list(unique_gpus),
            }
        )

    return hosts


def add_navigation_to_host_page(html_file: Path, hostname: str) -> None:
    """
    Add navigation header to a host page.

    Args:
        html_file: Path to the HTML file to modify
        hostname: Hostname for the page title
    """
    try:
        with open(html_file, encoding="utf-8") as f:
            content = f.read()

        # Add navigation after the opening container div
        nav_html = f"""
        <div style="margin-bottom: 20px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; border-left: 4px solid #007bff;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <a href="index.html" style="color: #007bff; text-decoration: none; font-weight: bold;">← Back to Dashboard</a>
                <span style="color: #666; font-size: 14px;">Host: {hostname}</span>
            </div>
        </div>"""

        # Insert after the first <div class="container">
        content = content.replace('<div class="container">', f'<div class="container">{nav_html}', 1)

        with open(html_file, "w", encoding="utf-8") as f:
            f.write(content)

    except Exception as e:
        print(f"Warning: Could not add navigation to {html_file}: {e}")


def generate_host_page(
    hostname: str, output_dir: Path, db_path: str, hours_back: int, end_time: datetime.datetime | None = None
) -> str:
    """
    Generate an individual host page using gpu_timeline_heatmap.

    Args:
        hostname: Host to generate page for
        output_dir: Output directory for the website
        db_path: Database path
        hours_back: Hours to look back
        end_time: End time for analysis

    Returns:
        Filename of generated page
    """
    # Create safe filename from hostname
    safe_hostname = hostname.replace(".", "_").replace("-", "_")
    html_filename = f"host_{safe_hostname}.html"

    # Build command to generate host page
    cmd = [
        "uv",
        "run",
        "python",
        "../gpu_timeline_heatmap.py",
        "--db-path",
        db_path,
        "--hours-back",
        str(hours_back),
        "--host",
        hostname,
        "--output-format",
        "html",
        "--output-dir",
        str(output_dir),
        "--title",
        f"GPU Timeline - {hostname}",
    ]

    if end_time:
        cmd.extend(["--end-time", end_time.strftime("%Y-%m-%d %H:%M:%S")])

    # Execute command
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path.cwd())
        if result.returncode != 0:
            print(f"Warning: Failed to generate page for {hostname}: {result.stderr}")
            return None

        # Find the generated file and rename it
        generated_files = list(output_dir.glob("gpu_timeline_heatmap_*.html"))
        if generated_files:
            latest_file = max(generated_files, key=lambda p: p.stat().st_mtime)
            target_file = output_dir / html_filename

            # Add navigation before moving the file
            add_navigation_to_host_page(latest_file, hostname)

            latest_file.rename(target_file)
            return html_filename

    except Exception as e:
        print(f"Error generating page for {hostname}: {e}")

    return None


def create_index_page(
    hosts: list[dict],
    output_dir: Path,
    title: str = "GPU Cluster Monitoring Dashboard",
    time_range: str = "",
    total_gpus: int = 0,
    generation_time: str = "",
) -> None:
    """
    Create the main index page with host listing and navigation.

    Args:
        hosts: List of host dictionaries with statistics
        output_dir: Output directory
        title: Page title
        time_range: Time range string for display
        total_gpus: Total GPU count across cluster
        generation_time: When the site was generated
    """

    # Calculate cluster statistics
    total_hosts = len(hosts)
    avg_utilization = np.mean([h["utilization_pct"] for h in hosts]) if hosts else 0

    # Group hosts by device type
    device_groups = {}
    for host in hosts:
        device = host["primary_device"]
        if device not in device_groups:
            device_groups[device] = []
        device_groups[device].append(host)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f7;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 30px;
            text-align: center;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 2.5em;
            font-weight: 300;
        }}
        
        .header p {{
            margin: 0;
            opacity: 0.9;
            font-size: 1.1em;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
        }}
        
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }}
        
        .stat-label {{
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .section {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 30px;
            overflow: hidden;
        }}
        
        .section-header {{
            background-color: #f8f9fa;
            padding: 20px;
            border-bottom: 1px solid #e9ecef;
        }}
        
        .section-header h2 {{
            margin: 0;
            color: #333;
            font-size: 1.3em;
        }}
        
        .host-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
            padding: 20px;
        }}
        
        .host-card {{
            border: 1px solid #e9ecef;
            border-radius: 8px;
            padding: 20px;
            transition: all 0.2s ease;
        }}
        
        .host-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }}
        
        .host-name {{
            font-weight: bold;
            color: #333;
            margin-bottom: 10px;
            font-size: 1.1em;
        }}
        
        .host-stats {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 15px;
            font-size: 0.9em;
            color: #666;
        }}
        
        .utilization-bar {{
            background-color: #e9ecef;
            height: 8px;
            border-radius: 4px;
            margin-bottom: 15px;
            overflow: hidden;
        }}
        
        .utilization-fill {{
            height: 100%;
            border-radius: 4px;
            transition: width 0.3s ease;
        }}
        
        .view-button {{
            display: inline-block;
            padding: 10px 20px;
            background-color: #007bff;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-size: 0.9em;
            transition: background-color 0.2s;
        }}
        
        .view-button:hover {{
            background-color: #0056b3;
        }}
        
        .device-group {{
            margin-bottom: 20px;
        }}
        
        .device-group h3 {{
            color: #495057;
            margin: 0 0 15px 0;
            padding: 0 20px;
            font-size: 1.1em;
        }}
        
        .footer {{
            text-align: center;
            color: #666;
            margin-top: 40px;
            padding: 20px;
            border-top: 1px solid #e9ecef;
        }}
        
        @media (max-width: 768px) {{
            .stats-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}
            
            .host-grid {{
                grid-template-columns: 1fr;
            }}
            
            .header h1 {{
                font-size: 2em;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{title}</h1>
        <p>Real-time GPU cluster monitoring and utilization tracking</p>
        {f'<p>Time Range: {time_range}</p>' if time_range else ''}
    </div>
    
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-number">{total_hosts}</div>
            <div class="stat-label">Total Hosts</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">{total_gpus}</div>
            <div class="stat-label">Total GPUs</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">{avg_utilization:.1f}%</div>
            <div class="stat-label">Avg Utilization</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">{len(device_groups)}</div>
            <div class="stat-label">Device Types</div>
        </div>
    </div>"""

    # Add device groups
    for device_type, device_hosts in device_groups.items():
        html_content += f"""
    <div class="section">
        <div class="section-header">
            <h2>{device_type} Hosts ({len(device_hosts)} hosts)</h2>
        </div>
        <div class="host-grid">"""

        for host in sorted(device_hosts, key=lambda x: x["hostname"]):
            safe_hostname = host["hostname"].replace(".", "_").replace("-", "_")
            utilization_color = (
                "#28a745" if host["utilization_pct"] > 70 else "#ffc107" if host["utilization_pct"] > 30 else "#dc3545"
            )

            html_content += f"""
            <div class="host-card">
                <div class="host-name">{host['hostname']}</div>
                <div class="host-stats">
                    <span>{host['gpu_count']} GPUs</span>
                    <span>{host['utilization_pct']:.1f}% utilized</span>
                </div>
                <div class="utilization-bar">
                    <div class="utilization-fill" 
                         style="width: {host['utilization_pct']}%; background-color: {utilization_color};"></div>
                </div>
                <a href="host_{safe_hostname}.html" class="view-button">View Timeline →</a>
            </div>"""

        html_content += """
        </div>
    </div>"""

    html_content += f"""
    <div class="footer">
        <p>Generated: {generation_time}</p>
        <p>GPU Monitoring Dashboard - Powered by gpu_timeline_heatmap</p>
    </div>
</body>
</html>"""

    # Write index page
    index_path = output_dir / "index.html"
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Index page created: {index_path}")


def main(
    db_path: str = typer.Option("gpu_state_2025-08.db", help="Path to SQLite database"),
    output_dir: str = typer.Option("gpu_website", help="Output directory for website"),
    hours_back: int = typer.Option(24, help="Number of hours to analyze"),
    end_time: str | None = typer.Option(None, help="End time for analysis (YYYY-MM-DD HH:MM:SS)"),
    min_gpus: int = typer.Option(1, help="Minimum GPUs required for host inclusion"),
    title: str = typer.Option("GPU Cluster Monitoring Dashboard", help="Website title"),
    max_hosts: int | None = typer.Option(None, help="Maximum number of hosts to process (for testing)"),
):
    """
    Generate a comprehensive GPU monitoring website with host-by-host breakdowns.

    Creates a multi-page website with an index page listing all hosts and individual
    timeline pages for each host. Perfect for creating GPU cluster monitoring dashboards.
    """

    # Parse end_time if provided
    parsed_end_time = None
    if end_time:
        try:
            parsed_end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print("Error: Invalid end_time format. Use YYYY-MM-DD HH:MM:SS")
            return

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print("🚀 Generating GPU monitoring website...")
    print(f"📊 Database: {db_path}")
    print(f"⏰ Time range: {hours_back} hours back")
    print(f"📁 Output: {output_path}")

    # Load data
    print("\n📥 Loading GPU data...")
    df = get_time_filtered_data(db_path, hours_back, parsed_end_time)

    if df.empty:
        print("❌ No data found for the specified time range")
        return

    # Apply host filtering
    df = gpu_utils.filter_df(df)
    print(f"✅ Loaded {len(df)} records after host filtering")

    # Discover hosts
    print("\n🔍 Discovering hosts...")
    hosts = discover_hosts(df, min_gpus=min_gpus)

    if not hosts:
        print("❌ No hosts found meeting criteria")
        return

    print(f"✅ Found {len(hosts)} hosts")

    # Limit hosts for testing if specified
    if max_hosts:
        hosts = hosts[:max_hosts]
        print(f"🔧 Limited to {len(hosts)} hosts for testing")

    # Generate individual host pages
    print("\n📄 Generating individual host pages...")
    successful_hosts = []

    for i, host in enumerate(hosts, 1):
        hostname = host["hostname"]
        print(f"  {i}/{len(hosts)} Processing {hostname}...")

        html_file = generate_host_page(hostname, output_path, db_path, hours_back, parsed_end_time)

        if html_file:
            host["html_file"] = html_file
            successful_hosts.append(host)
        else:
            print(f"    ⚠️  Failed to generate page for {hostname}")

    print(f"✅ Generated {len(successful_hosts)} host pages")

    # Calculate time range string
    if df["timestamp"].empty:
        time_range = "No data"
    else:
        start_time = df["timestamp"].min()
        end_time_actual = df["timestamp"].max()
        time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time_actual.strftime('%Y-%m-%d %H:%M')}"

    # Calculate total GPUs
    total_gpus = df["AssignedGPUs"].nunique()
    generation_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Generate index page
    print("\n🏠 Creating index page...")
    create_index_page(
        successful_hosts,
        output_path,
        title=title,
        time_range=time_range,
        total_gpus=total_gpus,
        generation_time=generation_time,
    )

    print("\n🎉 Website generation complete!")
    print(f"📂 Website location: {output_path}")
    print(f"🌐 Open: {output_path / 'index.html'}")
    print(f"📊 Total pages: {len(successful_hosts) + 1} (1 index + {len(successful_hosts)} host pages)")


if __name__ == "__main__":
    typer.run(main)
