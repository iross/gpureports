#!/usr/bin/env python3
"""
Script to investigate and visualize backfill usage over the last month.
Creates plots showing backfill usage trends by slot type and user.
"""

import os
import sys

# import seaborn as sns  # Not available in pixi environment
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

# Add parent directory to path to import project modules
sys.path.append(str(Path(__file__).parent.parent))

import gpu_utils
from gpu_utils import analyze_backfill_utilization_by_day, load_host_exclusions
from usage_stats import get_time_filtered_data


def get_latest_db_file():
    """Find the most recent database file by filename (YYYY-MM)."""
    db_files = list(Path(".").glob("gpu_state_*.db"))
    if not db_files:
        raise FileNotFoundError("No database files found")

    # Sort by filename (which contains date), newest first
    db_files.sort(reverse=True)
    return str(db_files[0])


# This function is now implemented in gpu_utils.py as analyze_backfill_utilization_by_day


def create_usage_plots(usage_df, output_dir="plots"):
    """Create visualization plots for backfill usage."""
    os.makedirs(output_dir, exist_ok=True)

    # Set up the plotting style
    plt.style.use("default")
    # sns.set_palette("husl")  # Not using seaborn

    # Create single plot focusing on utilization rate
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    fig.suptitle("Backfill Utilization Rate - Last Two Weeks", fontsize=16, fontweight="bold")

    # Convert date column to datetime for plotting
    usage_df["date"] = pd.to_datetime(usage_df["date"])

    # Calculate total backfill usage across all slot types
    total_usage = usage_df.groupby("date").agg({"AssignedGPUs": "sum", "State": "sum"}).reset_index()
    total_usage["total_utilization"] = (total_usage["State"] / total_usage["AssignedGPUs"] * 100).fillna(0)

    # Plot utilization rate over time with different colors and markers for each slot type
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]  # Blue, Orange, Green, Red
    markers = ["o", "s", "^", "D"]

    for i, slot_type in enumerate(usage_df["slot_type"].unique()):
        slot_data = usage_df[usage_df["slot_type"] == slot_type]
        ax.plot(
            slot_data["date"],
            slot_data["utilization"],
            marker=markers[i],
            label=slot_type,
            linewidth=3,
            markersize=6,
            color=colors[i % len(colors)],
        )

    # Add total backfill usage line
    ax.plot(
        total_usage["date"],
        total_usage["total_utilization"],
        marker="D",
        label="Total Backfill",
        linewidth=4,
        markersize=7,
        color="#d62728",
        linestyle="--",
    )

    ax.set_title("Utilization Rate Over Time", fontsize=14)
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Utilization Rate (%)", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 100)

    # Format x-axis to show dates nicely for 2-week period
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    plt.xticks(rotation=45)

    plt.tight_layout()

    # Save the plot
    plot_file = os.path.join(output_dir, "backfill_utilization_2weeks.png")
    plt.savefig(plot_file, dpi=300, bbox_inches="tight")
    print(f"Plot saved to: {plot_file}")

    plt.show()


def print_summary_stats(usage_df):
    """Print summary statistics."""
    print("\n" + "=" * 60)
    print("BACKFILL USAGE SUMMARY - LAST TWO WEEKS")
    print("=" * 60)

    for slot_type in usage_df["slot_type"].unique():
        slot_data = usage_df[usage_df["slot_type"] == slot_type]

        print(f"\n{slot_type}:")
        print(f"  Average GPUs: {slot_data['AssignedGPUs'].mean():.1f}")
        print(f"  Average Claimed: {slot_data['State'].mean():.1f}")
        print(f"  Average Utilization: {slot_data['utilization'].mean():.1f}%")
        print(f"  Max GPUs: {slot_data['AssignedGPUs'].max():.0f}")
        print(f"  Max Claimed: {slot_data['State'].max():.0f}")
        print(f"  Peak Utilization: {slot_data['utilization'].max():.1f}%")

    # Calculate and display total backfill statistics
    total_stats = usage_df.groupby("date").agg({"AssignedGPUs": "sum", "State": "sum"}).reset_index()
    total_stats["total_utilization"] = (total_stats["State"] / total_stats["AssignedGPUs"] * 100).fillna(0)

    print("\nTotal Backfill (All Slot Types):")
    print(f"  Average GPUs: {total_stats['AssignedGPUs'].mean():.1f}")
    print(f"  Average Claimed: {total_stats['State'].mean():.1f}")
    print(f"  Average Utilization: {total_stats['total_utilization'].mean():.1f}%")
    print(f"  Max GPUs: {total_stats['AssignedGPUs'].max():.0f}")
    print(f"  Max Claimed: {total_stats['State'].max():.0f}")
    print(f"  Peak Utilization: {total_stats['total_utilization'].max():.1f}%")


def main():
    """Main function to run the backfill usage analysis."""
    print("Investigating backfill usage over the last two weeks...")

    # Set up GPU utils
    gpu_utils.HOST_EXCLUSIONS = load_host_exclusions(None, "masked_hosts.yaml")
    gpu_utils.FILTERED_HOSTS_INFO = []

    try:
        # Get the latest database file
        db_file = get_latest_db_file()
        print(f"Using database: {db_file}")

        # Load data for the last 2 weeks (336 hours)
        df = get_time_filtered_data(db_file, 336, None)

        if df.empty:
            print("No data found in the database for the specified time period.")
            return

        print(f"Loaded {len(df)} records spanning {(df['timestamp'].max() - df['timestamp'].min()).days} days")
        print("Processing full dataset for accurate utilization calculations...")

        # Analyze usage over time using shared function
        usage_df = analyze_backfill_utilization_by_day(df)

        if usage_df.empty:
            print("No backfill usage data found.")
            return

        # Print summary statistics
        print_summary_stats(usage_df)

        # Create visualizations
        print("\nGenerating plots...")
        create_usage_plots(usage_df)

        print("\nAnalysis complete!")

    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
