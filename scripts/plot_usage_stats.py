#!/usr/bin/env python3
"""
GPU Usage Statistics Plotter

This script creates plots showing GPU usage statistics for Priority, Shared, and Backfill
classes over time in 15-minute intervals. It builds on the usage_stats.py functionality
to provide visual analysis of GPU utilization patterns.
"""

import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import typer

# Import functions from usage_stats
from usage_stats import calculate_allocation_usage_by_device, calculate_time_series_usage, get_time_filtered_data


def filter_by_gpu_model(df: pd.DataFrame, gpu_model: str) -> pd.DataFrame:
    """
    Filter DataFrame to only include records for a specific GPU model.

    Args:
        df: DataFrame with GPU state data
        gpu_model: GPU model name to filter for (e.g., 'NVIDIA A100-SXM4-80GB')

    Returns:
        Filtered DataFrame
    """
    if not gpu_model:
        return df

    return df[df["GPUs_DeviceName"] == gpu_model].copy()


def get_available_gpu_models(df: pd.DataFrame) -> list:
    """
    Get list of available GPU models in the dataset.

    Args:
        df: DataFrame with GPU state data

    Returns:
        List of unique GPU model names
    """
    return sorted(df["GPUs_DeviceName"].dropna().unique().tolist())


# Optional seaborn import for better styling
try:
    import seaborn as sns

    plt.style.use("seaborn-v0_8")
    sns.set_palette("husl")
    HAS_SEABORN = True
except ImportError:
    # Fall back to basic matplotlib styling
    plt.style.use("ggplot")
    HAS_SEABORN = False


def create_usage_timeline_plot(ts_df: pd.DataFrame, title: str = "GPU Usage Over Time", save_path: str | None = None):
    """
    Create a timeline plot showing usage percentages for all GPU classes.

    Args:
        ts_df: Time series DataFrame from calculate_time_series_usage()
        title: Plot title
        save_path: Optional path to save the plot
    """
    fig, ax = plt.subplots(figsize=(14, 8))

    # Plot lines for each GPU class
    if "priority_usage_percent" in ts_df.columns:
        ax.plot(
            ts_df["timestamp"],
            ts_df["priority_usage_percent"],
            "b-",
            linewidth=2,
            label="Priority",
            marker="o",
            markersize=4,
        )

    if "shared_usage_percent" in ts_df.columns:
        ax.plot(
            ts_df["timestamp"],
            ts_df["shared_usage_percent"],
            "g-",
            linewidth=2,
            label="Shared",
            marker="s",
            markersize=4,
        )

    if "backfill_usage_percent" in ts_df.columns:
        ax.plot(
            ts_df["timestamp"],
            ts_df["backfill_usage_percent"],
            "r-",
            linewidth=2,
            label="Backfill",
            marker="^",
            markersize=4,
        )

    # Format the plot
    ax.set_xlabel("Time", fontsize=12)
    ax.set_ylabel("Usage Percentage (%)", fontsize=12)
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_ylim(0, 105)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=11)

    # Format x-axis
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    ax.xaxis.set_minor_locator(mdates.HourLocator())

    plt.xticks(rotation=45)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        print(f"Saved plot to {save_path}")

    return fig, ax


def create_gpu_count_plot(ts_df: pd.DataFrame, title: str = "GPU Counts Over Time", save_path: str | None = None):
    """
    Create a stacked area plot showing GPU counts (claimed vs unclaimed) over time.

    Args:
        ts_df: Time series DataFrame from calculate_time_series_usage()
        title: Plot title
        save_path: Optional path to save the plot
    """
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    classes = ["priority", "shared", "backfill"]
    colors = ["blue", "green", "red"]

    for i, (gpu_class, color) in enumerate(zip(classes, colors, strict=False)):
        ax = axes[i]

        claimed_col = f"{gpu_class}_claimed"
        total_col = f"{gpu_class}_total"

        if claimed_col in ts_df.columns and total_col in ts_df.columns:
            # Create stacked area plot
            ax.fill_between(
                ts_df["timestamp"], 0, ts_df[claimed_col], color=color, alpha=0.7, label=f"{gpu_class.title()} Claimed"
            )
            ax.fill_between(
                ts_df["timestamp"],
                ts_df[claimed_col],
                ts_df[total_col],
                color=color,
                alpha=0.3,
                label=f"{gpu_class.title()} Unclaimed",
            )

            # Add line for total
            ax.plot(ts_df["timestamp"], ts_df[total_col], color="black", linewidth=1, alpha=0.8, linestyle="--")

        ax.set_ylabel(f"{gpu_class.title()}\nGPU Count", fontsize=11)
        ax.set_title(f"{gpu_class.title()} GPU Usage", fontsize=12, fontweight="bold")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right", fontsize=10)

        # Set reasonable y-limits
        if total_col in ts_df.columns:
            max_total = ts_df[total_col].max()
            ax.set_ylim(0, max_total * 1.1)

    # Format x-axis for bottom plot only
    axes[-1].xaxis.set_major_locator(mdates.HourLocator(interval=3))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    axes[-1].set_xlabel("Time", fontsize=12)

    plt.xticks(rotation=45)
    plt.suptitle(title, fontsize=14, fontweight="bold")
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        print(f"Saved plot to {save_path}")

    return fig, axes


def create_device_usage_heatmap(
    df: pd.DataFrame, title: str = "GPU Usage by Device Type", save_path: str | None = None
):
    """
    Create a heatmap showing usage percentages by device type and GPU class.

    Args:
        df: Raw GPU data DataFrame
        title: Plot title
        save_path: Optional path to save the plot
    """
    # Get device stats for all devices
    device_stats = calculate_allocation_usage_by_device(df, "", include_all_devices=True)

    # Convert to DataFrame for heatmap
    heatmap_data = []
    for gpu_class, devices in device_stats.items():
        for device_name, stats in devices.items():
            heatmap_data.append(
                {
                    "GPU_Class": gpu_class,
                    "Device": device_name,
                    "Usage_Percent": stats["allocation_usage_percent"],
                    "Avg_Claimed": stats["avg_claimed"],
                    "Avg_Total": stats["avg_total_available"],
                }
            )

    if not heatmap_data:
        print("No device data available for heatmap")
        return None, None

    heatmap_df = pd.DataFrame(heatmap_data)

    # Pivot for heatmap
    pivot_df = heatmap_df.pivot(index="Device", columns="GPU_Class", values="Usage_Percent")

    # Reorder columns: Shared, Priority, Backfill
    desired_order = ["Shared", "Priority", "Backfill"]
    available_columns = [col for col in desired_order if col in pivot_df.columns]
    if available_columns:
        pivot_df = pivot_df[available_columns]

    # Create heatmap
    fig, ax = plt.subplots(figsize=(10, max(6, len(pivot_df) * 0.5)))

    # Always use matplotlib imshow to avoid seaborn white line artifacts
    im = ax.imshow(pivot_df.values, cmap="RdYlBu_r", aspect="auto", vmin=0, vmax=100)

    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Usage Percentage (%)")

    # Set ticks and labels - position them at cell centers
    ax.set_xticks(range(len(pivot_df.columns)))
    ax.set_yticks(range(len(pivot_df.index)))
    ax.set_xticklabels(pivot_df.columns)
    ax.set_yticklabels(pivot_df.index)

    # Remove tick marks and grid lines that cause white bars
    ax.tick_params(which="both", length=0)  # Remove tick marks
    ax.grid(False)  # Ensure no grid

    # Add text annotations
    for i in range(len(pivot_df.index)):
        for j in range(len(pivot_df.columns)):
            value = pivot_df.iloc[i, j]
            if not pd.isna(value):
                ax.text(
                    j,
                    i,
                    f"{value:.1f}",
                    ha="center",
                    va="center",
                    color="white" if value > 50 else "black",
                    fontweight="bold",
                )

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("GPU Class", fontsize=12)
    ax.set_ylabel("Device Type", fontsize=12)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        print(f"Saved plot to {save_path}")

    return fig, ax


def create_utilization_distribution_plot(
    ts_df: pd.DataFrame, title: str = "Usage Distribution", save_path: str | None = None
):
    """
    Create box plots showing the distribution of usage percentages.

    Args:
        ts_df: Time series DataFrame from calculate_time_series_usage()
        title: Plot title
        save_path: Optional path to save the plot
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    # Prepare data for box plot
    usage_data = []
    labels = []

    for gpu_class in ["priority", "shared", "backfill"]:
        usage_col = f"{gpu_class}_usage_percent"
        if usage_col in ts_df.columns:
            usage_data.append(ts_df[usage_col].values)
            labels.append(gpu_class.title())

    if usage_data:
        # Create box plot
        bp = ax.boxplot(
            usage_data,
            labels=labels,
            patch_artist=True,
            boxprops={"facecolor": "lightblue", "alpha": 0.7},
            medianprops={"color": "red", "linewidth": 2},
        )

        # Color the boxes
        colors = ["blue", "green", "red"]
        for patch, color in zip(bp["boxes"], colors[: len(bp["boxes"])], strict=False):
            patch.set_facecolor(color)
            patch.set_alpha(0.3)

    ax.set_ylabel("Usage Percentage (%)", fontsize=12)
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 105)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        print(f"Saved plot to {save_path}")

    return fig, ax


def create_summary_dashboard(df: pd.DataFrame, ts_df: pd.DataFrame, period_str: str, save_path: str | None = None):
    """
    Create a comprehensive dashboard with multiple subplots.

    Args:
        df: Raw GPU data DataFrame
        ts_df: Time series DataFrame
        period_str: String describing the time period
        save_path: Optional path to save the plot
    """
    fig = plt.figure(figsize=(20, 12))

    # Create a grid layout
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)

    # Main timeline plot (top row, spans 2 columns)
    ax1 = fig.add_subplot(gs[0, :2])
    if "priority_usage_percent" in ts_df.columns:
        ax1.plot(
            ts_df["timestamp"],
            ts_df["priority_usage_percent"],
            "b-",
            linewidth=2,
            label="Priority",
            marker="o",
            markersize=3,
        )
    if "shared_usage_percent" in ts_df.columns:
        ax1.plot(
            ts_df["timestamp"],
            ts_df["shared_usage_percent"],
            "g-",
            linewidth=2,
            label="Shared",
            marker="s",
            markersize=3,
        )
    if "backfill_usage_percent" in ts_df.columns:
        ax1.plot(
            ts_df["timestamp"],
            ts_df["backfill_usage_percent"],
            "r-",
            linewidth=2,
            label="Backfill",
            marker="^",
            markersize=3,
        )

    ax1.set_ylabel("Usage %", fontsize=10)
    ax1.set_title("GPU Usage Timeline", fontsize=12, fontweight="bold")
    ax1.grid(True, alpha=0.3)
    ax1.legend(fontsize=9)
    ax1.set_ylim(0, 105)

    # Usage distribution (top right)
    ax2 = fig.add_subplot(gs[0, 2])
    usage_data = []
    labels = []
    for gpu_class in ["priority", "shared", "backfill"]:
        usage_col = f"{gpu_class}_usage_percent"
        if usage_col in ts_df.columns:
            usage_data.append(ts_df[usage_col].values)
            labels.append(gpu_class.title())

    if usage_data:
        bp = ax2.boxplot(usage_data, labels=labels, patch_artist=True)
        colors = ["blue", "green", "red"]
        for patch, color in zip(bp["boxes"], colors[: len(bp["boxes"])], strict=False):
            patch.set_facecolor(color)
            patch.set_alpha(0.3)

    ax2.set_ylabel("Usage %", fontsize=10)
    ax2.set_title("Usage Distribution", fontsize=10, fontweight="bold")
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(0, 105)

    # GPU counts over time (middle row)
    classes = ["priority", "shared", "backfill"]
    colors = ["blue", "green", "red"]

    for i, (gpu_class, color) in enumerate(zip(classes, colors, strict=False)):
        ax = fig.add_subplot(gs[1, i])

        claimed_col = f"{gpu_class}_claimed"
        total_col = f"{gpu_class}_total"

        if claimed_col in ts_df.columns and total_col in ts_df.columns:
            ax.fill_between(ts_df["timestamp"], 0, ts_df[claimed_col], color=color, alpha=0.7, label="Claimed")
            ax.fill_between(
                ts_df["timestamp"], ts_df[claimed_col], ts_df[total_col], color=color, alpha=0.3, label="Unclaimed"
            )

        ax.set_ylabel("GPU Count", fontsize=10)
        ax.set_title(f"{gpu_class.title()} GPUs", fontsize=10, fontweight="bold")
        ax.grid(True, alpha=0.3)
        if i == 0:  # Only show legend on first plot
            ax.legend(fontsize=8)

    # Average usage summary (bottom row)
    ax6 = fig.add_subplot(gs[2, :])

    # Calculate averages
    avg_data = []
    class_names = []
    for gpu_class in ["priority", "shared", "backfill"]:
        usage_col = f"{gpu_class}_usage_percent"
        claimed_col = f"{gpu_class}_claimed"
        total_col = f"{gpu_class}_total"

        if usage_col in ts_df.columns:
            avg_usage = ts_df[usage_col].mean()
            avg_claimed = ts_df[claimed_col].mean() if claimed_col in ts_df.columns else 0
            avg_total = ts_df[total_col].mean() if total_col in ts_df.columns else 0

            avg_data.append(avg_usage)
            class_names.append(f"{gpu_class.title()}\n({avg_claimed:.1f}/{avg_total:.1f})")

    if avg_data:
        bars = ax6.bar(class_names, avg_data, color=["blue", "green", "red"], alpha=0.7)
        ax6.set_ylabel("Average Usage %", fontsize=10)
        ax6.set_title("Average Usage Summary", fontsize=10, fontweight="bold")
        ax6.grid(True, alpha=0.3, axis="y")
        ax6.set_ylim(0, 105)

        # Add value labels on bars
        for bar, value in zip(bars, avg_data, strict=False):
            height = bar.get_height()
            ax6.text(
                bar.get_x() + bar.get_width() / 2.0, height + 1, f"{value:.1f}%", ha="center", va="bottom", fontsize=9
            )

    # Format x-axis for time plots
    for ax in [ax1] + [fig.add_subplot(gs[1, i]) for i in range(3)]:
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=8)

    plt.suptitle(f"GPU Utilization Dashboard - {period_str}", fontsize=16, fontweight="bold")

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        print(f"Saved dashboard to {save_path}")

    return fig


def main(
    hours_back: int = typer.Option(24, help="Number of hours to analyze (default: 24)"),
    host: str = typer.Option("", help="Host name to filter results"),
    db_path: str = typer.Option("gpu_state_2025-06.db", help="Path to SQLite database"),
    bucket_minutes: int = typer.Option(15, help="Time bucket size in minutes"),
    end_time: str | None = typer.Option(
        None, help="End time for analysis (YYYY-MM-DD HH:MM:SS), defaults to latest in DB"
    ),
    output_dir: str = typer.Option("plots", help="Directory to save plots"),
    plot_types: str = typer.Option(
        "all", help="Types of plots to create: all, timeline, counts, heatmap, distribution, dashboard"
    ),
    show_plots: bool = typer.Option(False, help="Display plots interactively"),
    all_devices: bool = typer.Option(False, help="Include all device types in heatmap"),
    gpu_model: str | None = typer.Option(
        None, help="Filter plots to specific GPU model (e.g., 'NVIDIA A100-SXM4-80GB')"
    ),
):
    """
    Create plots showing GPU usage statistics over time in 15-minute intervals.

    This tool creates various visualizations of GPU utilization patterns.
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
    output_path.mkdir(exist_ok=True)

    print("Loading GPU data...")

    # Get data
    df = get_time_filtered_data(db_path, hours_back, parsed_end_time)

    if len(df) == 0:
        print("No data found in the specified time range.")
        return

    # Handle GPU model filtering
    if gpu_model:
        available_models = get_available_gpu_models(df)
        if gpu_model not in available_models:
            print(f"GPU model '{gpu_model}' not found in data.")
            print("Available GPU models:")
            for model in available_models:
                print(f"  - {model}")
            return

        print(f"Filtering data for GPU model: {gpu_model}")
        df = filter_by_gpu_model(df, gpu_model)

        if len(df) == 0:
            print(f"No data found for GPU model '{gpu_model}' in the specified time range.")
            return

    # Calculate time series data
    print("Calculating time series statistics...")
    ts_df = calculate_time_series_usage(df, bucket_minutes, host)

    if len(ts_df) == 0:
        print("No time series data generated.")
        return

    # Create period string for titles
    start_time = df["timestamp"].min()
    end_time_actual = df["timestamp"].max()
    period_str = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time_actual.strftime('%Y-%m-%d %H:%M')}"

    # Add GPU model to title if filtering
    if gpu_model:
        title_suffix = f" - {gpu_model}"
        file_suffix = f"_{gpu_model.replace(' ', '_').replace('-', '_')}"
    else:
        title_suffix = ""
        file_suffix = ""

    print(f"Creating plots for period: {period_str}")
    if gpu_model:
        print(f"Filtered to GPU model: {gpu_model}")
    print(f"Found {len(ts_df)} time intervals")

    # Determine which plots to create
    create_all = plot_types == "all"
    plot_list = (
        plot_types.split(",") if not create_all else ["timeline", "counts", "heatmap", "distribution", "dashboard"]
    )

    # Create plots
    if "timeline" in plot_list or create_all:
        print("Creating timeline plot...")
        fig, ax = create_usage_timeline_plot(
            ts_df,
            f"GPU Usage Timeline - {period_str}{title_suffix}",
            output_path / f"gpu_usage_timeline{file_suffix}.png",
        )
        if show_plots:
            plt.show()
        else:
            plt.close(fig)

    if "counts" in plot_list or create_all:
        print("Creating GPU counts plot...")
        fig, axes = create_gpu_count_plot(
            ts_df,
            f"GPU Counts Over Time - {period_str}{title_suffix}",
            output_path / f"gpu_counts_over_time{file_suffix}.png",
        )
        if show_plots:
            plt.show()
        else:
            plt.close(fig)

    if "heatmap" in plot_list or create_all:
        # Skip heatmap when filtering by GPU model (since it would only show one device type)
        if not gpu_model:
            print("Creating device usage heatmap...")
            fig, ax = create_device_usage_heatmap(
                df, f"GPU Usage by Device Type - {period_str}", output_path / "device_usage_heatmap.png"
            )
            if fig is not None:
                if show_plots:
                    plt.show()
                else:
                    plt.close(fig)
        else:
            print("Skipping heatmap (not applicable when filtering by GPU model)")

    if "distribution" in plot_list or create_all:
        print("Creating usage distribution plot...")
        fig, ax = create_utilization_distribution_plot(
            ts_df,
            f"Usage Distribution - {period_str}{title_suffix}",
            output_path / f"usage_distribution{file_suffix}.png",
        )
        if show_plots:
            plt.show()
        else:
            plt.close(fig)

    if "dashboard" in plot_list or create_all:
        print("Creating summary dashboard...")
        fig = create_summary_dashboard(
            df, ts_df, f"{period_str}{title_suffix}", output_path / f"gpu_usage_dashboard{file_suffix}.png"
        )
        if show_plots:
            plt.show()
        else:
            plt.close(fig)

    print(f"\nPlots saved to: {output_path}")
    print("\nGenerated plots:")
    for plot_file in output_path.glob("*.png"):
        print(f"  - {plot_file.name}")


if __name__ == "__main__":
    typer.run(main)
