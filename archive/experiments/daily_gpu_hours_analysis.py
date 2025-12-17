#!/usr/bin/env python3
"""
GPU Daily Hours Analysis Script

Analyzes total GPU hours used per day across multiple database files.
Calculates usage based on the 'Claimed' state of GPUs over time, separated by slot type.
"""

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

try:
    from datetime import datetime as dt

    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import numpy as np
    from scipy import stats

    MATPLOTLIB_AVAILABLE = True
    SCIPY_AVAILABLE = True
except ImportError:
    try:
        from datetime import datetime as dt

        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        import numpy as np

        MATPLOTLIB_AVAILABLE = True
        SCIPY_AVAILABLE = False
    except ImportError:
        MATPLOTLIB_AVAILABLE = False
        SCIPY_AVAILABLE = False


def get_gpu_hours_from_db(db_path):
    """Extract GPU usage data from a single database file, separated by slot type."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query to get claimed primary slots with timestamps
        primary_query = """
        SELECT
            timestamp,
            COUNT(*) as claimed_gpus
        FROM gpu_state
        WHERE State = 'Claimed' AND Name LIKE 'slot1_%'
        GROUP BY timestamp
        ORDER BY timestamp
        """

        # Query to get claimed backfill slots with timestamps
        backfill_query = """
        SELECT
            timestamp,
            COUNT(*) as claimed_gpus
        FROM gpu_state
        WHERE State = 'Claimed' AND Name LIKE 'backfill2_%'
        GROUP BY timestamp
        ORDER BY timestamp
        """

        cursor.execute(primary_query)
        primary_results = cursor.fetchall()

        cursor.execute(backfill_query)
        backfill_results = cursor.fetchall()

        conn.close()

        if not primary_results and not backfill_results:
            print(f"Warning: No claimed GPU data found in {db_path}")
            return {}

        # Calculate GPU hours for both slot types
        daily_data = defaultdict(
            lambda: {
                "primary_gpu_hours": 0,
                "primary_claimed_gpus": 0,
                "primary_measurements": 0,
                "backfill_gpu_hours": 0,
                "backfill_claimed_gpus": 0,
                "backfill_measurements": 0,
            }
        )

        # Process primary slots
        for i in range(len(primary_results)):
            timestamp_str, claimed_gpus = primary_results[i]
            current_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            date_str = current_time.date().isoformat()

            # Calculate time interval for this measurement
            if i < len(primary_results) - 1:
                next_timestamp_str = primary_results[i + 1][0]
                next_time = datetime.fromisoformat(next_timestamp_str.replace("Z", "+00:00"))
                interval_hours = (next_time - current_time).total_seconds() / 3600
            else:
                if i > 0:
                    prev_timestamp_str = primary_results[i - 1][0]
                    prev_time = datetime.fromisoformat(prev_timestamp_str.replace("Z", "+00:00"))
                    interval_hours = (current_time - prev_time).total_seconds() / 3600
                else:
                    interval_hours = 0.25

            gpu_hours = claimed_gpus * interval_hours
            daily_data[date_str]["primary_gpu_hours"] += gpu_hours
            daily_data[date_str]["primary_claimed_gpus"] += claimed_gpus
            daily_data[date_str]["primary_measurements"] += 1

        # Process backfill slots
        for i in range(len(backfill_results)):
            timestamp_str, claimed_gpus = backfill_results[i]
            current_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            date_str = current_time.date().isoformat()

            # Calculate time interval for this measurement
            if i < len(backfill_results) - 1:
                next_timestamp_str = backfill_results[i + 1][0]
                next_time = datetime.fromisoformat(next_timestamp_str.replace("Z", "+00:00"))
                interval_hours = (next_time - current_time).total_seconds() / 3600
            else:
                if i > 0:
                    prev_timestamp_str = backfill_results[i - 1][0]
                    prev_time = datetime.fromisoformat(prev_timestamp_str.replace("Z", "+00:00"))
                    interval_hours = (current_time - prev_time).total_seconds() / 3600
                else:
                    interval_hours = 0.25

            gpu_hours = claimed_gpus * interval_hours
            daily_data[date_str]["backfill_gpu_hours"] += gpu_hours
            daily_data[date_str]["backfill_claimed_gpus"] += claimed_gpus
            daily_data[date_str]["backfill_measurements"] += 1

        # Convert to final format with totals
        data = {}
        for date_str, metrics in daily_data.items():
            # Calculate averages
            avg_primary_gpus = (
                metrics["primary_claimed_gpus"] / metrics["primary_measurements"]
                if metrics["primary_measurements"] > 0
                else 0
            )
            avg_backfill_gpus = (
                metrics["backfill_claimed_gpus"] / metrics["backfill_measurements"]
                if metrics["backfill_measurements"] > 0
                else 0
            )

            total_gpu_hours = metrics["primary_gpu_hours"] + metrics["backfill_gpu_hours"]
            total_avg_gpus = avg_primary_gpus + avg_backfill_gpus

            data[date_str] = {
                "claimed_gpus": total_avg_gpus,
                "gpu_hours": total_gpu_hours,
                "primary_gpu_hours": metrics["primary_gpu_hours"],
                "primary_claimed_gpus": avg_primary_gpus,
                "backfill_gpu_hours": metrics["backfill_gpu_hours"],
                "backfill_claimed_gpus": avg_backfill_gpus,
            }

        return data
    except Exception as e:
        print(f"Error processing {db_path}: {e}")
        return {}


def analyze_gpu_usage(db_files):
    """Analyze GPU usage across multiple database files."""
    all_data = defaultdict(
        lambda: {
            "claimed_gpus": 0,
            "gpu_hours": 0,
            "db_count": 0,
            "primary_gpu_hours": 0,
            "primary_claimed_gpus": 0,
            "backfill_gpu_hours": 0,
            "backfill_claimed_gpus": 0,
        }
    )

    for db_file in db_files:
        if not Path(db_file).exists():
            print(f"Warning: Database file {db_file} not found")
            continue

        print(f"Processing {db_file}...")
        data = get_gpu_hours_from_db(db_file)

        for date_str, metrics in data.items():
            # Sum up data from multiple databases
            all_data[date_str]["claimed_gpus"] += metrics["claimed_gpus"]
            all_data[date_str]["gpu_hours"] += metrics["gpu_hours"]
            all_data[date_str]["primary_gpu_hours"] += metrics["primary_gpu_hours"]
            all_data[date_str]["primary_claimed_gpus"] += metrics["primary_claimed_gpus"]
            all_data[date_str]["backfill_gpu_hours"] += metrics["backfill_gpu_hours"]
            all_data[date_str]["backfill_claimed_gpus"] += metrics["backfill_claimed_gpus"]
            all_data[date_str]["db_count"] += 1

    if not all_data:
        print("No data found in any database files")
        return None

    # Convert to sorted list of tuples with slot type breakdown
    daily_summary = []
    for date_str in sorted(all_data.keys()):
        metrics = all_data[date_str]
        daily_summary.append(
            (
                date_str,
                metrics["claimed_gpus"],
                metrics["gpu_hours"],
                metrics["primary_gpu_hours"],
                metrics["primary_claimed_gpus"],
                metrics["backfill_gpu_hours"],
                metrics["backfill_claimed_gpus"],
            )
        )

    return daily_summary


def calculate_monthly_stats(daily_data):
    """Calculate monthly statistics from daily data."""
    monthly = defaultdict(
        lambda: {"total_hours": 0, "days": 0, "hours_list": [], "primary_hours": 0, "backfill_hours": 0}
    )

    for row in daily_data:
        date_str = row[0]
        gpu_hours = row[2]
        primary_hours = row[3] if len(row) > 3 else 0
        backfill_hours = row[5] if len(row) > 5 else 0

        month = date_str[:7]  # Extract YYYY-MM
        monthly[month]["total_hours"] += gpu_hours
        monthly[month]["primary_hours"] += primary_hours
        monthly[month]["backfill_hours"] += backfill_hours
        monthly[month]["days"] += 1
        monthly[month]["hours_list"].append(gpu_hours)

    # Calculate averages
    for month in monthly:
        hours_list = monthly[month]["hours_list"]
        monthly[month]["avg_hours"] = monthly[month]["total_hours"] / monthly[month]["days"]

    return dict(monthly)


def calculate_linear_trend(dates, values):
    """Calculate linear trend statistics for the data."""

    # Convert dates to ordinal numbers for regression
    date_nums = [datetime.strptime(date, "%Y-%m-%d").toordinal() for date in dates]

    if SCIPY_AVAILABLE:
        # Use scipy for full statistics
        slope, intercept, r_value, p_value, std_err = stats.linregress(date_nums, values)
        r_squared = r_value**2
    else:
        # Use numpy for basic linear regression
        coefficients = [0, 0]  # Fallback when numpy not available
        slope = coefficients[0]
        intercept = coefficients[1]

        # Calculate correlation coefficient manually
        r_value = 0  # Fallback when numpy not available
        # r_value set above
        r_squared = r_value**2

        # Set p_value and std_err to None when scipy not available
        p_value = None
        std_err = None

    # Calculate trend line values
    trend_line = [slope * x + intercept for x in date_nums]

    # Calculate trend per day and per month
    trend_per_day = slope
    trend_per_month = slope * 30.44  # Average days per month
    trend_per_year = slope * 365.25

    return {
        "slope": slope,
        "intercept": intercept,
        "r_squared": r_squared,
        "r_value": r_value,
        "p_value": p_value,
        "std_err": std_err,
        "trend_line": trend_line,
        "trend_per_day": trend_per_day,
        "trend_per_month": trend_per_month,
        "trend_per_year": trend_per_year,
        "date_nums": date_nums,
    }


def format_trend_stats(trend_stats, data_name="GPU Hours"):
    """Format trend statistics for display."""
    if not trend_stats:
        return "Trend analysis requires scipy package"

    direction = "increasing" if trend_stats["slope"] > 0 else "decreasing"
    strength = ""
    r_sq = trend_stats["r_squared"]

    if r_sq >= 0.7:
        strength = "strong"
    elif r_sq >= 0.3:
        strength = "moderate"
    else:
        strength = "weak"

    if trend_stats["p_value"] is not None:
        significance = "significant" if trend_stats["p_value"] < 0.05 else "not significant"
        p_value_text = f", {significance} (p = {trend_stats['p_value']:.3f})"
    else:
        p_value_text = " (scipy required for significance testing)"

    return (
        f"Linear Trend: {direction} at {abs(trend_stats['trend_per_day']):.1f} {data_name.lower()}/day "
        f"({abs(trend_stats['trend_per_month']):.0f}/month, {abs(trend_stats['trend_per_year']):.0f}/year)\n"
        f"Trend Strength: {strength} (R² = {r_sq:.3f}){p_value_text}"
    )


def aggregate_to_weekly(daily_data):
    """Aggregate daily data to weekly data for clearer trend visualization."""
    if not daily_data:
        return []

    # Group data by week (Monday as start of week)
    weekly_data = defaultdict(
        lambda: {
            "dates": [],
            "gpu_hours": [],
            "primary_hours": [],
            "backfill_hours": [],
            "claimed_gpus": [],
            "primary_gpus": [],
            "backfill_gpus": [],
        }
    )

    for row in daily_data:
        date_str = row[0]
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")

        # Get Monday of the week (ISO week)
        days_since_monday = date_obj.weekday()
        monday = date_obj - timedelta(days=days_since_monday)
        week_key = monday.strftime("%Y-%m-%d")

        # Add data to weekly group
        weekly_data[week_key]["dates"].append(date_str)
        weekly_data[week_key]["gpu_hours"].append(row[2])
        weekly_data[week_key]["claimed_gpus"].append(row[1])

        # Handle slot breakdown if available
        if len(row) > 5:
            weekly_data[week_key]["primary_hours"].append(row[3])
            weekly_data[week_key]["primary_gpus"].append(row[4])
            weekly_data[week_key]["backfill_hours"].append(row[5])
            weekly_data[week_key]["backfill_gpus"].append(row[6])

    # Calculate weekly averages/totals
    weekly_summary = []
    for week_start in sorted(weekly_data.keys()):
        week_info = weekly_data[week_start]

        # Calculate totals and averages
        total_gpu_hours = sum(week_info["gpu_hours"])
        avg_claimed_gpus = sum(week_info["claimed_gpus"]) / len(week_info["claimed_gpus"])

        if week_info["primary_hours"]:  # Has slot breakdown
            total_primary_hours = sum(week_info["primary_hours"])
            total_backfill_hours = sum(week_info["backfill_hours"])
            avg_primary_gpus = sum(week_info["primary_gpus"]) / len(week_info["primary_gpus"])
            avg_backfill_gpus = sum(week_info["backfill_gpus"]) / len(week_info["backfill_gpus"])

            weekly_summary.append(
                (
                    week_start,
                    avg_claimed_gpus,
                    total_gpu_hours,
                    total_primary_hours,
                    avg_primary_gpus,
                    total_backfill_hours,
                    avg_backfill_gpus,
                )
            )
        else:
            weekly_summary.append((week_start, avg_claimed_gpus, total_gpu_hours))

    return weekly_summary


def print_summary_stats(daily_data):
    """Print summary statistics."""
    print("\n" + "=" * 60)
    print("GPU USAGE SUMMARY STATISTICS")
    print("=" * 60)

    if not daily_data:
        print("No data to analyze")
        return

    dates = [row[0] for row in daily_data]
    gpu_hours = [row[2] for row in daily_data]
    primary_hours = [row[3] for row in daily_data] if len(daily_data[0]) > 3 else []
    backfill_hours = [row[5] for row in daily_data] if len(daily_data[0]) > 5 else []

    print(f"Date Range: {min(dates)} to {max(dates)}")
    print(f"Total Days: {len(daily_data)}")
    print(f"Total GPU Hours: {sum(gpu_hours):,.0f}")

    if primary_hours and backfill_hours:
        print(f"  Primary Slot Hours: {sum(primary_hours):,.0f} ({sum(primary_hours)/sum(gpu_hours)*100:.1f}%)")
        print(f"  Backfill Slot Hours: {sum(backfill_hours):,.0f} ({sum(backfill_hours)/sum(gpu_hours)*100:.1f}%)")

    print(f"Average GPU Hours per Day: {sum(gpu_hours) / len(gpu_hours):.1f}")
    print(f"Peak GPU Hours in a Day: {max(gpu_hours):.0f}")
    print(f"Min GPU Hours in a Day: {min(gpu_hours):.0f}")

    # Trend analysis
    total_trend = calculate_linear_trend(dates, gpu_hours)
    if total_trend:
        print("\nTREND ANALYSIS:")
        print("-" * 40)
        print(format_trend_stats(total_trend, "GPU Hours"))

        if primary_hours and backfill_hours:
            primary_trend = calculate_linear_trend(dates, primary_hours)
            backfill_trend = calculate_linear_trend(dates, backfill_hours)

            if primary_trend:
                print("\nPrimary Slots:")
                print(format_trend_stats(primary_trend, "Primary Hours"))

            if backfill_trend:
                print("\nBackfill Slots:")
                print(format_trend_stats(backfill_trend, "Backfill Hours"))

    # Monthly breakdown
    monthly_stats = calculate_monthly_stats(daily_data)
    print("\nMONTHLY BREAKDOWN:")
    print("-" * 80)
    for month in sorted(monthly_stats.keys()):
        stats = monthly_stats[month]
        print(
            f"{month}: {stats['total_hours']:,.0f} total hours "
            f"({stats['avg_hours']:.1f} avg/day, {stats['days']} days)"
        )
        if "primary_hours" in stats and "backfill_hours" in stats:
            print(
                f"        Primary: {stats['primary_hours']:,.0f} hours, "
                f"Backfill: {stats['backfill_hours']:,.0f} hours"
            )


def create_plots(daily_data, output_dir=None, show_linear_trend=False, transition_date=None):
    """Create various plots to visualize GPU usage data, separated by slot type.

    Args:
        daily_data: List of daily usage data
        output_dir: Directory to save plots
        show_linear_trend: Whether to show linear trend lines
        transition_date: Date string (YYYY-MM-DD) to split backfill slots into before/after periods
    """
    if not MATPLOTLIB_AVAILABLE:
        print("Error: matplotlib is required for plotting. Install it with: pip install matplotlib")
        return

    if not daily_data:
        print("No data available for plotting")
        return

    # Convert data for plotting

    dates = [dt.strptime(row[0], "%Y-%m-%d") for row in daily_data]
    gpu_hours = [row[2] for row in daily_data]
    claimed_gpus = [row[1] for row in daily_data]

    # Check if we have slot type breakdown data
    has_slot_breakdown = len(daily_data[0]) > 5
    if has_slot_breakdown:
        primary_hours = [row[3] for row in daily_data]
        backfill_hours = [row[5] for row in daily_data]

        # Split backfill data by transition date if provided
        if transition_date:
            transition_dt = dt.strptime(transition_date, "%Y-%m-%d")
            backfill_before = [
                hours for date, hours in zip(dates, backfill_hours, strict=False) if date < transition_dt
            ]
            backfill_after = [
                hours for date, hours in zip(dates, backfill_hours, strict=False) if date >= transition_dt
            ]
        else:
            backfill_before = None
            backfill_after = None

    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle("GPU Usage Analysis - Primary vs Backfill Slots", fontsize=16, fontweight="bold")

    # Plot 1: Daily GPU Hours over time by slot type
    if has_slot_breakdown:
        ax1.plot(dates, primary_hours, color="#2E86AB", linewidth=2, marker="o", markersize=3, label="Primary Slots")
        ax1.plot(dates, backfill_hours, color="#A23B72", linewidth=2, marker="s", markersize=3, label="Backfill Slots")
        ax1.plot(dates, gpu_hours, color="#404040", linestyle="--", linewidth=1, alpha=0.8, label="Total")

        # Add linear trend lines if requested
        if show_linear_trend:
            # Use daily data for trend calculation but plot on weekly dates
            daily_dates = [row[0] for row in daily_data]
            daily_primary = [row[3] for row in daily_data] if len(daily_data[0]) > 5 else []
            daily_backfill = [row[5] for row in daily_data] if len(daily_data[0]) > 5 else []
            daily_total = [row[2] for row in daily_data]

            primary_trend = calculate_linear_trend(daily_dates, daily_primary) if daily_primary else None
            backfill_trend = calculate_linear_trend(daily_dates, daily_backfill) if daily_backfill else None
            total_trend = calculate_linear_trend(daily_dates, daily_total)

            if primary_trend:
                ax1.plot(
                    dates,
                    primary_trend["trend_line"],
                    color="#2E86AB",
                    linestyle=":",
                    linewidth=2,
                    alpha=0.8,
                    label=f'Primary Trend (R²={primary_trend["r_squared"]:.3f})',
                )
            if backfill_trend:
                ax1.plot(
                    dates,
                    backfill_trend["trend_line"],
                    color="#A23B72",
                    linestyle=":",
                    linewidth=2,
                    alpha=0.8,
                    label=f'Backfill Trend (R²={backfill_trend["r_squared"]:.3f})',
                )
            if total_trend:
                ax1.plot(
                    dates,
                    total_trend["trend_line"],
                    color="#404040",
                    linestyle=":",
                    linewidth=2,
                    alpha=0.8,
                    label=f'Total Trend (R²={total_trend["r_squared"]:.3f})',
                )

        ax1.legend()
    else:
        ax1.plot(dates, gpu_hours, color="#2E86AB", linewidth=2, marker="o", markersize=4)

        # Add linear trend line if requested
        if show_linear_trend:
            daily_dates = [row[0] for row in daily_data]
            daily_total = [row[2] for row in daily_data]
            total_trend = calculate_linear_trend(daily_dates, daily_total)
            if total_trend:
                ax1.plot(
                    dates,
                    total_trend["trend_line"],
                    color="#C9302C",
                    linestyle=":",
                    linewidth=2,
                    alpha=0.8,
                    label=f'Linear Trend (R²={total_trend["r_squared"]:.3f})',
                )
                ax1.legend()

    ax1.set_title("Daily GPU Hours Over Time")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("GPU Hours")
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    # Plot 2: Stacked area chart showing slot type breakdown
    if has_slot_breakdown:
        ax2.fill_between(dates, 0, primary_hours, alpha=0.8, color="#2E86AB", label="Primary Slots")
        ax2.fill_between(
            dates,
            primary_hours,
            [p + b for p, b in zip(primary_hours, backfill_hours, strict=False)],
            alpha=0.8,
            color="#A23B72",
            label="Backfill Slots",
        )
        ax2.legend()
        ax2.set_title("GPU Hours by Slot Type (Stacked)")
    else:
        ax2.plot(dates, claimed_gpus, color="#F18F01", linewidth=2, marker="s", markersize=4)
        ax2.set_title("Average Claimed GPUs Over Time")

    ax2.set_xlabel("Date")
    ax2.set_ylabel("GPU Hours" if has_slot_breakdown else "Number of GPUs")
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

    # Plot 3: Distribution histograms
    if has_slot_breakdown:
        if transition_date and backfill_before and backfill_after:
            # Show split backfill slots
            ax3.hist(
                [primary_hours, backfill_before, backfill_after],
                bins=20,
                alpha=0.7,
                label=["Primary Slots", f"Backfill (Before {transition_date})", f"Backfill (After {transition_date})"],
                color=["#2E86AB", "#A23B72", "#F18F01"],
                edgecolor="#404040",
            )
            ax3.legend(fontsize=9)
            ax3.set_title(f"Distribution of Daily GPU Hours by Slot Type\n(Backfill split at {transition_date})")
        else:
            # Show original combined backfill
            ax3.hist(
                [primary_hours, backfill_hours],
                bins=20,
                alpha=0.8,
                label=["Primary Slots", "Backfill Slots"],
                color=["#2E86AB", "#A23B72"],
                edgecolor="#404040",
            )
            ax3.legend()
            ax3.set_title("Distribution of Daily GPU Hours by Slot Type")
    else:
        ax3.hist(gpu_hours, bins=20, color="#5DADE2", alpha=0.8, edgecolor="#404040")
        ax3.set_title("Distribution of Daily GPU Hours")

    ax3.set_xlabel("GPU Hours")
    ax3.set_ylabel("Frequency")
    ax3.grid(True, alpha=0.3)

    # Plot 4: Monthly totals by slot type
    monthly_stats = calculate_monthly_stats(daily_data)
    months = sorted(monthly_stats.keys())

    if has_slot_breakdown:
        primary_monthly = [monthly_stats[month]["primary_hours"] for month in months]
        backfill_monthly = [monthly_stats[month]["backfill_hours"] for month in months]

        width = 0.35
        x = range(len(months))
        ax4.bar(
            [i - width / 2 for i in x],
            primary_monthly,
            width,
            label="Primary Slots",
            color="#2E86AB",
            alpha=0.8,
            edgecolor="#404040",
        )
        ax4.bar(
            [i + width / 2 for i in x],
            backfill_monthly,
            width,
            label="Backfill Slots",
            color="#A23B72",
            alpha=0.8,
            edgecolor="#404040",
        )
        ax4.set_xticks(x)
        ax4.set_xticklabels(months)
        ax4.legend()
        ax4.set_title("Monthly GPU Hours by Slot Type")
    else:
        monthly_totals = [monthly_stats[month]["total_hours"] for month in months]
        ax4.bar(months, monthly_totals, color="#F18F01", alpha=0.8, edgecolor="#404040")
        ax4.set_title("Monthly GPU Hours Totals")

    ax4.set_xlabel("Month")
    ax4.set_ylabel("Total GPU Hours")
    ax4.grid(True, alpha=0.3)
    plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()

    # Always save the plot
    if output_dir:
        output_path = Path(output_dir) / "gpu_usage_analysis.png"
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = Path("gpu_usage_analysis.png")

    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Plot saved to: {output_path}")

    # Show plot only if display is available (optional)
    try:
        plt.show()
    except:
        print("Display not available - plot saved to file only")


def create_trend_plot(daily_data, output_dir=None, show_linear_trend=False):
    """Create a detailed trend plot with moving average, separated by slot type."""
    if not MATPLOTLIB_AVAILABLE:
        print("Error: matplotlib is required for plotting. Install it with: pip install matplotlib")
        return

    if not daily_data:
        print("No data available for plotting")
        return

    # Use weekly data for clearer trends
    daily_data = aggregate_to_weekly(daily_data)

    dates = [dt.strptime(row[0], "%Y-%m-%d") for row in daily_data]
    gpu_hours = [row[2] for row in daily_data]

    # Check if we have slot type breakdown data
    has_slot_breakdown = len(daily_data[0]) > 5 if daily_data else False
    if has_slot_breakdown:
        primary_hours = [row[3] for row in daily_data]
        backfill_hours = [row[5] for row in daily_data]

    # Calculate 4-week moving average (since we're using weekly data)
    window_size = 4

    def moving_average(data):
        moving_avg = []
        for i in range(len(data)):
            start_idx = max(0, i - window_size + 1)
            avg = sum(data[start_idx : i + 1]) / (i - start_idx + 1)
            moving_avg.append(avg)
        return moving_avg

    total_moving_avg = moving_average(gpu_hours)

    plt.figure(figsize=(14, 8))

    if has_slot_breakdown:
        primary_moving_avg = moving_average(primary_hours)
        backfill_moving_avg = moving_average(backfill_hours)

        # Plot raw data with transparency
        plt.plot(dates, primary_hours, color="#2E86AB", alpha=0.3, linewidth=1, label="Primary Slots (weekly)")
        plt.plot(dates, backfill_hours, color="#A23B72", alpha=0.3, linewidth=1, label="Backfill Slots (weekly)")
        plt.plot(dates, gpu_hours, color="#404040", alpha=0.3, linewidth=1, label="Total (weekly)")

        # Plot moving averages
        plt.plot(dates, primary_moving_avg, color="#2E86AB", linewidth=2, label="Primary Slots (4-week avg)")
        plt.plot(dates, backfill_moving_avg, color="#A23B72", linewidth=2, label="Backfill Slots (4-week avg)")
        plt.plot(dates, total_moving_avg, color="#404040", linewidth=2, label="Total (4-week avg)")

        # Add linear trend lines if requested
        if show_linear_trend:
            # Use daily data for trend calculation but plot on weekly dates
            daily_dates = [row[0] for row in daily_data]
            daily_primary = [row[3] for row in daily_data] if len(daily_data[0]) > 5 else []
            daily_backfill = [row[5] for row in daily_data] if len(daily_data[0]) > 5 else []
            daily_total = [row[2] for row in daily_data]

            primary_trend = calculate_linear_trend(daily_dates, daily_primary) if daily_primary else None
            backfill_trend = calculate_linear_trend(daily_dates, daily_backfill) if daily_backfill else None
            total_trend = calculate_linear_trend(daily_dates, daily_total)

            # Calculate trend values for weekly dates
            if primary_trend and dates:
                weekly_date_nums = [datetime.strptime(row[0], "%Y-%m-%d").toordinal() for row in daily_data]
                primary_trend_values = [
                    primary_trend["slope"] * x + primary_trend["intercept"] for x in weekly_date_nums
                ]
                plt.plot(
                    dates,
                    primary_trend_values,
                    color="#2E86AB",
                    linestyle="--",
                    linewidth=3,
                    alpha=0.9,
                    label=f'Primary Linear Trend (R²={primary_trend["r_squared"]:.3f})',
                )
            if backfill_trend and dates:
                weekly_date_nums = [datetime.strptime(row[0], "%Y-%m-%d").toordinal() for row in daily_data]
                backfill_trend_values = [
                    backfill_trend["slope"] * x + backfill_trend["intercept"] for x in weekly_date_nums
                ]
                plt.plot(
                    dates,
                    backfill_trend_values,
                    color="#A23B72",
                    linestyle="--",
                    linewidth=3,
                    alpha=0.9,
                    label=f'Backfill Linear Trend (R²={backfill_trend["r_squared"]:.3f})',
                )
            if total_trend and dates:
                weekly_date_nums = [datetime.strptime(row[0], "%Y-%m-%d").toordinal() for row in daily_data]
                total_trend_values = [total_trend["slope"] * x + total_trend["intercept"] for x in weekly_date_nums]
                plt.plot(
                    dates,
                    total_trend_values,
                    color="#404040",
                    linestyle="--",
                    linewidth=3,
                    alpha=0.9,
                    label=f'Total Linear Trend (R²={total_trend["r_squared"]:.3f})',
                )

        plt.title("GPU Usage Trend Analysis - Primary vs Backfill Slots", fontsize=14, fontweight="bold")
    else:
        plt.plot(dates, gpu_hours, color="#2E86AB", alpha=0.6, linewidth=1, label="Weekly GPU Hours")
        plt.plot(dates, total_moving_avg, color="#C9302C", linewidth=2, label="4-week Moving Average")

        # Add linear trend line if requested
        if show_linear_trend:
            daily_dates = [row[0] for row in daily_data]
            daily_total = [row[2] for row in daily_data]
            total_trend = calculate_linear_trend(daily_dates, daily_total)
            if total_trend and dates:
                weekly_date_nums = [datetime.strptime(row[0], "%Y-%m-%d").toordinal() for row in daily_data]
                total_trend_values = [total_trend["slope"] * x + total_trend["intercept"] for x in weekly_date_nums]
                plt.plot(
                    dates,
                    total_trend_values,
                    color="#404040",
                    linestyle="--",
                    linewidth=3,
                    alpha=0.9,
                    label=f'Linear Trend (R²={total_trend["r_squared"]:.3f})',
                )

        plt.title("GPU Usage Trend Analysis", fontsize=14, fontweight="bold")

    plt.xlabel("Week Starting")
    plt.ylabel("GPU Hours (Weekly Total)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.gca().xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.xticks(rotation=45)

    plt.tight_layout()

    # Always save the plot
    if output_dir:
        output_path = Path(output_dir) / "gpu_usage_trend.png"
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = Path("gpu_usage_trend.png")

    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Trend plot saved to: {output_path}")

    # Show plot only if display is available (optional)
    try:
        plt.show()
    except:
        print("Display not available - plot saved to file only")


def main():
    parser = argparse.ArgumentParser(description="Analyze daily GPU hours from multiple database files")
    parser.add_argument(
        "--databases",
        "-d",
        nargs="+",
        default=[
            "gpu_state_2025-06.db",
            "gpu_state_2025-07.db",
            "gpu_state_2025-08.db",
            "gpu_state_2025-09.db",
            "gpu_state_2025-10.db",
        ],
        help="Database files to analyze",
    )
    parser.add_argument("--output", "-o", help="Output CSV file path")
    parser.add_argument("--detailed", action="store_true", help="Show detailed daily breakdown")
    parser.add_argument("--plot", action="store_true", help="Generate and save visualization plots as PNG")
    parser.add_argument(
        "--trend", action="store_true", help="Generate and save trend analysis plot with moving average as PNG"
    )
    parser.add_argument(
        "--linear-trend",
        action="store_true",
        help="Add linear trend lines to plots and show trend statistics (requires matplotlib for plot lines)",
    )
    parser.add_argument("--plot-output", help="Directory to save plot images")
    parser.add_argument(
        "--transition-date",
        default=None,
        help="Date (YYYY-MM-DD) to split backfill slots into before/after periods. If not specified, shows combined backfill. Example: 2025-09-15",
    )

    args = parser.parse_args()

    print("GPU Daily Hours Analysis - Primary vs Backfill Slots")
    print("=" * 55)
    print(f"Analyzing databases: {', '.join(args.databases)}")

    # Analyze the data
    daily_summary = analyze_gpu_usage(args.databases)
    if daily_summary is None:
        sys.exit(1)

    # Print summary statistics
    print_summary_stats(daily_summary)

    # Show detailed breakdown if requested
    if args.detailed:
        print("\nDETAILED DAILY BREAKDOWN:")
        print("-" * 90)
        if len(daily_summary[0]) > 5:  # Has slot breakdown
            print(f"{'Date':<12} {'Total GPUs':<11} {'Total Hours':<12} {'Primary Hours':<13} {'Backfill Hours':<14}")
            print("-" * 90)
            for row in daily_summary:
                date_str, claimed_gpus, gpu_hours, primary_hours, _, backfill_hours, _ = row
                print(
                    f"{date_str:<12} {claimed_gpus:<11.1f} {gpu_hours:<12.0f} {primary_hours:<13.0f} {backfill_hours:<14.0f}"
                )
        else:
            print(f"{'Date':<12} {'Avg GPUs':<10} {'GPU Hours':<10}")
            print("-" * 60)
            for date_str, claimed_gpus, gpu_hours in daily_summary:
                print(f"{date_str:<12} {claimed_gpus:<10.1f} {gpu_hours:<10.0f}")

    # Save to CSV if requested
    if args.output:
        with open(args.output, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            if len(daily_summary[0]) > 5:  # Has slot breakdown
                writer.writerow(
                    [
                        "date",
                        "claimed_gpus",
                        "gpu_hours",
                        "primary_gpu_hours",
                        "primary_claimed_gpus",
                        "backfill_gpu_hours",
                        "backfill_claimed_gpus",
                    ]
                )
            else:
                writer.writerow(["date", "claimed_gpus", "gpu_hours"])

            for row in daily_summary:
                writer.writerow(row)
        print(f"\nDetailed results saved to: {args.output}")

    # Generate plots if requested
    if args.plot:
        try:
            create_plots(daily_summary, args.plot_output, args.linear_trend, args.transition_date)
        except ImportError:
            print("Error: matplotlib is required for plotting. Install it with: pip install matplotlib")
        except Exception as e:
            print(f"Error generating plots: {e}")

    if args.trend:
        try:
            create_trend_plot(daily_summary, args.plot_output, args.linear_trend)
        except ImportError:
            print("Error: matplotlib is required for plotting. Install it with: pip install matplotlib")
        except Exception as e:
            print(f"Error generating trend plot: {e}")


if __name__ == "__main__":
    main()
