#!/usr/bin/env python3
"""
Plot InitialWaitDuration trend over time to detect increases.

Computes a rolling median over a configurable time window (default 6h) on
single-GPU jobs, anchored to submission time (QDate), and overlays a 14-day
rolling baseline median for comparison.

Usage:
    python plot_wait_time_trend.py
    python plot_wait_time_trend.py --csv my_dump.csv --window 2h
    python plot_wait_time_trend.py --window 12h --cap-hours 72
"""

import argparse

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

BASELINE_WINDOW_DAYS = 14
MIN_WAIT_SECONDS = 1
DEFAULT_CAP_HOURS = 48
DEFAULT_WINDOW = "6h"
MIN_PERIODS = 5  # minimum jobs in a rolling window to emit a value


ANCHOR_COLS = {
    "submit": "QDate",
    "start": "JobCurrentStartDate",
}
ANCHOR_LABELS = {
    "submit": "Submission time (QDate)",
    "start": "Job start time (JobCurrentStartDate)",
}


def load_data(csv_path: str, cap_hours: float, anchor: str) -> pd.DataFrame:
    time_col = ANCHOR_COLS[anchor]
    cols = [time_col, "initialwaitduration", "RequestGpus"]
    df = pd.read_csv(csv_path, usecols=cols, low_memory=False)
    total = len(df)
    df = df.dropna(subset=[time_col, "initialwaitduration"])
    df = df[df["initialwaitduration"] >= MIN_WAIT_SECONDS]
    df = df[df["RequestGpus"] == 1]
    valid = len(df)

    df["submitted_at"] = pd.to_datetime(df[time_col], unit="s")
    df["wait_hours"] = df["initialwaitduration"] / 3600

    capped = (df["wait_hours"] > cap_hours).sum()
    df = df[df["wait_hours"] <= cap_hours].copy()
    kept = len(df)

    print(f"Loaded {total:,} rows total")
    print(f"  {valid:,} single-GPU jobs with valid wait data")
    print(f"  {capped:,} dropped as outliers (wait > {cap_hours:.0f} h, " f"{capped / valid * 100:.1f}% of valid rows)")
    print(f"  {kept:,} kept for analysis")
    return df


def rolling_stats(df: pd.DataFrame, window: str) -> pd.DataFrame:
    ts = df.set_index("submitted_at").sort_index()["wait_hours"]

    # Evaluate the rolling median on a regular 30-minute grid to avoid
    # duplicate-timestamp noise from jobs submitted at the same second.
    grid = pd.date_range(ts.index.min().floor("30min"), ts.index.max().ceil("30min"), freq="30min")
    rolling_median = pd.Series(index=grid, dtype=float)
    window_td = pd.Timedelta(window)
    for t in grid:
        window_jobs = ts[(ts.index >= t - window_td) & (ts.index < t)]
        if len(window_jobs) >= MIN_PERIODS:
            rolling_median[t] = window_jobs.median()

    stats = pd.DataFrame({"rolling_median": rolling_median})

    # 14-day baseline: daily medians rolled forward.
    daily = ts.resample("1D").median().dropna()
    baseline = daily.rolling(window=BASELINE_WINDOW_DAYS, min_periods=3, center=False).median()
    stats["baseline"] = baseline.reindex(stats.index, method="ffill")
    stats["ratio"] = stats["rolling_median"] / stats["baseline"]
    return stats


def print_summary(df: pd.DataFrame, stats: pd.DataFrame, recent_days: int) -> None:
    cutoff = df["submitted_at"].max() - pd.Timedelta(days=recent_days)
    recent_df = df[df["submitted_at"] > cutoff]
    hist_df = df[df["submitted_at"] <= cutoff]

    print("\n" + "=" * 60)
    print(f"Summary: recent {recent_days} days vs prior period")
    print("=" * 60)
    if hist_df.empty or recent_df.empty:
        print("Not enough data to compare periods.")
        return

    for label, subset in [("Historical", hist_df), (f"Recent ({recent_days} days)", recent_df)]:
        print(f"\n  {label}")
        print(f"    Median wait : {subset['wait_hours'].median():.2f} h")
        print(f"    P90 wait    : {subset['wait_hours'].quantile(0.90):.2f} h")
        print(f"    Jobs        : {len(subset):,}")

    ratio = recent_df["wait_hours"].median() / hist_df["wait_hours"].median()
    direction = "HIGHER" if ratio > 1 else "lower"
    print(f"\n  Median wait is {ratio:.2f}x {direction} than historical")

    elevated = stats[stats["ratio"] > 2.0].dropna()
    if not elevated.empty:
        # Collapse consecutive elevated grid points into events separated by
        # gaps of more than 6 hours.
        diffs = elevated.index.to_series().diff()
        new_event = pd.concat([pd.Series([True]), diffs.iloc[1:] > pd.Timedelta("6h")])
        new_event.index = elevated.index
        event_starts = elevated.index[new_event]
        print(f"\n  Elevated periods (rolling median > 2× baseline): {len(event_starts)}")
        for t in event_starts:
            window_data = stats.loc[t : t + pd.Timedelta("6h")].dropna()
            peak_ratio = window_data["ratio"].max()
            peak_median = window_data["rolling_median"].max()
            print(f"    {t.strftime('%Y-%m-%d %H:%M')}  " f"peak median={peak_median:.1f}h  ratio={peak_ratio:.1f}x")


def plot(df: pd.DataFrame, stats: pd.DataFrame, window: str, anchor: str, output: str) -> None:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 9), gridspec_kw={"height_ratios": [3, 1]}, sharex=True)

    ax1.plot(
        stats.index,
        stats["rolling_median"],
        color="#2980b9",
        linewidth=1.5,
        label=f"{window} rolling median",
    )
    ax1.plot(
        stats.index,
        stats["baseline"],
        color="#e67e22",
        linewidth=1.8,
        linestyle="-.",
        label=f"{BASELINE_WINDOW_DAYS}-day rolling baseline",
    )

    elevated_mask = stats["ratio"] > 2.0
    if elevated_mask.any():
        ax1.scatter(
            stats.index[elevated_mask],
            stats.loc[elevated_mask, "rolling_median"],
            color="#e74c3c",
            zorder=5,
            s=15,
            label="Rolling median > 2× baseline",
        )

    ax1.set_ylabel("Wait duration (hours)")
    ax1.set_title(f"InitialWaitDuration — single-GPU jobs, {window} rolling median  " f"[anchored to {anchor} time]")
    ax1.legend(loc="upper left", fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(bottom=0)

    # Bottom panel: hourly job count
    hourly_counts = df.set_index("submitted_at").resample("1h")["wait_hours"].count()
    ax2.bar(hourly_counts.index, hourly_counts.values, width=1 / 24, color="#7f8c8d", alpha=0.7)
    ax2.set_ylabel("Jobs / hour")
    ax2.set_xlabel(ANCHOR_LABELS[anchor])
    ax2.grid(True, alpha=0.3)

    x_end = df["submitted_at"].max()
    x_start = x_end - pd.Timedelta(days=14)
    for ax in (ax1, ax2):
        ax.set_xlim(x_start, x_end)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig(output, dpi=150, bbox_inches="tight")
    print(f"\nPlot saved to {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="elasticsearch_dump.csv", help="Input CSV file")
    parser.add_argument("--output", default="wait_time_trend.png", help="Output PNG file")
    parser.add_argument(
        "--window",
        default=DEFAULT_WINDOW,
        help=f"Rolling window size as a pandas offset string, e.g. 2h, 6h, 12h, 1d " f"(default: {DEFAULT_WINDOW})",
    )
    parser.add_argument(
        "--cap-hours",
        type=float,
        default=DEFAULT_CAP_HOURS,
        help=f"Drop jobs with wait > this many hours before analysis (default: {DEFAULT_CAP_HOURS})",
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=7,
        help="Days to treat as 'recent' in the printed summary (default: 7)",
    )
    parser.add_argument(
        "--anchor",
        choices=["submit", "start"],
        default="submit",
        help="Time anchor: 'submit' uses QDate, 'start' uses JobCurrentStartDate (default: submit)",
    )
    args = parser.parse_args()

    df = load_data(args.csv, cap_hours=args.cap_hours, anchor=args.anchor)
    print(f"Date range: {df['submitted_at'].min().date()} to {df['submitted_at'].max().date()}")

    stats = rolling_stats(df, window=args.window)
    print_summary(df, stats, recent_days=args.recent_days)
    plot(df, stats, window=args.window, anchor=args.anchor, output=args.output)


if __name__ == "__main__":
    main()
