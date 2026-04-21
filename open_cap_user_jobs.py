#!/usr/bin/env python3
"""
Open-capacity user job analysis: maximum number of jobs a single user has
running on open-capacity slots per time slice.

Open capacity = primary (non-backfill) slots with PrioritizedProjects empty.
GPU UUIDs are deduplicated per snapshot using the same rank logic as usage_stats.

Usage:
    python open_cap_user_jobs.py
    python open_cap_user_jobs.py --db gpu_state_2026-04.db --top-n 8
    python open_cap_user_jobs.py --start 2026-04-10 --end 2026-04-15
"""

import argparse
import sqlite3

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

RESAMPLE_FREQ = "15min"
TOP_N_DEFAULT = 6


def load_user_jobs(db_path: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """
    Per-snapshot count of claimed open-capacity GPUs per user, after dedup.
    Returns a long-format DataFrame with columns: timestamp, user, jobs.
    """
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        f"""
        WITH ranked AS (
            SELECT
                timestamp, AssignedGPUs, State, Name, PrioritizedProjects, RemoteOwner,
                ROW_NUMBER() OVER (
                    PARTITION BY timestamp, AssignedGPUs
                    ORDER BY CASE
                        WHEN State='Claimed'   AND Name NOT LIKE '%backfill%' THEN 3
                        WHEN State='Unclaimed' AND Name NOT LIKE '%backfill%' THEN 2
                        WHEN State='Claimed'   AND Name     LIKE '%backfill%' THEN 1
                        ELSE 0
                    END DESC
                ) AS rn
            FROM gpu_state
            WHERE AssignedGPUs IS NOT NULL AND AssignedGPUs != ''
              AND timestamp >= '{start_str}' AND timestamp <= '{end_str}'
        ),
        deduped AS (
            SELECT timestamp, AssignedGPUs, State, Name, PrioritizedProjects, RemoteOwner
            FROM ranked WHERE rn = 1
        ),
        open_cap_claimed AS (
            SELECT timestamp, RemoteOwner, AssignedGPUs
            FROM deduped
            WHERE (PrioritizedProjects IS NULL OR PrioritizedProjects = '')
              AND Name NOT LIKE '%backfill%'
              AND State = 'Claimed'
              AND RemoteOwner IS NOT NULL AND RemoteOwner != ''
        )
        SELECT
            timestamp,
            RemoteOwner AS user,
            COUNT(DISTINCT AssignedGPUs) AS jobs
        FROM open_cap_claimed
        GROUP BY timestamp, RemoteOwner
        ORDER BY timestamp, jobs DESC
        """,
        conn,
    )
    conn.close()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # Strip domain suffix for cleaner labels
    df["user"] = df["user"].str.replace(r"@.*", "", regex=True)
    return df


def resample_user_jobs(raw: pd.DataFrame, freq: str = RESAMPLE_FREQ) -> tuple[pd.DataFrame, pd.Series]:
    """
    Bucket raw snapshots into freq-wide windows.
    Returns:
        pivot  — wide DataFrame (index=bucket, columns=user, values=max jobs)
        peak   — Series (index=bucket) of the single-user maximum per bucket
    """
    raw = raw.copy()
    raw["bucket"] = raw["timestamp"].dt.floor(freq)
    # Per bucket + user: take the max snapshot value within the window
    bucketed = raw.groupby(["bucket", "user"])["jobs"].max().reset_index()
    pivot = bucketed.pivot(index="bucket", columns="user", values="jobs").fillna(0)
    peak = pivot.max(axis=1)
    return pivot, peak


def top_users_by_peak(pivot: pd.DataFrame, n: int) -> list[str]:
    return pivot.max().nlargest(n).index.tolist()


def print_summary(pivot: pd.DataFrame, peak: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> None:
    top = pivot.max().sort_values(ascending=False)
    # Approximate GPU-hours: each bucket is RESAMPLE_FREQ wide
    bucket_hours = pd.Timedelta(RESAMPLE_FREQ).total_seconds() / 3600
    gpu_hours = (pivot * bucket_hours).sum().sort_values(ascending=False)

    print("\n" + "=" * 64)
    print("OPEN-CAPACITY USER JOB SUMMARY")
    print(f"{start.date()} → {end.date()}")
    print("=" * 64)
    print(f"\nOverall peak: {int(peak.max())} jobs by a single user" f"  ({peak.idxmax().strftime('%Y-%m-%d %H:%M')})")
    print(f"\n{'User':<22} {'Peak jobs':>10} {'GPU-hours':>12}")
    print("-" * 46)
    for user in top.index[:15]:
        print(f"  {user:<20} {int(top[user]):>10} {gpu_hours[user]:>12.0f}")


def plot(
    pivot: pd.DataFrame,
    peak: pd.Series,
    top_users: list[str],
    output: str,
) -> None:
    fig, (ax1, ax2) = plt.subplots(
        2,
        1,
        figsize=(16, 9),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]},
    )

    colors = plt.cm.tab10.colors  # type: ignore[attr-defined]

    # ── per-user lines ────────────────────────────────────────────────────
    for i, user in enumerate(top_users):
        if user not in pivot.columns:
            continue
        ax1.plot(pivot.index, pivot[user], color=colors[i % len(colors)], lw=1.0, alpha=0.75, label=user)

    ax1.set_ylabel("GPUs running on open capacity")
    ax1.set_title("Max single-user open-capacity GPU utilisation per 15-minute slice")
    ax1.legend(loc="upper left", fontsize=8, ncol=2)
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(bottom=0)

    # ── total claimed open-cap GPUs ───────────────────────────────────────
    total = pivot.sum(axis=1)
    ax2.fill_between(total.index, total.values, color="#7f8c8d", alpha=0.5, label="Total claimed open-cap GPUs")
    ax2.plot(total.index, total.values, color="#7f8c8d", lw=0.8)
    ax2.set_ylabel("Total")
    ax2.set_xlabel("Time")
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc="upper left", fontsize=8)
    ax2.set_ylim(bottom=0)

    for ax in (ax1, ax2):
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig(output, dpi=150, bbox_inches="tight")
    print(f"\nPlot saved to {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="gpu_state_2026-04.db")
    parser.add_argument("--output", default="open_cap_user_jobs.png")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument(
        "--top-n", type=int, default=TOP_N_DEFAULT, help=f"Number of top users to plot (default: {TOP_N_DEFAULT})"
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    db_start, db_end = conn.execute("SELECT MIN(timestamp), MAX(timestamp) FROM gpu_state").fetchone()
    conn.close()
    start = pd.Timestamp(args.start) if args.start else pd.Timestamp(db_start)
    end = pd.Timestamp(args.end) if args.end else pd.Timestamp(db_end)

    raw = load_user_jobs(args.db, start, end)
    pivot, peak = resample_user_jobs(raw)
    top_users = top_users_by_peak(pivot, args.top_n)

    print_summary(pivot, peak, start, end)
    plot(pivot, peak, top_users, args.output)


if __name__ == "__main__":
    main()
