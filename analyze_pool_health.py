#!/usr/bin/env python3
"""
Pool health analysis: detect mismatches between open-capacity slot availability
and job wait times.

Core signal: if open-capacity slots are sitting Unclaimed while single-GPU jobs
are waiting a long time, something is misconfigured (job requirements don't match
available slots, or machines have configuration preventing matches).

Usage:
    python analyze_pool_health.py
    python analyze_pool_health.py --csv my_dump.csv --db gpu_state_2026-04.db
    python analyze_pool_health.py --start 2026-04-01 --end 2026-04-15
"""

import argparse
import sqlite3

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

ROLLING_WINDOW = "12h"
BASELINE_DAYS = 14
MIN_WAIT_S = 1
CAP_WAIT_H = 48
MIN_PERIODS = 5
WAIT_SPIKE_RATIO = 2.0
BURST_JOBS_PER_HOUR = 500

# Rolling correlation between open_unclaimed and rolling-median wait time.
# A sustained positive correlation (idle slots rising WITH wait time) is the
# early-warning signal that normal supply/demand is inverted.
CORR_WINDOW_H = 24  # hours of data per correlation estimate
CORR_MIN_PERIODS = 8  # minimum non-NaN pairs within the window
CORR_FLIP_MIN_H = 4  # minimum consecutive positive-correlation hours to report


# ── Slot metrics ─────────────────────────────────────────────────────────────


def load_slot_metrics(db_path: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """
    Hourly open-capacity unclaimed slot counts using per-snapshot GPU UUID deduplication.

    Matches usage_stats methodology: for each sampled timestamp, deduplicate GPU UUIDs
    by preferring primary-claimed (3) > primary-unclaimed (2) > backfill-claimed (1),
    then count unique unclaimed open-capacity GPUs per snapshot and average per hour.
    This avoids double-counting GPUs that appear in both primary and backfill slot rows.
    """
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        f"""
        WITH ranked AS (
            SELECT
                timestamp,
                AssignedGPUs,
                State,
                Name,
                PrioritizedProjects,
                ROW_NUMBER() OVER (
                    PARTITION BY timestamp, AssignedGPUs
                    ORDER BY
                        CASE
                            WHEN State='Claimed'   AND Name NOT LIKE '%backfill%' THEN 3
                            WHEN State='Unclaimed' AND Name NOT LIKE '%backfill%' THEN 2
                            WHEN State='Claimed'   AND Name     LIKE '%backfill%' THEN 1
                            ELSE 0
                        END DESC
                ) AS rn
            FROM gpu_state
            WHERE AssignedGPUs IS NOT NULL
              AND AssignedGPUs != ''
              AND timestamp >= '{start_str}'
              AND timestamp <= '{end_str}'
        ),
        deduped AS (
            SELECT timestamp, AssignedGPUs, State, Name, PrioritizedProjects
            FROM ranked
            WHERE rn = 1
        ),
        open_cap AS (
            -- Open-capacity primary slots: no PrioritizedProjects, not backfill
            SELECT timestamp, AssignedGPUs, State
            FROM deduped
            WHERE (PrioritizedProjects IS NULL OR PrioritizedProjects = '')
              AND Name NOT LIKE '%backfill%'
        ),
        per_snapshot AS (
            SELECT
                timestamp,
                strftime('%Y-%m-%d %H:00', timestamp) AS hour,
                COUNT(DISTINCT CASE WHEN State = 'Unclaimed' THEN AssignedGPUs END)
                    AS unclaimed_gpus,
                COUNT(DISTINCT CASE WHEN State = 'Claimed'   THEN AssignedGPUs END)
                    AS claimed_gpus
            FROM open_cap
            GROUP BY timestamp
        )
        SELECT
            hour,
            AVG(unclaimed_gpus) AS open_unclaimed,
            AVG(claimed_gpus)   AS open_claimed,
            COUNT(*)            AS sample_count
        FROM per_snapshot
        GROUP BY hour
        ORDER BY hour
        """,
        conn,
    )
    conn.close()

    df["hour"] = pd.to_datetime(df["hour"])
    df = df.set_index("hour").sort_index()
    return df.loc[start:end]


# ── Job metrics ───────────────────────────────────────────────────────────────


def load_jobs(csv_path: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    cols = ["QDate", "JobCurrentStartDate", "initialwaitduration", "RequestGpus", "Owner"]
    df = pd.read_csv(csv_path, usecols=cols, low_memory=False)
    df["submitted_at"] = pd.to_datetime(df["QDate"], unit="s")
    # Anchor wait-time trend on start date: aligns with when the pool state
    # actually caused the delay, and avoids burst-submission spikes collapsing
    # many long-wait jobs onto a single submission timestamp.
    df["started_at"] = pd.to_datetime(df["JobCurrentStartDate"], unit="s")
    df = df[(df["RequestGpus"] == 1) & (df["initialwaitduration"] >= MIN_WAIT_S) & df["started_at"].notna()]
    df["wait_h"] = df["initialwaitduration"] / 3600
    df = df[df["wait_h"] <= CAP_WAIT_H]
    return df[df["started_at"].between(start, end)].copy()


def rolling_wait_stats(jobs: pd.DataFrame) -> pd.DataFrame:
    ts = jobs.set_index("started_at").sort_index()["wait_h"]
    if ts.empty:
        return pd.DataFrame(columns=["rolling_median", "baseline", "ratio"])

    grid = pd.date_range(ts.index.min().floor("30min"), ts.index.max().ceil("30min"), freq="30min")
    window_td = pd.Timedelta(ROLLING_WINDOW)
    medians = pd.Series(index=grid, dtype=float)
    for t in grid:
        w = ts[(ts.index >= t - window_td) & (ts.index < t)]
        if len(w) >= MIN_PERIODS:
            medians[t] = w.median()

    stats = pd.DataFrame({"rolling_median": medians})
    daily = ts.resample("1D").median().dropna()
    baseline = daily.rolling(BASELINE_DAYS, min_periods=3).median()
    stats["baseline"] = baseline.reindex(stats.index, method="ffill")
    stats["ratio"] = stats["rolling_median"] / stats["baseline"]
    return stats


# ── Mismatch detection ────────────────────────────────────────────────────────


def detect_mismatch_periods(
    slots: pd.DataFrame,
    wait_stats: pd.DataFrame,
    baseline_days: int = 5,
) -> pd.DatetimeIndex:
    """
    Return timestamps where open-capacity unclaimed slots are anomalously high
    AND wait times are elevated.

    "Anomalously high" is defined relative to a stable early-window baseline
    (first baseline_days of data) rather than a rolling mean, so sustained
    elevated periods don't get normalised away.  The threshold is
    baseline_mean + 1.5 * baseline_std.
    """
    oc = slots["open_unclaimed"].resample("1h").mean()

    baseline_end = oc.index[0] + pd.Timedelta(days=baseline_days)
    baseline = oc.loc[:baseline_end]
    threshold = baseline.mean() + 1.0 * baseline.std()

    ratio_hourly = wait_stats["ratio"].resample("1h").max()

    idx = oc.index.intersection(ratio_hourly.index)
    mismatch = (oc.loc[idx] > threshold) & (ratio_hourly.loc[idx] > WAIT_SPIKE_RATIO)
    return mismatch[mismatch].index


def detect_bursts(jobs: pd.DataFrame) -> list[dict]:
    hourly = (
        jobs.set_index("submitted_at")
        .resample("1h")
        .agg(
            count=("wait_h", "size"),
            top_owner=("Owner", lambda x: x.value_counts().index[0] if len(x) else ""),
        )
    )
    bursts = hourly[hourly["count"] > BURST_JOBS_PER_HOUR]
    if bursts.empty:
        return []
    gaps = bursts.index.to_series().diff() > pd.Timedelta("6h")
    new_ev = pd.concat([pd.Series([True]), gaps.iloc[1:]])
    new_ev.index = bursts.index
    return [
        {"start": t, "jobs_per_hr": bursts.loc[t, "count"], "owner": bursts.loc[t, "top_owner"]}
        for t in bursts.index[new_ev]
    ]


# ── Rolling correlation ───────────────────────────────────────────────────────


def compute_rolling_corr(
    slots: pd.DataFrame,
    wait_stats: pd.DataFrame,
    window_h: int = CORR_WINDOW_H,
) -> pd.Series:
    """
    Hourly Pearson correlation between open_unclaimed and rolling-median wait time
    over a sliding window.  NaN wait values (sparse overnight hours) are dropped
    within each window; min_periods guards against windows that are too sparse.
    """
    oc = slots["open_unclaimed"].resample("1h").mean()
    wait = wait_stats["rolling_median"].resample("1h").median()
    combined = pd.DataFrame({"oc": oc, "wait": wait})
    return combined["oc"].rolling(window=window_h, min_periods=CORR_MIN_PERIODS).corr(combined["wait"])


def detect_corr_flip_events(rolling_corr: pd.Series) -> list[dict]:
    """
    Return each run of consecutive hours where rolling correlation is positive,
    provided the run lasts at least CORR_FLIP_MIN_H hours.
    """
    positive = (rolling_corr > 0).fillna(False)
    events: list[dict] = []
    in_run = False
    run_start = None

    for t, is_pos in positive.items():
        if is_pos and not in_run:
            in_run = True
            run_start = t
        elif not is_pos and in_run:
            hours = int((t - run_start) / pd.Timedelta("1h"))
            if hours >= CORR_FLIP_MIN_H:
                events.append({"start": run_start, "end": t, "hours": hours})
            in_run = False

    if in_run:
        t = rolling_corr.index[-1]
        hours = int((t - run_start) / pd.Timedelta("1h")) + 1
        if hours >= CORR_FLIP_MIN_H:
            events.append({"start": run_start, "end": t, "hours": hours})

    return events


# ── Report ────────────────────────────────────────────────────────────────────


def print_report(slots, wait_stats, mismatch_times, bursts, corr_flips) -> None:
    oc_start = slots["open_unclaimed"].iloc[:24].mean()
    oc_end = slots["open_unclaimed"].iloc[-24:].mean()

    print("\n" + "=" * 68)
    print("POOL HEALTH REPORT")
    print("=" * 68)
    print(f"\nOpen-capacity unclaimed (avg): " f"{oc_start:.0f} slots at start → {oc_end:.0f} at end")

    if mismatch_times.empty:
        print("\nNo mismatch periods detected.")
    else:
        # Collapse consecutive hours into events
        gaps = mismatch_times.to_series().diff() > pd.Timedelta("2h")
        new_ev = pd.concat([pd.Series([True]), gaps.iloc[1:]])
        new_ev.index = mismatch_times
        starts = mismatch_times[new_ev]

        print("\nMISMATCH PERIODS — open slots unclaimed while jobs wait:")
        for t in starts:
            window = wait_stats.loc[t : t + pd.Timedelta("6h")].dropna()
            peak_wait = window["rolling_median"].max() if not window.empty else float("nan")
            peak_oc = slots["open_unclaimed"].loc[t : t + pd.Timedelta("6h")].max()
            print(
                f"  {t.strftime('%Y-%m-%d %H:%M')}  "
                f"peak wait {peak_wait:.1f}h  "
                f"open unclaimed {peak_oc:.0f} slots"
            )

    print(
        f"\nCORRELATION FLIP EVENTS — oc/wait correlation positive for ≥{CORR_FLIP_MIN_H}h " f"(early-warning signal):"
    )
    if not corr_flips:
        print("  None.")
    for ev in corr_flips:
        print(
            f"  {ev['start'].strftime('%Y-%m-%d %H:%M')} → " f"{ev['end'].strftime('%Y-%m-%d %H:%M')}  ({ev['hours']}h)"
        )

    print(f"\nQUEUE BURSTS (>{BURST_JOBS_PER_HOUR} jobs/hr):")
    if not bursts:
        print("  None.")
    for ev in bursts:
        print(f"  {ev['start'].strftime('%Y-%m-%d %H:%M')}  " f"{ev['jobs_per_hr']:,}/hr  {ev['owner']}")


# ── Plot ──────────────────────────────────────────────────────────────────────


def plot(jobs, slots, wait_stats, mismatch_times, bursts, rolling_corr, output) -> None:
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(
        4,
        1,
        figsize=(16, 13),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 2, 1.5, 1.5]},
    )

    # ── wait time ─────────────────────────────────────────────────────────
    ax1.plot(
        wait_stats.index,
        wait_stats["rolling_median"],
        color="#2980b9",
        lw=1.5,
        label=f"{ROLLING_WINDOW} rolling median",
    )
    ax1.plot(
        wait_stats.index,
        wait_stats["baseline"],
        color="#e67e22",
        lw=1.8,
        ls="-.",
        label=f"{BASELINE_DAYS}-day rolling baseline",
    )

    elevated = wait_stats["ratio"] > WAIT_SPIKE_RATIO
    if elevated.any():
        ax1.scatter(
            wait_stats.index[elevated],
            wait_stats.loc[elevated, "rolling_median"],
            color="#e74c3c",
            zorder=5,
            s=15,
            label=f"Median > {WAIT_SPIKE_RATIO:.0f}× baseline",
        )

    # Shade mismatch periods
    if not mismatch_times.empty:
        gaps = mismatch_times.to_series().diff() > pd.Timedelta("2h")
        new_ev = pd.concat([pd.Series([True]), gaps.iloc[1:]])
        new_ev.index = mismatch_times
        starts = mismatch_times[new_ev].to_list()
        for i, t_start in enumerate(starts):
            nxt = starts[i + 1] if i + 1 < len(starts) else t_start + pd.Timedelta("6h")
            t_end = min(nxt, t_start + pd.Timedelta("6h"))
            for ax in (ax1, ax2):
                ax.axvspan(
                    t_start,
                    t_end,
                    color="#e74c3c",
                    alpha=0.12,
                    label="Mismatch: open slots idle, jobs waiting" if i == 0 else None,
                )

    for ev in bursts:
        ax1.axvline(
            ev["start"],
            color="#8e44ad",
            ls=":",
            lw=1.2,
            alpha=0.7,
            label=f"Burst >{BURST_JOBS_PER_HOUR}/hr" if ev == bursts[0] else None,
        )

    ax1.set_ylabel("Wait duration (hours)")
    ax1.set_title("GPU Pool Health: Open-Capacity Slot Availability vs. Job Wait Time")
    ax1.legend(loc="upper left", fontsize=8, ncol=2)
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(bottom=0)

    # ── open capacity unclaimed ───────────────────────────────────────────
    oc_hourly = slots["open_unclaimed"]
    ax2.fill_between(oc_hourly.index, oc_hourly, color="#27ae60", alpha=0.3, label="Open-capacity unclaimed")
    ax2.plot(oc_hourly.index, oc_hourly, color="#27ae60", lw=1.0)

    # Rolling mean baseline for reference
    roll_mean = oc_hourly.rolling("24h", min_periods=6).mean()
    ax2.plot(roll_mean.index, roll_mean, color="#2c7a4b", lw=1.5, ls="--", alpha=0.8, label="24h rolling mean")

    for ev in bursts:
        ax2.axvline(ev["start"], color="#8e44ad", ls=":", lw=1.2, alpha=0.5)

    ax2.set_ylabel("Open-capacity unclaimed slots")
    ax2.set_ylim(bottom=0)
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc="upper left", fontsize=8)

    # ── jobs started per hour ─────────────────────────────────────────────
    subs = jobs.set_index("started_at").resample("1h")["wait_h"].count()
    ax3.bar(subs.index, subs.values, width=1 / 24, color="#7f8c8d", alpha=0.7)
    for ev in bursts:
        ax3.axvline(
            ev["start"], color="#8e44ad", ls=":", lw=1.2, alpha=0.7, label=ev["owner"] if ev == bursts[0] else None
        )

    ax3.set_ylabel("Jobs started / hr")
    ax3.grid(True, alpha=0.3)
    if bursts:
        ax3.legend(loc="upper left", fontsize=8)

    # ── rolling oc/wait correlation ───────────────────────────────────────
    ax4.plot(
        rolling_corr.index,
        rolling_corr,
        color="#555555",
        lw=1.2,
        label=f"{CORR_WINDOW_H}h rolling correlation (oc vs wait)",
    )
    ax4.axhline(0, color="black", lw=0.8, ls="--")

    # Shade positive (warning) and negative (normal) regions
    ax4.fill_between(
        rolling_corr.index,
        rolling_corr,
        0,
        where=(rolling_corr > 0),
        color="#e74c3c",
        alpha=0.25,
        label="Positive (inverted: warning)",
    )
    ax4.fill_between(
        rolling_corr.index,
        rolling_corr,
        0,
        where=(rolling_corr <= 0),
        color="#27ae60",
        alpha=0.15,
        label="Negative (normal)",
    )

    ax4.set_ylabel("Pearson r")
    ax4.set_xlabel("Date (job start time)")
    ax4.set_ylim(-1, 1)
    ax4.grid(True, alpha=0.3)
    ax4.legend(loc="upper left", fontsize=8)

    # x-axis
    for ax in (ax1, ax2, ax3, ax4):
        ax.set_xlim(jobs["started_at"].min(), jobs["started_at"].max())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Plot saved to {output}")


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="elasticsearch_dump.csv")
    parser.add_argument("--db", default="gpu_state_2026-04.db")
    parser.add_argument("--output", default="pool_health_analysis.png")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    db_start, db_end = conn.execute("SELECT MIN(timestamp), MAX(timestamp) FROM gpu_state").fetchone()
    conn.close()
    start = pd.Timestamp(args.start) if args.start else pd.Timestamp(db_start)
    end = pd.Timestamp(args.end) if args.end else pd.Timestamp(db_end)

    slots = load_slot_metrics(args.db, start, end)

    # Load full CSV history so the 14-day baseline has data before the window.
    # Wait trend is anchored on job start date; burst detection uses submit time.
    all_jobs = load_jobs(args.csv, pd.Timestamp("2000-01-01"), pd.Timestamp("2099-01-01"))
    wait_stats = rolling_wait_stats(all_jobs)
    jobs = all_jobs[all_jobs["started_at"].between(start, end)]
    wait_stats = wait_stats.loc[start:end]

    # Burst detection needs the full submission-time picture, not filtered by start date
    all_submit = pd.read_csv(
        args.csv,
        usecols=["QDate", "initialwaitduration", "RequestGpus", "Owner"],
        low_memory=False,
    )
    all_submit["submitted_at"] = pd.to_datetime(all_submit["QDate"], unit="s")
    all_submit["wait_h"] = all_submit["initialwaitduration"].fillna(0) / 3600
    submit_window = all_submit[(all_submit["RequestGpus"] == 1) & all_submit["submitted_at"].between(start, end)]

    mismatch_times = detect_mismatch_periods(slots, wait_stats)
    bursts = detect_bursts(submit_window)
    rolling_corr = compute_rolling_corr(slots, wait_stats)
    corr_flips = detect_corr_flip_events(rolling_corr)

    print_report(slots, wait_stats, mismatch_times, bursts, corr_flips)
    plot(jobs, slots, wait_stats, mismatch_times, bursts, rolling_corr, args.output)


if __name__ == "__main__":
    main()
