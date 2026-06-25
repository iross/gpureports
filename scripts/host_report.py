#!/usr/bin/env python3
"""
Host-level GPU usage report generator.

Generates a Markdown report with plots for a specific CHTC GPU host,
covering: who has been running, job pressure, and fairshare analysis.

Run from the project root:
    python scripts/host_report.py --host isye --project ISyE
    python scripts/host_report.py --host voyles --project Voyles --hours-back 336
"""

import datetime
import sqlite3
import sys
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import typer

sys.path.insert(0, str(Path(__file__).parent.parent))

from gpu_utils import filter_df_enhanced, get_most_recent_database, load_chtc_owned_hosts
from stats_data import get_preprocessed_dataframe, get_time_filtered_data

try:
    import seaborn as sns  # noqa: F401

    plt.style.use("seaborn-v0_8")
except ImportError:
    plt.style.use("ggplot")

app = typer.Typer(add_completion=False)

BUCKET_HOURS = 0.25  # 15 minutes in hours


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def _is_excluded(user: str, exclude: set[str]) -> bool:
    """Return True if the user's short name (before @) is in the exclusion set."""
    return bool(pd.isna(user)) or not str(user).strip() or user.split("@")[0] in exclude


def calculate_user_hours(df: pd.DataFrame, exclude: set[str] | None = None) -> dict[str, float]:
    """GPU-hours per user from a claimed-slot DataFrame (any slot type)."""
    exclude = exclude or set()
    if df.empty:
        return {}
    df = get_preprocessed_dataframe(df)
    claimed = df[df["State"] == "Claimed"]
    if claimed.empty:
        return {}
    user_hours: dict[str, float] = {}
    for (_, user), grp in claimed.groupby(["15min_bucket", "RemoteOwner"]):
        if _is_excluded(user, exclude):
            continue
        gpus = grp["AssignedGPUs"].nunique()
        user_hours[user] = user_hours.get(user, 0.0) + gpus * BUCKET_HOURS
    return user_hours


def build_user_timeseries(df: pd.DataFrame, exclude: set[str] | None = None) -> pd.DataFrame:
    """15-min time series of GPU count per user. Returns columns: [15min_bucket, user, gpus]."""
    exclude = exclude or set()
    if df.empty:
        return pd.DataFrame(columns=["15min_bucket", "user", "gpus"])
    df = get_preprocessed_dataframe(df)
    claimed = df[df["State"] == "Claimed"]
    if claimed.empty:
        return pd.DataFrame(columns=["15min_bucket", "user", "gpus"])
    rows = []
    for (bucket, user), grp in claimed.groupby(["15min_bucket", "RemoteOwner"]):
        if _is_excluded(user, exclude):
            continue
        rows.append({"15min_bucket": bucket, "user": user, "gpus": grp["AssignedGPUs"].nunique()})
    return pd.DataFrame(rows)


def load_job_pressure_timeseries(
    base_dir: str,
    project: str,
    start_ts: datetime.datetime,
    end_ts: datetime.datetime,
    exclude: set[str] | None = None,
) -> pd.DataFrame:
    """
    Reconstruct a 15-min bucket time series of idle GPU queue depth.

    Filters job_pressure rows by ChtcProjects (if project is non-empty) and the
    [start_ts, end_ts] window.  Returns columns: [bucket, owner, queued_gpus].
    """
    exclude = exclude or set()
    start_unix = int(start_ts.timestamp())
    end_unix = int(end_ts.timestamp())

    # Discover DB files for the time range (mirrors get_required_databases pattern)
    db_files: list[str] = []
    cur = start_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_month = end_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur <= end_month:
        p = Path(base_dir) / f"job_pressure_{cur.strftime('%Y-%m')}.db"
        if p.exists():
            db_files.append(str(p))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    if not db_files:
        return pd.DataFrame(columns=["bucket", "owner", "queued_gpus"])

    frames = []
    for path in db_files:
        conn = sqlite3.connect(path)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(job_pressure)")}
        if "first_seen" in cols:
            rows = pd.read_sql_query(
                "SELECT Owner, RequestGPUs, ChtcProjects, first_seen, last_seen"
                " FROM job_pressure WHERE last_seen >= ? AND first_seen <= ?",
                conn,
                params=(start_unix, end_unix),
            )
        elif "timestamp" in cols:
            # Old snapshot schema: one row per collection tick; convert to unix ints
            rows = pd.read_sql_query(
                "SELECT Owner, RequestGPUs, ChtcProjects,"
                " CAST(strftime('%s', timestamp) AS INTEGER) AS first_seen,"
                " CAST(strftime('%s', timestamp) AS INTEGER) AS last_seen"
                " FROM job_pressure"
                " WHERE CAST(strftime('%s', timestamp) AS INTEGER) BETWEEN ? AND ?",
                conn,
                params=(start_unix, end_unix),
            )
        else:
            conn.close()
            typer.echo(f"Warning: unrecognised schema in {path}, skipping.", err=True)
            continue
        conn.close()
        frames.append(rows)

    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=["bucket", "owner", "queued_gpus"])

    jobs = pd.concat(frames, ignore_index=True)
    if project:
        jobs = jobs[jobs["ChtcProjects"].str.contains(project, case=False, na=False)]
    if exclude:
        jobs = jobs[~jobs["Owner"].str.split("@").str[0].isin(exclude)]
    if jobs.empty:
        return pd.DataFrame(columns=["bucket", "owner", "queued_gpus"])

    # Build 15-min bucket grid as Unix timestamps for fast overlap detection
    bucket_start = start_ts.replace(second=0, microsecond=0)
    bucket_start -= datetime.timedelta(minutes=bucket_start.minute % 15)
    buckets = pd.date_range(bucket_start, end_ts, freq="15min")
    bucket_unix = (buckets.astype("int64") // 1_000_000_000).to_numpy()  # ns → s
    bucket_size = 900  # seconds per 15-min bucket

    records = []
    for _, job in jobs.iterrows():
        owner = str(job["Owner"]) if job["Owner"] else "Unknown"
        gpus = float(job["RequestGPUs"]) if job["RequestGPUs"] else 0.0
        fs, ls = int(job["first_seen"]), int(job["last_seen"])
        # Bucket b overlaps interval [fs, ls] when: fs < b + bucket_size AND ls >= b
        mask = (bucket_unix <= ls) & (bucket_unix + bucket_size > fs)
        for bucket in buckets[mask]:
            records.append({"bucket": bucket, "owner": owner, "queued_gpus": gpus})

    if not records:
        return pd.DataFrame(columns=["bucket", "owner", "queued_gpus"])

    result = pd.DataFrame(records)
    return result.groupby(["bucket", "owner"])["queued_gpus"].sum().reset_index()


def compute_gini(values: list[float]) -> float:
    """Gini coefficient: 0 = perfect equality, 1 = one user takes everything."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    n = len(values)
    abs_diffs = sum(abs(xi - xj) for xi in values for xj in values)
    return abs_diffs / (2 * n * n * mean)


def compute_contention(
    priority_ts: pd.DataFrame,
    pressure_df: pd.DataFrame,
    priority_total_gpus: int,
) -> tuple[dict[str, dict[str, float]], int, int]:
    """
    Blocked GPU-hours per user, split into self-contention vs other-user contention.

    Self-contention:  user has jobs queued AND is already running on the host in
                      that bucket (waiting for more of their own slots to free up).
    Other-contention: user has jobs queued but a *different* user holds the host.

    Returns (contention_by_user, n_full_buckets, n_total_buckets) where
    contention_by_user maps username -> {"self": float, "other": float}.
    """
    if pressure_df.empty or priority_ts.empty or priority_total_gpus == 0:
        return {}, 0, 0

    # Claimed GPUs summed across all users per bucket
    claimed_per_bucket = priority_ts.groupby("15min_bucket")["gpus"].sum()
    n_total = len(claimed_per_bucket)
    full_buckets = set(claimed_per_bucket[claimed_per_bucket >= priority_total_gpus].index)
    n_full = len(full_buckets)

    if not full_buckets:
        return {}, n_full, n_total

    # pressure_df["bucket"] and priority_ts["15min_bucket"] are both floored to
    # 15-min Timestamps, so direct membership test works.
    blocked = pressure_df[pressure_df["bucket"].isin(full_buckets)]
    if blocked.empty:
        return {}, n_full, n_total

    # Build a lookup: bucket -> set of short usernames currently running.
    # RemoteOwner in gpu_state is "user@domain"; Owner in job_pressure is "user" — normalize both.
    running_ts = priority_ts[priority_ts["15min_bucket"].isin(full_buckets)].copy()
    running_ts["short_user"] = running_ts["user"].str.split("@").str[0]
    running_by_bucket: dict = running_ts.groupby("15min_bucket")["short_user"].apply(set).to_dict()

    user_contention: dict[str, dict[str, float]] = {}
    for owner, grp in blocked.groupby("owner"):
        short_owner = str(owner).split("@")[0]
        self_hours = 0.0
        other_hours = 0.0
        for bucket, bucket_grp in grp.groupby("bucket"):
            hours = float(bucket_grp["queued_gpus"].sum()) * BUCKET_HOURS
            running_here = running_by_bucket.get(bucket, set())
            if short_owner in running_here:
                self_hours += hours
            else:
                other_hours += hours
        if self_hours + other_hours > 0:
            user_contention[owner] = {"self": self_hours, "other": other_hours}

    return user_contention, n_full, n_total


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------


def _date_axis(ax: plt.Axes, hours_back: int) -> None:
    if hours_back <= 48:
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    else:
        ax.xaxis.set_major_locator(mdates.DayLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")


def plot_user_stacked_area(
    ts_df: pd.DataFrame,
    max_gpus: int,
    title: str,
    save_path: Path,
    hours_back: int = 168,
) -> None:
    """Stacked area: GPU count by user over time."""
    fig, ax = plt.subplots(figsize=(14, 5))
    if ts_df.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes, fontsize=14)
    else:
        pivot = ts_df.pivot_table(index="15min_bucket", columns="user", values="gpus", aggfunc="sum", fill_value=0)
        user_order = pivot.sum().sort_values(ascending=False).index.tolist()
        pivot = pivot[user_order]
        times = pivot.index.to_pydatetime()
        ax.stackplot(times, [pivot[u].values for u in user_order], labels=user_order, alpha=0.8)
        if max_gpus > 0:
            ax.axhline(max_gpus, color="black", linewidth=1.5, linestyle="--", label=f"Capacity ({max_gpus} GPUs)")
        ax.set_ylim(0, max(max_gpus, 1) + 0.5)
        ax.legend(loc="upper left", fontsize=8, ncol=2)
        _date_axis(ax, hours_back)
    ax.set_xlabel("Time")
    ax.set_ylabel("GPUs in use")
    ax.set_title(title)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()


def plot_job_pressure(
    pressure_df: pd.DataFrame,
    title: str,
    save_path: Path,
    hours_back: int = 168,
) -> None:
    """Stacked area: idle GPU queue depth by user over time."""
    fig, ax = plt.subplots(figsize=(14, 5))
    if pressure_df.empty:
        ax.text(0.5, 0.5, "No queued jobs found", ha="center", va="center", transform=ax.transAxes, fontsize=14)
    else:
        pivot = pressure_df.pivot_table(
            index="bucket", columns="owner", values="queued_gpus", aggfunc="sum", fill_value=0
        )
        user_order = pivot.sum().sort_values(ascending=False).index.tolist()
        pivot = pivot[user_order]
        times = pivot.index.to_pydatetime()
        ax.stackplot(times, [pivot[u].values for u in user_order], labels=user_order, alpha=0.8)
        ax.legend(loc="upper left", fontsize=8, ncol=2)
        _date_axis(ax, hours_back)
    ax.set_xlabel("Time")
    ax.set_ylabel("Queued GPUs")
    ax.set_title(title)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()


def plot_fairshare(
    user_hours: dict[str, float],
    title: str,
    save_path: Path,
) -> None:
    """Horizontal bar chart of actual GPU-hours vs equal share."""
    fig, ax = plt.subplots(figsize=(10, max(3, len(user_hours) * 0.6 + 1)))
    if not user_hours:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes, fontsize=14)
    else:
        users = sorted(user_hours, key=lambda u: user_hours[u], reverse=True)
        actuals = [user_hours[u] for u in users]
        equal = sum(actuals) / len(actuals)
        short_names = [u.split("@")[0] for u in users]
        y = range(len(users))
        ax.barh(list(y), actuals, color="steelblue", alpha=0.8, label="Actual GPU-hours")
        ax.axvline(equal, color="crimson", linewidth=2, linestyle="--", label=f"Equal share ({equal:.1f} h)")
        ax.set_yticks(list(y))
        ax.set_yticklabels(short_names)
        ax.set_xlabel("GPU-hours")
        ax.legend()
    ax.set_title(title)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()


def plot_contention(
    contention: dict[str, dict[str, float]],
    title: str,
    save_path: Path,
) -> None:
    """Stacked horizontal bar chart: self-contention vs other-user contention per user."""
    fig, ax = plt.subplots(figsize=(10, max(3, len(contention) * 0.6 + 1)))
    if not contention:
        ax.text(
            0.5,
            0.5,
            "No contention detected",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=14,
        )
    else:
        users = sorted(contention, key=lambda u: contention[u]["self"] + contention[u]["other"], reverse=True)
        other_vals = [contention[u]["other"] for u in users]
        self_vals = [contention[u]["self"] for u in users]
        short_names = [u.split("@")[0] for u in users]
        y = list(range(len(users)))
        ax.barh(y, other_vals, color="firebrick", alpha=0.85, label="Other-user contention")
        ax.barh(y, self_vals, left=other_vals, color="darkorange", alpha=0.85, label="Self-contention")
        ax.set_yticks(y)
        ax.set_yticklabels(short_names)
        ax.set_xlabel("Blocked GPU-hours")
        ax.legend(loc="lower right", fontsize=9)
    ax.set_title(title)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()


# ---------------------------------------------------------------------------
# Markdown table helpers
# ---------------------------------------------------------------------------


def user_table_md(user_hours: dict[str, float], denom: float, pct_label: str) -> str:
    if not user_hours:
        return "_No usage found in this period._\n"
    lines = [f"| User | GPU-hours | % of {pct_label} |", "|---|---|---|"]
    for user, hours in sorted(user_hours.items(), key=lambda x: -x[1]):
        pct = hours / denom * 100 if denom > 0 else 0.0
        lines.append(f"| {user.split('@')[0]} | {hours:.1f} | {pct:.1f}% |")
    return "\n".join(lines) + "\n"


def pressure_table_md(pressure_df: pd.DataFrame) -> str:
    if pressure_df.empty:
        return "_No queued jobs found for this group in this period._\n"
    summary = (
        pressure_df.groupby("owner")["queued_gpus"]
        .agg(
            total_gpu_hours=lambda x: x.sum() * BUCKET_HOURS,
            peak="max",
        )
        .reset_index()
        .sort_values("total_gpu_hours", ascending=False)
    )
    lines = ["| User | Queued GPU-hours | Peak simultaneous GPUs |", "|---|---|---|"]
    for _, row in summary.iterrows():
        lines.append(f"| {row['owner'].split('@')[0]} | {row['total_gpu_hours']:.1f} | {int(row['peak'])} |")
    return "\n".join(lines) + "\n"


def contention_table_md(contention: dict[str, dict[str, float]]) -> str:
    if not contention:
        return "_No contention detected: no queued jobs coincided with a fully-claimed host._\n"
    lines = [
        "| User | Other-user contention (h) | Self-contention (h) | Total (h) |",
        "|---|---|---|---|",
    ]
    for user, vals in sorted(contention.items(), key=lambda x: -(x[1]["self"] + x[1]["other"])):
        total = vals["self"] + vals["other"]
        lines.append(f"| {user.split('@')[0]} | {vals['other']:.1f} | {vals['self']:.1f} | {total:.1f} |")
    return "\n".join(lines) + "\n"


def fairshare_table_md(user_hours: dict[str, float], equal_share: float) -> str:
    if not user_hours:
        return "_No priority usage found._\n"
    lines = ["| User | Actual GPU-hours | Equal share | Surplus / Deficit |", "|---|---|---|---|"]
    for user, actual in sorted(user_hours.items(), key=lambda x: -x[1]):
        delta = actual - equal_share
        sign = "+" if delta >= 0 else ""
        lines.append(f"| {user.split('@')[0]} | {actual:.1f} | {equal_share:.1f} | {sign}{delta:.1f} |")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@app.command()
def main(
    host: str = typer.Option(
        "", help="Substring filter on Machine column (e.g. 'isye'). Inferred from --project if omitted."
    ),
    project: str = typer.Option(
        "", help="Substring filter on ChtcProjects / PrioritizedProjects. Inferred from --host if omitted."
    ),
    hours_back: int = typer.Option(168, help="Lookback window in hours (168 = 7 days)"),
    db_path: str = typer.Option("", help="Path to gpu_state_*.db (auto-discovered if empty)"),
    output_dir: str = typer.Option(".", help="Directory for report and figures/"),
    exclude_users: str = typer.Option("", help="Comma-separated usernames to exclude (e.g. 'admin1,admin2')"),
    backfill: bool = typer.Option(False, "--backfill", help="Include backfill slot usage section"),
) -> None:
    """Generate a per-host GPU usage Markdown report with plots."""
    if not host and not project:
        typer.echo("Error: at least one of --host or --project is required.", err=True)
        raise typer.Exit(1)

    # Keep originals for slug / report title before any inference expands them.
    host_arg = host
    project_arg = project

    exclude: set[str] = {u.strip() for u in exclude_users.split(",") if u.strip()}
    out = Path(output_dir)
    figures_dir = out / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    base_dir = str(Path(db_path).parent) if db_path else "."
    if not db_path:
        db_path = get_most_recent_database(base_dir) or ""
    if not db_path:
        typer.echo("Error: no gpu_state_*.db found. Specify --db-path.", err=True)
        raise typer.Exit(1)

    typer.echo(f"Loading GPU state ({hours_back}h back) ...")
    df_raw = get_time_filtered_data(db_path, hours_back)
    if df_raw.empty:
        typer.echo("Error: no data for the specified time range.", err=True)
        raise typer.Exit(1)

    # Resolve host ↔ project from each other when only one is given.
    if host and not project:
        host_df = df_raw[df_raw["Machine"].str.contains(host, case=False, na=False)]
        chtc_owned_tmp = load_chtc_owned_hosts()
        is_chtc_tmp = any(m in chtc_owned_tmp for m in host_df["Machine"].dropna().unique())
        pclass_tmp = "Priority-CHTCOwned" if is_chtc_tmp else "Priority-ResearcherOwned"
        pri_tmp = filter_df_enhanced(host_df, pclass_tmp, "", "")
        counts = pri_tmp["PrioritizedProjects"].dropna().value_counts()
        if not counts.empty:
            project = str(counts.index[0])
            typer.echo(f"  Inferred project : {project}")
    elif project and not host:
        mask = df_raw["PrioritizedProjects"].str.contains(project, case=False, na=False)
        inferred_machines = sorted(df_raw[mask]["Machine"].dropna().unique())
        if not inferred_machines:
            typer.echo(f"Error: no machines found with PrioritizedProjects matching '{project}'.", err=True)
            raise typer.Exit(1)
        host = "|".join(inferred_machines)
        typer.echo(f"  Inferred host(s) : {', '.join(inferred_machines)}")

    df = df_raw[df_raw["Machine"].str.contains(host, case=False, na=False)].copy()
    if df.empty:
        typer.echo(f"Error: no data for host matching '{host}'.", err=True)
        raise typer.Exit(1)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    period_start = df["timestamp"].min()
    period_end = df["timestamp"].max()
    machines = sorted(df["Machine"].unique())
    gpu_types = sorted(df["GPUs_DeviceName"].dropna().unique())
    gpu_type_str = ", ".join(gpu_types) or "Unknown"
    total_gpus = df["AssignedGPUs"].dropna().nunique()
    project_names = sorted({p for p in df["PrioritizedProjects"].dropna().unique() if p})

    typer.echo(f"  Machines : {', '.join(machines)}")
    typer.echo(f"  GPU type : {gpu_type_str} × {total_gpus}")
    typer.echo(f"  Period   : {period_start} → {period_end}")

    # Determine priority/backfill class based on machine ownership
    chtc_owned = load_chtc_owned_hosts()
    is_chtc = any(m in chtc_owned for m in machines)
    priority_class = "Priority-CHTCOwned" if is_chtc else "Priority-ResearcherOwned"
    backfill_class = "Backfill-CHTCOwned" if is_chtc else "Backfill-ResearcherOwned"

    # Priority slots
    typer.echo("Computing priority slot usage ...")
    priority_all = filter_df_enhanced(df, priority_class, "", "")
    priority_total_gpus = priority_all["AssignedGPUs"].dropna().nunique()
    priority_claimed = filter_df_enhanced(df, priority_class, "Claimed", "")
    priority_ts = build_user_timeseries(priority_claimed, exclude)
    priority_hours = calculate_user_hours(priority_claimed, exclude)

    # Backfill slots (optional)
    if backfill:
        typer.echo("Computing backfill slot usage ...")
        backfill_all = filter_df_enhanced(df, backfill_class, "", "")
        backfill_total_gpus = backfill_all["AssignedGPUs"].dropna().nunique()
        backfill_claimed = filter_df_enhanced(df, backfill_class, "Claimed", "")
        backfill_ts = build_user_timeseries(backfill_claimed, exclude)
        backfill_hours = calculate_user_hours(backfill_claimed, exclude)
    else:
        backfill_total_gpus = 0
        backfill_ts = pd.DataFrame(columns=["15min_bucket", "user", "gpus"])
        backfill_hours: dict[str, float] = {}

    # Available GPU-hours (based on number of distinct 15-min snapshots in period)
    num_buckets = get_preprocessed_dataframe(df.copy())["15min_bucket"].nunique()
    avail_priority = priority_total_gpus * num_buckets * BUCKET_HOURS
    avail_backfill = backfill_total_gpus * num_buckets * BUCKET_HOURS
    used_priority = sum(priority_hours.values())
    used_backfill = sum(backfill_hours.values())

    def pct(num: float, denom: float) -> str:
        return f"{num / denom * 100:.1f}%" if denom > 0 else "—"

    # Job pressure
    typer.echo("Loading job pressure data ...")
    pressure_df = load_job_pressure_timeseries(base_dir, project, period_start, period_end, exclude)

    # Fairshare metrics
    gini = compute_gini(list(priority_hours.values()))
    equal_share = used_priority / len(priority_hours) if priority_hours else 0.0

    # Contention: queued jobs that coincided with a fully-claimed host
    contention_by_user, n_full_buckets, n_total_buckets = compute_contention(
        priority_ts, pressure_df, priority_total_gpus
    )

    # Plots
    report_title = host_arg or project_arg
    slug = report_title.lower().replace("/", "_").replace(".", "_").replace(" ", "_")
    typer.echo("Generating plots ...")

    plot_user_stacked_area(
        priority_ts,
        priority_total_gpus,
        f"Priority GPU Usage by User — {report_title}",
        figures_dir / f"{slug}_priority_users.png",
        hours_back,
    )
    if backfill:
        plot_user_stacked_area(
            backfill_ts,
            backfill_total_gpus or total_gpus,
            f"Secondary (Backfill) GPU Usage by User — {report_title}",
            figures_dir / f"{slug}_backfill_users.png",
            hours_back,
        )
    plot_job_pressure(
        pressure_df,
        f"Queued GPU Jobs ({project or 'all'}) — {report_title}",
        figures_dir / f"{slug}_job_pressure.png",
        hours_back,
    )
    plot_contention(
        contention_by_user,
        f"Contention: Blocked GPU-hours — {report_title}",
        figures_dir / f"{slug}_contention.png",
    )
    plot_fairshare(
        priority_hours,
        f"Fairshare: Priority GPU-hours — {report_title}",
        figures_dir / f"{slug}_fairshare.png",
    )

    # Build report
    period_str = f"{period_start.strftime('%Y-%m-%d %H:%M')} → {period_end.strftime('%Y-%m-%d %H:%M')}"
    project_label = project if project else "_(no filter — all groups)_"

    if backfill:
        overview_rows = "\n".join(
            [
                f"| Total GPUs | {priority_total_gpus} | {backfill_total_gpus} |",
                f"| Available GPU-hours | {avail_priority:.1f} | {avail_backfill:.1f} |",
                f"| Used GPU-hours | {used_priority:.1f} | {used_backfill:.1f} |",
                f"| Utilization | {pct(used_priority, avail_priority)} | {pct(used_backfill, avail_backfill)} |",
            ]
        )
        overview_table = f"| | Priority slots | Secondary (Backfill) slots |\n|---|---|---|\n{overview_rows}"
        backfill_section = f"""\
### Secondary (Backfill) Slots

{user_table_md(backfill_hours, used_backfill, "secondary used")}
![Secondary (Backfill) GPU usage by user](figures/{slug}_backfill_users.png)

---

"""
    else:
        overview_rows = "\n".join(
            [
                f"| Total GPUs | {priority_total_gpus} |",
                f"| Available GPU-hours | {avail_priority:.1f} |",
                f"| Used GPU-hours | {used_priority:.1f} |",
                f"| Utilization | {pct(used_priority, avail_priority)} |",
            ]
        )
        overview_table = f"| | Priority slots |\n|---|---|\n{overview_rows}"
        backfill_section = ""

    report = f"""\
# GPU Host Report: {report_title}

**Machine(s):** {", ".join(machines)}
**GPU type:** {gpu_type_str} × {total_gpus}
**Priority project(s):** {", ".join(project_names) if project_names else "—"}
**Period:** {period_str} ({hours_back}h)

---

## Overview

{overview_table}

---

## Who's Been Running

### Priority Slots

{user_table_md(priority_hours, used_priority, "priority used")}
![Priority GPU usage by user](figures/{slug}_priority_users.png)

---

{backfill_section}## Job Pressure

Project filter: {project_label}

{pressure_table_md(pressure_df)}
![Queued GPU jobs over time](figures/{slug}_job_pressure.png)

---

## Contention Analysis

**How this is calculated:** For each 15-minute snapshot where all {priority_total_gpus} priority \
GPU(s) are claimed (host at capacity), any user with jobs in the {project or "GPU"} queue \
during that snapshot accumulates _blocked GPU-hours_ equal to their requested GPU count × 0.25 h. \
Blocked hours are split into two categories:

- **Other-user contention** — the queued user has *no* jobs currently running on the host; \
someone else is holding all the slots.
- **Self-contention** — the queued user *is* already running on the host but wants more slots \
(their own jobs are the bottleneck).

The host was at full priority capacity in **{n_full_buckets} of {n_total_buckets} snapshots** \
({pct(n_full_buckets, n_total_buckets)} of the period).

{contention_table_md(contention_by_user)}
![Contention: blocked GPU-hours by user](figures/{slug}_contention.png)

---

## Fairshare Analysis

**Gini coefficient (priority slots):** {gini:.3f}
_(0 = perfectly equal, 1 = one user has everything)_

{fairshare_table_md(priority_hours, equal_share)}
![Fairshare bar chart](figures/{slug}_fairshare.png)
"""

    report_path = out / f"{slug}_report.md"
    report_path.write_text(report)
    typer.echo(f"Report: {report_path}")
    typer.echo(f"Figures: {figures_dir}/")


if __name__ == "__main__":
    app()
