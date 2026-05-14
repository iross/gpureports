#!/usr/bin/env python3
"""
Periodic snapshot of idle GPU job pressure in the HTCondor pool.

Queries all schedds for idle jobs requesting GPUs and records each idle
period as a single row with first_seen / last_seen INTEGER timestamps (Unix
seconds). Each poll extends last_seen for jobs still idle and opens a new
row for newly-appeared jobs, so long-idle jobs that previously generated
hundreds of identical rows now generate one.

Schema change from the original: timestamp TEXT column is replaced by
first_seen INTEGER and last_seen INTEGER; column order differs.
Existing old-schema DBs are read by migrate_job_pressure.py.

Crontab entry (on the production host):
    */5 * * * * /home/iaross/gpureports/.venv/bin/python \
        /home/iaross/gpureports/get_job_pressure.py &> /tmp/job_pressure.log
"""

import datetime
import sqlite3

import htcondor
import typer

COLL = htcondor.Collector("cm.chtc.wisc.edu")
TARGET_APS = ["ap2001.chtc.wisc.edu", "ap2002.chtc.wisc.edu"]

PROJ = [
    "GlobalJobId",
    "Owner",
    "RequestGPUs",
    "RequestCPUs",
    "RequestMemory",
    "RequestGPUMemory",
    "QDate",
    "ChtcProjects",
]

CONSTRAINT = "RequestGPUs >= 1 && JobStatus == 1"

# A job absent for longer than this is treated as a closed interval; the
# next sighting opens a fresh row. Set to 3× the actual crontab interval.
# Current crontab runs every 30 minutes → 3 × 1800 = 5400.
_STALE_SECONDS = 5400

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS job_pressure (
        GlobalJobId      TEXT NOT NULL,
        ScheddName       TEXT,
        Owner            TEXT,
        RequestGPUs      REAL,
        RequestCPUs      REAL,
        RequestMemory    REAL,
        RequestGPUMemory REAL,
        QDate            INTEGER,
        ChtcProjects     TEXT,
        first_seen       INTEGER NOT NULL,
        last_seen        INTEGER NOT NULL
    )
"""
_CREATE_IDX_LAST = "CREATE INDEX IF NOT EXISTS idx_last_seen  ON job_pressure (last_seen)"
_CREATE_IDX_FIRST = "CREATE INDEX IF NOT EXISTS idx_first_seen ON job_pressure (first_seen)"


def _eval_classad(val: object) -> object:
    """Evaluate a ClassAd ExprTree to a Python value if possible.

    In k8s (no local HTCondor config) schedd.query() returns unevaluated
    ExprTree objects; on baremetal the local config provides context so values
    come back as native Python types already.
    """
    if hasattr(val, "eval"):
        try:
            return val.eval()
        except Exception:
            pass
    return val


def _float_or_none(val: object) -> float | None:
    val = _eval_classad(val)
    if val is None:
        return None
    try:
        f = float(val)  # type: ignore[arg-type]
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _safe_float(val: object, default: float = 0.0) -> float:
    val = _eval_classad(val)
    if val is None:
        return default
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _safe_int(val: object, default: int = 0) -> int:
    val = _eval_classad(val)
    if val is None:
        return default
    try:
        return int(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def collect_idle_gpu_jobs() -> list[dict]:
    """Query all schedds for idle GPU jobs; return list of job attribute dicts."""
    try:
        schedd_ads = COLL.locateAll(htcondor.DaemonTypes.Schedd)
    except Exception as e:
        print(f"Warning: could not query collector for schedds: {e}")
        return []

    jobs: list[dict] = []
    for schedd_ad in schedd_ads:
        schedd_name = schedd_ad.get("Name", "")
        if schedd_name not in TARGET_APS:
            continue
        try:
            schedd = htcondor.Schedd(schedd_ad)
            ads = schedd.query(constraint=CONSTRAINT, projection=PROJ)
        except Exception as e:
            print(f"Warning: query failed for schedd {schedd_name}: {e}")
            continue

        for ad in ads:
            jobs.append(
                {
                    "GlobalJobId": ad.get("GlobalJobId", ""),
                    "ScheddName": schedd_name,
                    "Owner": ad.get("Owner", ""),
                    "RequestGPUs": _safe_float(ad.get("RequestGPUs")),
                    "RequestCPUs": _safe_float(ad.get("RequestCPUs")),
                    "RequestMemory": _safe_float(ad.get("RequestMemory")),
                    "RequestGPUMemory": _float_or_none(ad.get("RequestGPUMemory")),
                    "QDate": _safe_int(ad.get("QDate")),
                    "ChtcProjects": ad.get("ChtcProjects", ""),
                }
            )

    return jobs


def update_intervals(jobs: list[dict], db_path: str, now_ts: int) -> None:
    """Extend last_seen for continuing idle jobs; open new rows for new ones."""
    conn = sqlite3.connect(db_path)
    conn.execute(_CREATE_TABLE)
    conn.execute(_CREATE_IDX_LAST)
    conn.execute(_CREATE_IDX_FIRST)

    # Open intervals: rows whose last_seen is recent enough to still be active.
    open_rows = conn.execute(
        "SELECT GlobalJobId, rowid FROM job_pressure WHERE last_seen >= ?",
        (now_ts - _STALE_SECONDS,),
    ).fetchall()
    # If a GlobalJobId appears in multiple open rows (shouldn't happen), keep
    # the most recently updated one.
    open_map: dict[str, int] = {}
    for gid, rowid in open_rows:
        open_map[gid] = rowid

    current_ids = {j["GlobalJobId"] for j in jobs}

    continuing = current_ids & open_map.keys()
    if continuing:
        conn.executemany(
            "UPDATE job_pressure SET last_seen = ? WHERE rowid = ?",
            [(now_ts, open_map[gid]) for gid in continuing],
        )

    new_jobs = [j for j in jobs if j["GlobalJobId"] not in open_map]
    if new_jobs:
        conn.executemany(
            "INSERT INTO job_pressure VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    j["GlobalJobId"],
                    j["ScheddName"],
                    j["Owner"],
                    j["RequestGPUs"],
                    j["RequestCPUs"],
                    j["RequestMemory"],
                    j["RequestGPUMemory"],
                    j["QDate"],
                    j["ChtcProjects"],
                    now_ts,
                    now_ts,
                )
                for j in new_jobs
            ],
        )

    conn.commit()
    conn.close()


def main(db_path: str = typer.Argument("/home/iaross/gpureports")) -> None:
    now = datetime.datetime.now()
    now_ts = int(now.timestamp())
    month = now.strftime("%Y-%m")

    jobs = collect_idle_gpu_jobs()
    print(f"{now.isoformat()}: {len(jobs)} idle GPU jobs")
    if jobs:
        update_intervals(jobs, f"{db_path}/job_pressure_{month}.db", now_ts)


if __name__ == "__main__":
    typer.run(main)
