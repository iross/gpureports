#!/usr/bin/env python3
"""
Periodic snapshot of idle GPU job pressure in the HTCondor pool.

Queries all schedds for idle jobs requesting GPUs and records per-job
resource requests in a monthly SQLite database.

Crontab entry (on the production host):
    */5 * * * * /home/iaross/gpureports/.venv/bin/python \
        /home/iaross/gpureports/get_job_pressure.py &> /tmp/job_pressure.log
"""

import datetime
import sqlite3

import htcondor
import typer

COLL = htcondor.Collector("cm.chtc.wisc.edu")

PROJ = [
    "GlobalJobId",
    "Owner",
    "RequestGPUs",
    "RequestCPUs",
    "RequestMemory",
    "RequestGPUMemory",
    "QDate",
]

CONSTRAINT = "RequestGPUs >= 1 && JobStatus == 1"

_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS job_pressure (
        timestamp        TEXT,
        GlobalJobId      TEXT,
        ScheddName       TEXT,
        Owner            TEXT,
        RequestGPUs      REAL,
        RequestCPUs      REAL,
        RequestMemory    REAL,
        RequestGPUMemory REAL,
        QDate            INTEGER
    )
"""
_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_timestamp ON job_pressure (timestamp)"


def _float_or_none(val: object) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)  # type: ignore[arg-type]
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def collect_idle_gpu_jobs(timestamp: str) -> list[tuple]:
    """Query all schedds for idle GPU jobs; return rows ready for DB insertion."""
    try:
        schedd_ads = COLL.locateAll(htcondor.DaemonTypes.Schedd)
    except Exception as e:
        print(f"Warning: could not query collector for schedds: {e}")
        return []

    rows: list[tuple] = []
    for schedd_ad in schedd_ads:
        schedd_name = schedd_ad.get("Name", "")
        try:
            schedd = htcondor.Schedd(schedd_ad)
            ads = schedd.query(constraint=CONSTRAINT, projection=PROJ)
        except Exception as e:
            print(f"Warning: query failed for schedd {schedd_name}: {e}")
            continue

        for ad in ads:
            rows.append(
                (
                    timestamp,
                    ad.get("GlobalJobId", ""),
                    schedd_name,
                    ad.get("Owner", ""),
                    float(ad.get("RequestGPUs", 0) or 0),
                    float(ad.get("RequestCPUs", 0) or 0),
                    float(ad.get("RequestMemory", 0) or 0),
                    _float_or_none(ad.get("RequestGPUMemory")),
                    int(ad.get("QDate", 0) or 0),
                )
            )

    return rows


def store_rows(rows: list[tuple], db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(_CREATE_TABLE)
    conn.execute(_CREATE_INDEX)
    conn.executemany("INSERT INTO job_pressure VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def main(db_path: str = typer.Argument("/home/iaross/gpureports")) -> None:
    timestamp = datetime.datetime.now().isoformat()
    month = datetime.datetime.now().strftime("%Y-%m")
    rows = collect_idle_gpu_jobs(timestamp)
    print(f"{timestamp}: {len(rows)} idle GPU jobs")
    if rows:
        store_rows(rows, f"{db_path}/job_pressure_{month}.db")


if __name__ == "__main__":
    typer.run(main)
