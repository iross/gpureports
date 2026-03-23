import datetime
import sqlite3
from collections import defaultdict

import htcondor
import pandas as pd
import typer
from sqlalchemy import create_engine

coll = htcondor.Collector("cm.chtc.wisc.edu")


def get_gpus() -> pd.DataFrame:
    PROJ = [
        "Name",
        "AssignedGPUs",
        "AvailableGPUs",
        "State",
        "GPUs_DeviceName",
        "GPUs_GlobalMemoryMb",
        "PrioritizedProjects",
        "GPUsAverageUsage",
        "Machine",
        #   "GPUsMemoryUsage",
        "RemoteOwner",
        "GlobalJobId",
    ]
    res = coll.query(htcondor.AdTypes.Startd, constraint="GPUs >= 1", projection=PROJ)
    df = pd.DataFrame(columns=PROJ)
    for ad in res:
        ad["AvailableGPUs"] = ",".join([i.__str__().replace("GPUs_", "") for i in ad["AvailableGPUs"]])
        # drop all keys starting with GPUs_
        ad = {k: v for k, v in ad.items() if not k.startswith("GPUs_GPU_")}
        df = pd.concat([df, pd.DataFrame([dict(ad)])], ignore_index=True)

    # Backfill slots don't actually have these GPUs assigned, but for ease downstream, we'll pretend.
    df.loc[df["Name"].str.contains("backfill"), "AssignedGPUs"] = df.loc[
        df["Name"].str.contains("backfill"), "AvailableGPUs"
    ]

    # Replace GPU- with GPU_
    df["AssignedGPUs"] = df["AssignedGPUs"].str.replace("GPU_", "GPU-")

    df = df.assign(AssignedGPUs=df["AssignedGPUs"].str.split(",")).explode("AssignedGPUs")

    # add a timestamp column to the dataframe
    df["timestamp"] = pd.Timestamp.now()
    return df


def _parse_schedd_from_job_id(global_job_id: str) -> str | None:
    """Extract schedd hostname from a GlobalJobId.

    HTCondor GlobalJobId format: <schedd_name>#<cluster>.<proc>#<qdate>
    """
    if not global_job_id:
        return None
    return global_job_id.split("#")[0]


def collect_job_info(df: pd.DataFrame, db_path: str) -> None:
    """Query condor_q for claimed slots' jobs and store new entries in job_info DB.

    Only claimed slots are queried. Queries are batched per schedd hostname to
    minimise round trips. Jobs already present in the DB are skipped.

    Args:
        df: gpu_state DataFrame from get_gpus().
        db_path: Path to the job_info_YYYY-MM.db file.
    """
    claimed = df[df["State"].str.lower() == "claimed"].copy()
    claimed = claimed[claimed["GlobalJobId"].notna() & (claimed["GlobalJobId"] != "")]
    if claimed.empty:
        return

    job_ids = claimed["GlobalJobId"].dropna().unique().tolist()

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_info (
            GlobalJobId TEXT PRIMARY KEY,
            Cmd         TEXT,
            Args        TEXT,
            Owner       TEXT,
            RequestGPUs REAL,
            QDate       INTEGER,
            first_seen  TEXT
        )
    """)
    conn.commit()
    existing = {row[0] for row in conn.execute("SELECT GlobalJobId FROM job_info")}
    conn.close()

    new_job_ids = [j for j in job_ids if j not in existing]
    if not new_job_ids:
        return

    # Group by schedd hostname to batch queries
    schedd_to_jobs: dict[str, list[str]] = defaultdict(list)
    for job_id in new_job_ids:
        schedd = _parse_schedd_from_job_id(job_id)
        if schedd:
            schedd_to_jobs[schedd].append(job_id)

    rows: list[tuple] = []
    now_str = datetime.datetime.now().isoformat()

    for schedd_name, batch_ids in schedd_to_jobs.items():
        try:
            schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, schedd_name)
            schedd = htcondor.Schedd(schedd_ad)
        except Exception as e:
            print(f"Warning: could not locate schedd {schedd_name}: {e}")
            continue

        id_list = " || ".join(f'GlobalJobId == "{jid}"' for jid in batch_ids)
        constraint = f"({id_list})"
        proj = ["GlobalJobId", "Cmd", "Arguments", "Owner", "RequestGPUs", "QDate"]

        try:
            ads = schedd.query(constraint=constraint, projection=proj)
        except Exception as e:
            print(f"Warning: condor_q query failed for {schedd_name}: {e}")
            continue

        for ad in ads:
            rows.append(
                (
                    ad.get("GlobalJobId", ""),
                    ad.get("Cmd", ""),
                    ad.get("Arguments", ""),
                    ad.get("Owner", ""),
                    float(ad.get("RequestGPUs", 0) or 0),
                    int(ad.get("QDate", 0) or 0),
                    now_str,
                )
            )

    if not rows:
        return

    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT OR IGNORE INTO job_info "
        "(GlobalJobId, Cmd, Args, Owner, RequestGPUs, QDate, first_seen) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def main(db_path: str = typer.Argument("/home/iaross/gpureports")):
    df = get_gpus()
    month = datetime.datetime.now().strftime("%Y-%m")
    disk_engine = create_engine(f"sqlite:///{db_path}/gpu_state_{month}.db")
    df.to_sql("gpu_state", disk_engine, if_exists="append", index=False)

    job_info_db = f"{db_path}/job_info_{month}.db"
    collect_job_info(df, job_info_db)


if __name__ == "__main__":
    typer.run(main)
