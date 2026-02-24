#!/usr/bin/env python3
"""
Find all users who have run jobs on a specific host in primary slots over a time period.
"""

import datetime
import sqlite3
from pathlib import Path

import pandas as pd


def get_users_on_host(
    host: str,
    months_back: int = 2,
    slot_type: str = "Primary"
) -> set:
    """
    Get unique users who have run jobs on a specific host.

    Args:
        host: Hostname to filter for (e.g., "isyegpu4000.chtc.wisc.edu")
        months_back: Number of months to look back
        slot_type: Slot type to filter for ("Primary", "Shared", "Backfill", or "All")

    Returns:
        Set of unique usernames
    """
    # Determine which databases to load based on months_back
    today = datetime.datetime.now()
    db_paths = []

    for i in range(months_back + 1):
        target_date = today - datetime.timedelta(days=30 * i)
        db_name = f"gpu_state_{target_date.strftime('%Y-%m')}.db"
        db_path = Path(db_name)

        if db_path.exists() and db_path.stat().st_size > 0:
            db_paths.append(db_path)
            print(f"Found database: {db_name}")

    if not db_paths:
        print("Error: No database files found")
        return set()

    all_users = set()

    # Load data from each database
    for db_path in db_paths:
        print(f"\nProcessing {db_path.name}...")

        try:
            conn = sqlite3.connect(db_path)

            # Load data into DataFrame
            query = "SELECT * FROM gpu_state"
            df = pd.read_sql_query(query, conn)
            conn.close()

            if df.empty:
                print(f"  No data in {db_path.name}")
                continue

            print(f"  Loaded {len(df)} rows")

            # Filter for the specific host
            df = df[df["Machine"].str.contains(host, case=False, na=False)]
            print(f"  After host filter: {len(df)} rows")

            if df.empty:
                print(f"  No data for host {host}")
                continue

            # Filter by slot type
            if slot_type == "Primary":
                # Primary slots are those with PrioritizedProjects and not backfill
                df = df[
                    (df["PrioritizedProjects"].notna()) &
                    (df["PrioritizedProjects"] != "") &
                    (~df["Name"].str.contains("backfill", case=False, na=False))
                ]
                print(f"  After primary slot filter: {len(df)} rows")
            elif slot_type == "Shared":
                # Shared slots
                df = df[df["Name"].str.contains("shared", case=False, na=False)]
                print(f"  After shared slot filter: {len(df)} rows")
            elif slot_type == "Backfill":
                # Backfill slots
                df = df[df["Name"].str.contains("backfill", case=False, na=False)]
                print(f"  After backfill slot filter: {len(df)} rows")
            # If "All", don't filter

            # Filter for claimed slots (jobs actually running)
            df = df[df["State"] == "Claimed"]
            print(f"  After claimed filter: {len(df)} rows")

            if df.empty:
                print(f"  No claimed jobs for host {host}")
                continue

            # Extract unique users
            users = set(df["RemoteOwner"].dropna().unique())
            # Filter out empty strings
            users = {u for u in users if u and u.strip()}

            print(f"  Found {len(users)} unique users in this database")
            all_users.update(users)

        except Exception as e:
            print(f"  Error processing {db_path.name}: {e}")
            continue

    return all_users


def main():
    """Main function."""
    host = "isyegpu4000.chtc.wisc.edu"
    months_back = 2
    slot_type = "Primary"

    print(f"Finding all users who ran jobs on {host}")
    print(f"Looking back {months_back} months")
    print(f"Slot type: {slot_type}")
    print("=" * 80)

    users = get_users_on_host(host, months_back, slot_type)

    print("\n" + "=" * 80)
    print(f"\nTotal unique users: {len(users)}")
    print("\nUser list:")
    print("-" * 80)

    for user in sorted(users):
        print(f"  {user}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
