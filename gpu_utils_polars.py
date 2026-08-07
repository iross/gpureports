#!/usr/bin/env python3
"""
GPU state Parquet/SQLite file discovery and host-exclusion config loading.

Everything DataFrame-shaped (filtering, classification, dedup) moved to the
canonical pipeline in stats_calculations.prepare_frames() during TASK-49.1;
this module's own filter_df/filter_df_enhanced had no remaining callers once
that landed (see TASK-49.4) and were removed rather than kept as unused
duplicates of gpu_utils.py's pandas versions, which still have real callers.
"""

import datetime
from pathlib import Path

import polars as pl
import yaml

# Global variable to store host exclusion configuration
HOST_EXCLUSIONS = {}

# Global variable to cache hosted capacity list
_CHTC_OWNED_HOSTS = None


def load_chtc_owned_hosts(chtc_owned_file: str = "chtc_owned") -> set:
    """
    Load CHTC owned hosts from file.

    Args:
        chtc_owned_file: Path to file containing CHTC owned host names

    Returns:
        Set of CHTC owned host names
    """
    global _CHTC_OWNED_HOSTS

    if _CHTC_OWNED_HOSTS is not None:
        return _CHTC_OWNED_HOSTS

    chtc_owned_hosts = set()
    chtc_owned_path = Path(chtc_owned_file)

    if chtc_owned_path.exists():
        try:
            with open(chtc_owned_path) as f:
                for line in f:
                    host = line.strip()
                    if host:  # Skip empty lines
                        chtc_owned_hosts.add(host)
        except Exception as e:
            print(f"Warning: Could not load CHTC owned hosts from {chtc_owned_file}: {e}")
    else:
        print(f"Warning: CHTC owned file {chtc_owned_file} not found")

    _CHTC_OWNED_HOSTS = chtc_owned_hosts
    return chtc_owned_hosts


def load_host_exclusions(exclusions_config: str | None = None, yaml_file: str | None = None) -> dict[str, str]:
    """
    Load host exclusion configuration from YAML file or string.

    Args:
        exclusions_config: Optional string with exclusion configuration
        yaml_file: Optional path to YAML file with exclusions

    Returns:
        Dictionary mapping excluded host patterns to reasons
    """
    exclusions = {}

    if yaml_file and Path(yaml_file).exists():
        try:
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
                if data and "excluded_hosts" in data:
                    exclusions = data["excluded_hosts"]
        except Exception as e:
            print(f"Warning: Could not load exclusions from {yaml_file}: {e}")

    if exclusions_config:
        try:
            data = yaml.safe_load(exclusions_config)
            if data and "excluded_hosts" in data:
                exclusions.update(data["excluded_hosts"])
        except Exception as e:
            print(f"Warning: Could not parse exclusions config: {e}")

    return exclusions


def get_required_parquet_files(
    start_time: datetime.datetime, end_time: datetime.datetime, base_dir: str = "."
) -> list[tuple[str, str]]:
    """Return (path, format) pairs for each month in [start_time, end_time].

    Prefers Parquet over SQLite for each month; skips months with neither.
    Format is "parquet" or "sqlite".
    """
    files = []
    current = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while current <= end_month:
        stem = f"gpu_state_{current.strftime('%Y-%m')}"
        parquet = Path(base_dir) / f"{stem}.parquet"
        sqlite = Path(base_dir) / f"{stem}.db"
        if parquet.exists():
            files.append((str(parquet), "parquet"))
        elif sqlite.exists():
            files.append((str(sqlite), "sqlite"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return files


def get_most_recent_parquet(base_dir: str = ".") -> str | None:
    """Return path to the most recent gpu_state Parquet file, or None."""
    import glob

    files = sorted(glob.glob(str(Path(base_dir) / "gpu_state_*.parquet")))
    return files[-1] if files else None


def get_latest_timestamp_from_most_recent_parquet(base_dir: str = ".") -> datetime.datetime | None:
    """Return the latest timestamp across the most recent Parquet (or SQLite fallback)."""
    parquet = get_most_recent_parquet(base_dir)
    if parquet:
        try:
            ts = pl.scan_parquet(parquet).select(pl.col("timestamp").max()).collect()["timestamp"][0]
            if ts is not None:
                return ts.replace(tzinfo=None) if hasattr(ts, "tzinfo") else ts
        except Exception:
            pass

    # Fall back to SQLite
    return get_latest_timestamp_from_most_recent_db(base_dir)


def get_required_databases(start_time: datetime.datetime, end_time: datetime.datetime, base_dir: str = ".") -> list:
    """
    Get list of database files needed to cover the specified time range.

    Args:
        start_time: Start of time range
        end_time: End of time range
        base_dir: Directory containing database files

    Returns:
        List of database file paths
    """
    db_files = []

    # Generate list of months between start and end
    current = start_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_month = end_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end_month:
        db_file = Path(base_dir) / f"gpu_state_{current.strftime('%Y-%m')}.db"
        if db_file.exists():
            db_files.append(str(db_file))

        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return db_files


def get_most_recent_database(base_dir: str = ".") -> str | None:
    """
    Find the most recent database file in the given directory.

    Args:
        base_dir: Directory to search for database files

    Returns:
        Path to the most recent database file, or None if none found
    """
    import glob

    # Find all database files matching the pattern
    pattern = str(Path(base_dir) / "gpu_state_*.db")
    db_files = glob.glob(pattern)

    if not db_files:
        return None

    # Sort by filename (which contains YYYY-MM date) to get the most recent
    db_files.sort()
    return db_files[-1]


def get_latest_timestamp_from_most_recent_db(base_dir: str = ".") -> datetime.datetime | None:
    """
    Get the latest timestamp from the most recent database file.

    Args:
        base_dir: Directory containing database files

    Returns:
        Latest timestamp from the most recent database, or None if not found
    """
    import sqlite3

    most_recent_db = get_most_recent_database(base_dir)
    if not most_recent_db:
        return None

    try:
        conn = sqlite3.connect(most_recent_db)
        # Use Polars to read the max timestamp
        df = pl.read_database("SELECT MAX(timestamp) as max_time FROM gpu_state", conn)
        conn.close()

        if len(df) > 0 and df["max_time"][0] is not None:
            # Convert to datetime if it's a string
            max_time = df["max_time"][0]
            if isinstance(max_time, str):
                return datetime.datetime.fromisoformat(max_time)
            return max_time
    except Exception:
        pass

    return None
