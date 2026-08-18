#!/usr/bin/env python3
"""Convert monthly gpu_state SQLite databases to Parquet format.

Skips months that already have a .parquet file unless --force is given.
Reads from gpu_state_YYYY-MM.db, writes gpu_state_YYYY-MM.parquet alongside it.
"""

import sqlite3
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

app = typer.Typer(add_completion=False)


def _sqlite_to_parquet(db_path: Path, force: bool) -> tuple[int, int]:
    """Convert one SQLite file to Parquet. Returns (row_count, sqlite_bytes)."""
    parquet_path = db_path.with_suffix(".parquet")

    if parquet_path.exists() and not force:
        typer.echo(f"  skip  {db_path.name} (parquet exists; use --force to overwrite)")
        return 0, 0

    conn = sqlite3.connect(str(db_path))
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "gpu_state" not in tables:
        conn.close()
        typer.echo(f"  skip  {db_path.name} (no gpu_state table)")
        return 0, 0
    df = pl.read_database("SELECT * FROM gpu_state", conn)
    conn.close()

    if "timestamp" in df.columns and df.schema["timestamp"] in (pl.Utf8, pl.String):
        df = df.with_columns(pl.col("timestamp").str.to_datetime(strict=False))

    df.write_parquet(str(parquet_path), compression="zstd")

    sqlite_bytes = db_path.stat().st_size
    parquet_bytes = parquet_path.stat().st_size
    ratio = sqlite_bytes / parquet_bytes if parquet_bytes else 0
    typer.echo(
        f"  wrote {db_path.name} → {parquet_path.name}  "
        f"{len(df):,} rows  "
        f"{sqlite_bytes / 1e6:.1f} MB → {parquet_bytes / 1e6:.1f} MB  "
        f"({ratio:.0f}x)"
    )
    return len(df), sqlite_bytes


@app.command()
def main(
    directory: Annotated[Path, typer.Argument(help="Directory containing gpu_state_*.db files")] = Path("."),
    force: Annotated[bool, typer.Option("--force", help="Overwrite existing Parquet files")] = False,
    db: Annotated[
        Path | None, typer.Option("--db", help="Convert a single .db file instead of scanning directory")
    ] = None,
) -> None:
    """Migrate gpu_state SQLite files to Parquet."""
    if db is not None:
        if not db.exists():
            typer.echo(f"File not found: {db}", err=True)
            raise typer.Exit(1)
        db_files = [db]
    else:
        db_files = sorted(directory.glob("gpu_state_*.db"))
        if not db_files:
            typer.echo(f"No gpu_state_*.db files found in {directory}", err=True)
            raise typer.Exit(1)

    typer.echo(f"Found {len(db_files)} SQLite file(s)\n")

    total_rows = 0
    total_sqlite_bytes = 0
    for db_path in db_files:
        rows, sqlite_bytes = _sqlite_to_parquet(db_path, force)
        total_rows += rows
        total_sqlite_bytes += sqlite_bytes

    if total_rows:
        typer.echo(f"\nTotal: {total_rows:,} rows migrated from {total_sqlite_bytes / 1e6:.1f} MB of SQLite")


if __name__ == "__main__":
    app()
