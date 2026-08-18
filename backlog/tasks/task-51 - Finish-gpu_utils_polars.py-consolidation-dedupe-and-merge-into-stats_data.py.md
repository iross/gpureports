---
id: TASK-51
title: 'Finish gpu_utils_polars.py consolidation: dedupe and merge into stats_data.py'
status: Done
assignee:
  - iross
created_date: '2026-08-07 16:54'
updated_date: '2026-08-07 17:00'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
TASK-49.4 consolidated gpu_utils.py/gpu_utils_polars.py but left gpu_utils_polars.py carrying dead duplicate host-exclusion config loaders, and left it as a separate module from stats_data.py even though both do gpu_state Parquet/SQLite file discovery. plot_gpu_availability.py also still uses the SQLite-only discovery functions instead of the Parquet-preferring ones, so it likely misses recent data written since the TASK-31 Parquet migration.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 gpu_utils_polars.py's unused duplicate load_chtc_owned_hosts/load_host_exclusions/HOST_EXCLUSIONS/_CHTC_OWNED_HOSTS are removed
- [x] #2 gpu_utils_polars.py's file-discovery functions are merged into stats_data.py and gpu_utils_polars.py is deleted
- [x] #3 dashboard/data.py and scripts/plot_gpu_availability.py import file discovery from stats_data.py
- [x] #4 scripts/plot_gpu_availability.py uses Parquet-preferring discovery instead of the SQLite-only functions
- [x] #5 Dockerfile and Dockerfile.dashboard no longer COPY gpu_utils_polars.py
- [x] #6 existing tests pass and test_parquet_storage.py is updated to import from stats_data
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Read stats_data.py and gpu_utils_polars.py fully to understand naming overlap
2. Move gpu_utils_polars.py's 6 file-discovery functions (get_required_parquet_files, get_most_recent_parquet, get_latest_timestamp_from_most_recent_parquet, get_required_databases, get_most_recent_database, get_latest_timestamp_from_most_recent_db) into stats_data.py, resolving naming collisions with stats_data's existing get_latest_timestamp/parquet_glob
3. Drop the dead load_chtc_owned_hosts/load_host_exclusions/HOST_EXCLUSIONS/_CHTC_OWNED_HOSTS duplicate from gpu_utils_polars.py during the move (they don't get ported, gpu_utils.py's versions are canonical)
4. Delete gpu_utils_polars.py
5. Update dashboard/data.py and scripts/plot_gpu_availability.py imports to stats_data; switch plot_gpu_availability.py to the Parquet-preferring functions
6. Update tests/test_parquet_storage.py imports; update Dockerfile and Dockerfile.dashboard COPY lines
7. Run ruff, ty, and relevant pytest files
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Moved gpu_utils_polars.py's 6 Parquet/SQLite file-discovery functions (get_required_parquet_files, get_most_recent_parquet, get_latest_timestamp_from_most_recent_parquet, get_required_databases, get_most_recent_database, get_latest_timestamp_from_most_recent_db) verbatim into stats_data.py, which already owned the canonical Parquet-loading pipeline. Dropped the module's dead duplicate load_chtc_owned_hosts/load_host_exclusions/HOST_EXCLUSIONS/_CHTC_OWNED_HOSTS -- grep confirmed no caller anywhere imported these from gpu_utils_polars (only from gpu_utils, which stays canonical). Deleted gpu_utils_polars.py.

Updated dashboard/data.py's import to pull the same two aliased names from stats_data instead. Fixed scripts/plot_gpu_availability.py, which had been silently broken against current data: it used the SQLite-only get_required_databases/get_latest_timestamp_from_most_recent_db, and since the TASK-31 migration writes only Parquet, it would find zero files and exit with 'no database files found.' Switched it to the Parquet-preferring functions and taught its load_data() to branch on format (mirroring dashboard/data.py's _query_dbs pattern) so it still reads pre-migration SQLite months too. Verified by running it against real data (Loaded 193,521 rows, plot rendered correctly) -- previously this would have errored immediately.

Updated tests/test_parquet_storage.py's import and section comments from gpu_utils_polars to stats_data. Removed gpu_utils_polars.py from both Dockerfile and Dockerfile.dashboard COPY lines (stats_data.py was already copied in both). Updated README.md's module list/description and gpu_utils.py's module docstring to stop pointing at the now-deleted file.

ruff check/format, ty check, and the full pytest suite (90 passed) all pass. No test changes needed beyond the import swap since behavior is unchanged for every caller except plot_gpu_availability.py's deliberate fix.

Modified: stats_data.py, dashboard/data.py, scripts/plot_gpu_availability.py, tests/test_parquet_storage.py, gpu_utils.py, README.md, Dockerfile, Dockerfile.dashboard. Deleted: gpu_utils_polars.py.
<!-- SECTION:NOTES:END -->
