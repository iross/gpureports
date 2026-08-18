---
id: TASK-49.1
title: >-
  Unify dashboard, report.py, and stats_data.py onto one canonical query
  pipeline
status: Done
assignee:
  - '@claude'
created_date: '2026-07-31 21:00'
updated_date: '2026-08-07 15:29'
labels: []
dependencies: []
parent_task_id: TASK-49
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
dashboard/data.py, report.py (via gpu_utils_polars.py), and stats_data.py/stats_calculations.py each independently implement loading, host-masking, deduplication, and classification of gpu_state Parquet data, and they've already been shown to disagree (see TASK-47 on dedup rank order). Rebuild dashboard/data.py and report.py on top of stats_data.py's scan_time_filtered() and stats_calculations.py's prepare_frames() instead of maintaining separate implementations, so there is exactly one place that defines 'how gpu_state rows become classified GPU states.'
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 dashboard/data.py's heatmap/counts/users queries are built on scan_time_filtered()/prepare_frames() rather than its own _query_dbs/_dedup_and_bucket/_classify_states
- [x] #2 report.py uses the same canonical pipeline rather than its own separate load path
- [x] #3 Existing dashboard and report behavior (heatmap rendering, email report output) is unchanged for a fixed sample time range, verified by comparing output before/after
- [x] #4 tests/test_pandas_polars_parity.py and tests/test_parquet_storage.py still pass
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
0. Correction from initial planning: the live email cron (emailer.sh/_emailer.sh) calls usage_stats.py, not report.py, and usage_stats.py's only cron-exercised path (group_by_device=True, always passed) is already on scan_time_filtered()/prepare_frames(). report.py is invoked only manually via `just last-day`/`last-day-html`/`last-hour` -- no cron caller. Verified: report.py's group_by_device default is True and no caller anywhere in the repo passes --group-by-device=False, so report.py's group_by_device=False branch has zero live callers and can likely be deleted outright rather than migrated (confirm with a final grep before deleting).
1. Pre-check: confirm no live caller of usage_stats.py's group_by_device=False/timeseries paths (get_time_filtered_data + calculate_allocation_usage_enhanced/calculate_time_series_usage) exists beyond scripts/plot_usage_stats.py and scripts/plot_example.py (both already identified) before deciding whether stats_calculations.py's pandas tail needs migrating or can stay a documented holdout -- this feeds TASK-49.4, not 49.1 itself.
2. Pin 2-3 fixed historical time windows (a plain day, a month-boundary-spanning window to exercise multi-file concat/SQLite-fallback, a window covering a PreventJobsReason period) as literal constants in a throwaway script. Capture JSON snapshots of get_heatmap_data/get_counts_data/get_opencap_users_data and report.py main() output for those windows on main, BEFORE any change.
3. dashboard/data.py: replace _query_dbs/_dedup_and_bucket/_classify_states with scan_time_filtered() + prepare_frames(), plus a thin mapping layer deriving the existing 6 STATE_CODES from prepare_frames's _pp_prio/_is_bf/state columns (fold CHTC-owned into the existing prioritized bucket; check the interactive-slot delta via the snapshot diff rather than assume it's negligible). Switch masked-host loading to gpu_utils.load_host_exclusions() instead of dashboard's own _load_masked_hosts YAML reader.
4. get_counts_data: source its primary/backfill counts from PreparedFrames.raw and PreparedFrames.raw_bf instead of its own load+filter pipeline, preserving the deliberate no-dedup behavior (keep the existing explanatory comment).
5. get_opencap_users_data: replace its standalone file-loading loop and inline dedup with a filter over PreparedFrames.dedup.
6. report.py: delete get_time_filtered_data/get_multi_db_data in favor of stats_data.scan_time_filtered(); delete report.py's own calculate_allocation_usage_by_device_enhanced/calculate_allocation_usage_by_memory in favor of stats_calculations's versions, collapsing main()'s group_by_device=True path to one prepare_frames() call. Delete the now-dead group_by_device=False branch per step 0's finding. Fix report.py's stale module docstring ("For reporting, HTML generation, and email functions, see usage_stats.py (pandas version)") -- usage_stats.py's live path is already polars-based via prepare_frames, not pandas.
7. Re-run the snapshot script on the refactored branch; diff against the BEFORE JSON. Any difference must be either fixed or explicitly documented as a deliberate change (expect one: dashboard's dedup currently truncates-to-bucket before deduping, while prepare_frames dedups at raw timestamp then truncates -- unifying this is a real, small, intentional behavior fix).
8. Rewrite tests/test_parquet_storage.py::TestDashboardQueryDbs and tests/test_dashboard_data.py against the new public entry points (they currently assert on the private _query_dbs/_dedup_and_bucket signatures being removed). Add a small unit test for the state-code mapping table, and fold the snapshot comparison into a permanent regression test (tests/test_report_equivalence.py's synthetic-fixture pattern is the template).
9. Land dashboard changes and report.py changes as separate commits/PRs given their different blast radii. Verify report.py by running `just last-day`/`just last-hour` manually and diffing against the pinned snapshot (no cron involvement -- report.py has no automated caller). The live email cron (usage_stats.py via emailer.sh) is unaffected by this task and needs no separate verification here.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Rebuilt dashboard/data.py and report.py onto stats_calculations.prepare_frames()/stats_data.scan_time_filtered(), verified via before/after JSON snapshots on 3 pinned real-data windows (plain day, month boundary, PreventJobsReason-heavy day) x 3 bucket sizes (5/15/60 min).

dashboard/data.py: removed _dedup_and_bucket/_classify_states/_rank_inputs; get_heatmap_data, get_counts_data, and get_opencap_users_data now all call prepare_frames() and read its _is_bf/_pp_prio/State/_excluded columns instead of re-deriving them. Added _map_state_codes (canonical-columns -> the existing 6 STATE_CODES) and _collapse_to_bucket_winner (a second rank-based collapse needed because prepare_frames() dedups at raw-timestamp granularity while the heatmap needs one winner per display bucket -- reuses slot_dedup_rank, so it's not a second disagreeing implementation). Switched masked-host loading from dashboard's own YAML reader to gpu_utils.load_host_exclusions()/HOST_EXCLUSIONS, matching report.py/usage_stats.py's pattern.

Deliberately KEPT _query_dbs unchanged rather than removing it per the AC's literal wording: it's the only remaining mixed Parquet/SQLite loader in the codebase (stats_data.scan_time_filtered() is Parquet-only), and dashboard's custom date-range picker can reach arbitrarily old months. Removing SQLite fallback wasn't part of this task's intent and risked silently dropping data for old months if any SQLite-only ones still exist in production. report.py's OWN mixed loader (get_multi_db_data) WAS deleted -- its only two live callers (just last-day/last-hour) always query recent, Parquet-only windows.

stats_calculations.prepare_frames() gained a bucket_minutes parameter (default 15, preserving every existing caller's behavior) so dashboard can request its own display-bucket granularity instead of the hardcoded internal 15m truncate.

report.py: deleted get_time_filtered_data/get_multi_db_data, calculate_allocation_usage/calculate_time_series_usage (confirmed zero callers anywhere in the repo -- not even report.py's own main() called them), get_preprocessed_dataframe/clear_dataframe_cache/the two module-level caches (all unused once the above went), and calculate_allocation_usage_by_device_enhanced/calculate_allocation_usage_by_memory (report.py's own naive-rank versions). main() now calls stats_calculations's canonical versions of all four device/memory/h200/backfill-user stats off one prepare_frames() call. Also deleted the group_by_device=False branch and CLI flag: confirmed zero callers pass --group-by-device=False anywhere (justfile always passes --group-by-device, matching the default anyway) -- updated justfile's last-day/last-day-html/last-hour targets to drop the now-nonexistent flag.

Verification for report.py (no pre-existing test coverage existed): ran Current month file not found, using most recent: gpu_state_2026-07.parquet
Loading data (last 24 hours)...
Calculating device statistics...
Calculating user statistics...

======================================================================
                     CHTC GPU UTILIZATION REPORT                      
======================================================================
Period: 24 hours
======================================================================

REAL SLOTS:
----------------------------------------------------------------------
  TOTAL (primary + secondary): 86.3% (330.0/382.3 GPUs)
  Prioritized (TOTAL):          92.8% (218.4/235.3 GPUs)
    Primary:                    33.1% (78.0/235.3 GPUs)
----------------------------------------------------------------------
      Researcher-Owned Hardware: 35.0% (64.1/183.3 GPUs)
      Researcher-Reserved Capacity: 26.7% (13.9/52.0 GPUs)
    Secondary (Backfill):       88.3% (140.4/159.0 GPUs)
      Researcher-Owned Hardware: 88.4% (106.5/120.5 GPUs)
      Researcher-Reserved Capacity: 88.2% (33.9/38.5 GPUs)
  Open Capacity: 75.9% (111.6/147.0 GPUs)

REAL SLOTS BY MEMORY CATEGORY (filtered):
--------------------------------------------------------------------------------
  TOTAL: 50.4% (193.7/384.3 GPUs)
--------------------------------------------------------------------------------
  <48GB: 27.2% (16.2/59.3 GPUs)
  48GB: 54.9% (103.8/189.0 GPUs)
  80GB: 66.8% (34.7/52.0 GPUs)
  >80GB: 47.6% (39.0/82.0 GPUs)
  Unknown: 0.0% (0.0/2.0 GPUs)

H200 USAGE BY SLOT TYPE:
--------------------------------------------------------------------------------

  Open Capacity (31 users): 697.5 GPU-hours (58.1%)
  ------------------------------------------------------------
    jin263@chtc.wisc.edu: 120.7 hrs (10.0%)
    rebarchik@chtc.wisc.edu: 89.1 hrs (7.4%)
    mdangi2@chtc.wisc.edu: 85.9 hrs (7.1%)
    cromp@chtc.wisc.edu: 79.9 hrs (6.7%)
    zxu684@chtc.wisc.edu: 79.2 hrs (6.6%)
    wtang98@chtc.wisc.edu: 44.5 hrs (3.7%)
    pjaiswal2@chtc.wisc.edu: 24.0 hrs (2.0%)
    dehardy2@chtc.wisc.edu: 24.0 hrs (2.0%)
    mgstanley@chtc.wisc.edu: 22.8 hrs (1.9%)
    hcao65@chtc.wisc.edu: 16.8 hrs (1.4%)
    fernandezqui@chtc.wisc.edu: 15.6 hrs (1.3%)
    olayamunoz@chtc.wisc.edu: 15.6 hrs (1.3%)
    rwu246@chtc.wisc.edu: 8.9 hrs (0.7%)
    whong37@chtc.wisc.edu: 8.7 hrs (0.7%)
    cdroberts3@chtc.wisc.edu: 8.2 hrs (0.7%)
    asteinberger@chtc.wisc.edu: 5.9 hrs (0.5%)
    dxu252@chtc.wisc.edu: 5.7 hrs (0.5%)
    cawhittaker@chtc.wisc.edu: 4.9 hrs (0.4%)
    tdrink@chtc.wisc.edu: 4.7 hrs (0.4%)
    pavse@chtc.wisc.edu: 4.5 hrs (0.4%)
    jcloeffler2@chtc.wisc.edu: 4.2 hrs (0.4%)
    jmeng43@chtc.wisc.edu: 4.0 hrs (0.3%)
    xrong8@chtc.wisc.edu: 3.7 hrs (0.3%)
    waymentsteel@chtc.wisc.edu: 3.5 hrs (0.3%)
    yxu649@chtc.wisc.edu: 3.5 hrs (0.3%)
    jloeffler3@chtc.wisc.edu: 2.7 hrs (0.2%)
    cdonahue6@chtc.wisc.edu: 2.0 hrs (0.2%)
    hhuang549@chtc.wisc.edu: 1.7 hrs (0.1%)
    gumina2@chtc.wisc.edu: 1.2 hrs (0.1%)
    thu93@chtc.wisc.edu: 1.0 hrs (0.1%)
    jlundsgaard@chtc.wisc.edu: 0.5 hrs (0.0%)

  Researcher-Owned Hardware (13 users): 361.0 GPU-hours (30.0%)
  ------------------------------------------------------------
    zxu684@chtc.wisc.edu: 115.8 hrs (9.6%)
    rwu246@chtc.wisc.edu: 81.4 hrs (6.8%)
    fernandezqui@chtc.wisc.edu: 46.5 hrs (3.9%)
    hzhang2486@chtc.wisc.edu: 24.0 hrs (2.0%)
    elnesr@chtc.wisc.edu: 18.1 hrs (1.5%)
    czhao276@chtc.wisc.edu: 13.1 hrs (1.1%)
    bkern@grid-submitter.icecube.wisc.edu: 12.9 hrs (1.1%)
    jloeffler3@chtc.wisc.edu: 12.1 hrs (1.0%)
    waymentsteel@chtc.wisc.edu: 12.1 hrs (1.0%)
    xrong8@chtc.wisc.edu: 11.1 hrs (0.9%)
    asteinberger@chtc.wisc.edu: 7.7 hrs (0.6%)
    gumina2@chtc.wisc.edu: 5.2 hrs (0.4%)
    lhan67@chtc.wisc.edu: 1.0 hrs (0.1%)

  Researcher-Owned Hardware (4 users): 79.9 GPU-hours (6.7%)
  ------------------------------------------------------------
    svaren@chtc.wisc.edu: 50.0 hrs (4.2%)
    rhbryant@chtc.wisc.edu: 25.2 hrs (2.1%)
    rho9@chtc.wisc.edu: 3.7 hrs (0.3%)
    cdonahue6@chtc.wisc.edu: 1.0 hrs (0.1%)

  Researcher-Reserved Capacity (1 users): 48.2 GPU-hours (4.0%)
  ------------------------------------------------------------
    fwu89@chtc.wisc.edu: 48.2 hrs (4.0%)

  Researcher-Reserved Capacity (2 users): 14.8 GPU-hours (1.2%)
  ------------------------------------------------------------
    rwu246@chtc.wisc.edu: 13.4 hrs (1.1%)
    fwu89@chtc.wisc.edu: 1.5 hrs (0.1%)

BACKFILL USAGE BY SLOT TYPE (filtered):
--------------------------------------------------------------------------------

  Researcher-Owned Hardware (17 users): 2651.1 GPU-hours (75.7%)
  ------------------------------------------------------------
    fernandezqui@chtc.wisc.edu: 433.7 hrs (12.4%)
    elnesr@chtc.wisc.edu: 420.1 hrs (12.0%)
    jloeffler3@chtc.wisc.edu: 363.7 hrs (10.4%)
    gumina2@chtc.wisc.edu: 292.9 hrs (8.4%)
    bkern@grid-submitter.icecube.wisc.edu: 233.1 hrs (6.7%)
    deng94@chtc.wisc.edu: 194.5 hrs (5.6%)
    waymentsteel@chtc.wisc.edu: 191.8 hrs (5.5%)
    zxu684@chtc.wisc.edu: 124.2 hrs (3.5%)
    xrong8@chtc.wisc.edu: 110.6 hrs (3.2%)
    rwu246@chtc.wisc.edu: 105.4 hrs (3.0%)
    czhao276@chtc.wisc.edu: 59.4 hrs (1.7%)
    iaross@chtc.wisc.edu: 30.4 hrs (0.9%)
    veerannarupa@chtc.wisc.edu: 29.7 hrs (0.8%)
    asteinberger@chtc.wisc.edu: 27.5 hrs (0.8%)
    hzhang2486@chtc.wisc.edu: 24.0 hrs (0.7%)
    lhan67@chtc.wisc.edu: 9.4 hrs (0.3%)
    mhasan32@chtc.wisc.edu: 0.7 hrs (0.0%)

  Researcher-Reserved Capacity (17 users): 850.1 GPU-hours (24.3%)
  ------------------------------------------------------------
    deng94@chtc.wisc.edu: 128.2 hrs (3.7%)
    jloeffler3@chtc.wisc.edu: 105.4 hrs (3.0%)
    elnesr@chtc.wisc.edu: 96.2 hrs (2.7%)
    waymentsteel@chtc.wisc.edu: 93.3 hrs (2.7%)
    bkern@grid-submitter.icecube.wisc.edu: 76.0 hrs (2.2%)
    fernandezqui@chtc.wisc.edu: 72.5 hrs (2.1%)
    gumina2@chtc.wisc.edu: 68.0 hrs (1.9%)
    iaross@chtc.wisc.edu: 47.3 hrs (1.3%)
    veerannarupa@chtc.wisc.edu: 46.3 hrs (1.3%)
    asteinberger@chtc.wisc.edu: 30.9 hrs (0.9%)
    xrong8@chtc.wisc.edu: 24.5 hrs (0.7%)
    czhao276@chtc.wisc.edu: 24.0 hrs (0.7%)
    rwu246@chtc.wisc.edu: 19.3 hrs (0.6%)
    dlal2@chtc.wisc.edu: 8.2 hrs (0.2%)
    lhan67@chtc.wisc.edu: 5.4 hrs (0.2%)
    zxu684@chtc.wisc.edu: 3.2 hrs (0.1%)
    fwu89@chtc.wisc.edu: 1.5 hrs (0.0%)

Usage by Device Type (filtered):
----------------------------------------------------------------------

Researcher-Owned Hardware:
--------------------------------------------------
    A100 40GB: 1.4% (avg 0.3/20.0 GPUs)
    RTX 2080 Ti: 3.5% (avg 0.5/13.3 GPUs)
    H200: 15.1% (avg 3.3/22.0 GPUs)
    NVIDIA H200 NVL: 95.1% (avg 3.8/4.0 GPUs)
    L40: 69.1% (avg 40.1/58.0 GPUs)
    L40S: 31.5% (avg 15.8/50.0 GPUs)
    NVIDIA RTX PRO 6000 Blackwell Server Edition: 2.6% (avg 0.4/16.0 GPUs)
    ------------------------------
    TOTAL Researcher-Owned Hardware: 35.0% (avg 64.1/183.3 GPUs)

Researcher-Reserved Capacity:
--------------------------------------------------
    A100 80GB: 0.0% (avg 0.0/8.0 GPUs)
    H100 80GB: 48.6% (avg 3.9/8.0 GPUs)
    H200: 50.3% (avg 2.0/4.0 GPUs)
    L40S: 25.0% (avg 8.0/32.0 GPUs)
    ------------------------------
    TOTAL Researcher-Reserved Capacity: 26.7% (avg 13.9/52.0 GPUs)

Open Capacity:
--------------------------------------------------
    A100 40GB: 100.0% (avg 4.0/4.0 GPUs)
    A100 80GB: 93.0% (avg 26.1/28.0 GPUs)
    NVIDIA A100-SXM4-80GB MIG 3g.40gb: 100.0% (avg 8.0/8.0 GPUs)
    NVIDIA GB10: 50.0% (avg 1.0/2.0 GPUs)
    RTX 2080 Ti: 24.4% (avg 3.4/14.0 GPUs)
    H100 80GB: 59.8% (avg 4.8/8.0 GPUs)
    H200: 83.7% (avg 28.5/34.0 GPUs)
    L40: 66.7% (avg 18.0/27.0 GPUs)
    L40S: 100.0% (avg 22.0/22.0 GPUs)
    ------------------------------
    TOTAL Open Capacity: 75.9% (avg 111.6/147.0 GPUs)

Researcher-Owned Hardware:
--------------------------------------------------
    A100 40GB: 99.7% (avg 19.8/19.8 GPUs)
    RTX 2080 Ti: 71.5% (avg 10.1/14.1 GPUs)
    H200: 87.5% (avg 14.7/16.8 GPUs)
    NVIDIA H200 NVL: 0.0% (avg 0.0/0.2 GPUs)
    L40: 64.2% (avg 12.0/18.7 GPUs)
    L40S: 97.7% (avg 34.4/35.2 GPUs)
    NVIDIA RTX PRO 6000 Blackwell Server Edition: 99.5% (avg 15.5/15.6 GPUs)
    ------------------------------
    TOTAL Researcher-Owned Hardware: 88.4% (avg 106.5/120.5 GPUs)

Researcher-Reserved Capacity:
--------------------------------------------------
    A100 80GB: 99.9% (avg 8.0/8.0 GPUs)
    H100 80GB: 31.7% (avg 1.4/4.3 GPUs)
    H200: 29.1% (avg 0.6/2.1 GPUs)
    L40S: 99.7% (avg 23.9/24.0 GPUs)
    ------------------------------
    TOTAL Researcher-Reserved Capacity: 88.2% (avg 33.9/38.5 GPUs)

======================================================================
EXCLUDED HOSTS:
  voyles2000: Prioritized via hardcoded start expression, not PrioritizedProjects
  gpulab2003: Interactive slots only.
  gpulab2004: Interactive slots only.

======================================================================
Data Period: 2026-07-30 20:10:14 to 2026-07-31 20:05:16
======================================================================

Total runtime: 0.19 seconds against real production data before/after. Text and HTML output both work; runtime dropped 0.74s -> 0.18s. Device/memory stats numbers DID change (e.g. total Real-slot GPUs 389.3 -> 382.3) -- expected and correct: report.py's old calculate_allocation_usage_by_device_enhanced used a naive 3-tier state_priority (Claimed>Drained>other, no primary/backfill distinction) instead of slot_dedup_rank, exactly the 'third disagreeing implementation' this task's description names. h200_user_stats/backfill_user_stats (which already went through prepare_frames in both old and new report.py) are byte-identical aside from a harmless ordering swap between two users tied at the exact same hours -- confirms the fix is isolated to the previously-naive device/memory path. Added tests/test_report.py (3 cases: text output, html output, no-data-in-window) since report.py had zero tests before this.

Two small, deliberate behavior fixes surfaced by the snapshot diff (both improvements, not regressions): (1) get_heatmap_data's gpu_info lookup had an unordered .unique() that could arbitrarily pick a null GPUs_DeviceName over an available real one depending on row order -- added a nulls-last sort before dedup. (2) get_opencap_users_data's anonymized user-label ranking had no tie-break for users with equal peak GPU counts -- added RemoteOwner as a secondary sort key for determinism. Confirmed via a per-bucket multiset comparison that the underlying counted VALUES were already identical before this fix; only the arbitrary User-N label assignment for ties changed.

Rewrote tests/test_dashboard_data.py (imported the since-removed _dedup_and_bucket; now tests _collapse_to_bucket_winner) and added a unit test for _map_state_codes covering all 6 states plus the na fallback. tests/test_parquet_storage.py::TestDashboardQueryDbs needed no changes since _query_dbs was kept as-is. All 100 tests pass (96 pre-existing + 4 new). Smoke-tested the live dashboard: started uvicorn, curled /api/heatmap, /api/counts, /api/opencap_users, and / -- all 200 with sane data.

Modified files: stats_calculations.py, dashboard/data.py, report.py, justfile, tests/test_dashboard_data.py, tests/test_report.py (new).
<!-- SECTION:NOTES:END -->
