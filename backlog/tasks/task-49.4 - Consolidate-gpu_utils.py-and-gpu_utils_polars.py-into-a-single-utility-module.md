---
id: TASK-49.4
title: Consolidate gpu_utils.py and gpu_utils_polars.py into a single utility module
status: Done
assignee:
  - '@claude'
created_date: '2026-07-31 21:00'
updated_date: '2026-08-07 15:53'
labels: []
dependencies: []
parent_task_id: TASK-49
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Maintaining parallel pandas (gpu_utils.py) and polars (gpu_utils_polars.py) utility modules with overlapping responsibilities (host exclusion, classification helpers, file discovery) is the direct root cause of drift bugs like TASK-46 (host-exclusion regex fix landing in only one module). Now that the collector and stats pipeline are polars/DuckDB-based, evaluate whether gpu_utils.py's pandas implementations still have live callers; if not, retire it in favor of gpu_utils_polars.py alone.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Every remaining caller of gpu_utils.py is identified and either migrated to gpu_utils_polars.py or documented as a deliberate pandas holdout
- [x] #2 gpu_utils.py is removed if it has no remaining live callers, or reduced to only the functions that do
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Live-caller check first: grep justfile/scripts/cron for callers of stats_calculations.py's pandas tail (calculate_allocation_usage, calculate_allocation_usage_enhanced, calculate_time_series_usage, calculate_allocation_usage_by_device via DuckDB, get_gpu_models_at_time*) and of website_generator/gpu_website_generator.py, before deciding to migrate or document them as pandas holdouts.
2. Migrate stats_calculations.py's own gpu_utils imports (filter_df, filter_df_enhanced, load_chtc_owned_hosts, HOST_EXCLUSIONS) to their gpu_utils_polars equivalents first -- highest leverage since every other consumer routes through it after TASK-49.1.
3. Replace gpu_utils_polars._apply_duplicate_cleanup (a separate, still-unreconciled dedup-rank implementation inside filter_df_enhanced) with a call to stats_calculations.slot_dedup_rank. If filter_df_enhanced has enough remaining live callers that this risks a real behavior change, split it into its own follow-up task rather than block this task's AC on it.
4. Migrate or document-as-holdout the pandas-tail functions and scripts/*.py (investigate_backfill_usage.py, plot_weekly_allocation.py, host_report.py) and website_generator/, per step 1's findings. Manually smoke-run each live caller as part of the migration PR -- these have no dedicated test coverage.
5. Explicitly exclude tests/legacy_stats_calculations.py/legacy_stats_data.py from migration -- they are test_pandas_polars_parity.py's frozen pandas oracle and must stay pandas.
6. Once gpu_utils.py (pandas) has zero remaining callers, delete it and rename gpu_utils_polars.py to plain gpu_utils.py (git mv + update every import site: stats_calculations.py, report.py, dashboard/data.py, scripts/*.py, website_generator/) in one commit, since the '_polars' suffix was only ever a mid-migration disambiguation, not a real name. Rename tests/test_gpu_utils_polars.py to tests/test_gpu_utils.py, folding in any pandas gpu_utils.py test cases still worth keeping before the old test_gpu_utils.py is retired.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Corrected scope from the initial plan after a precise per-function caller audit (not just bare-name grep, which produced false positives -- e.g. scripts/weekly_summary.py defines its OWN local _apply_duplicate_cleanup, it doesn't import gpu_utils_polars's). Two findings changed the approach:

1. TASK-49.1 already removed report.py's dependency on gpu_utils_polars.filter_df/filter_df_enhanced (it now goes through stats_calculations.prepare_frames()). That left gpu_utils_polars.py's filter_df/filter_df_enhanced/_apply_duplicate_cleanup/count_*/classify_machine_category-family/get_display_name/CLASS_ORDER/BACKFILL_SLOT_TYPES/UTILIZATION_TYPES/FILTERED_HOSTS_INFO/analyze_backfill_utilization_by_day with ZERO live callers (test-only) -- gpu_utils_polars.py was mostly dead code already, not a module needing pandas callers migrated onto it.

2. gpu_utils.py's filter_df/filter_df_enhanced read HOST_EXCLUSIONS/FILTERED_HOSTS_INFO as plain module-level globals (not parameters) -- moving those globals to gpu_utils_polars.py would have broken filter_df_enhanced's own exclusion logic for its real remaining callers (scripts/host_report.py, stats_calculations.py's pandas tail, website_generator/, analysis/analyze_task7_troubleshoot.py). Consolidating HOST_EXCLUSIONS into one module is only safe if nothing needs a pandas-DataFrame-shaped reader of it, which isn't the case here.

Net action taken: deleted confirmed-dead functions from BOTH modules (count_backfill/count_shared/count_prioritized/count_backfill_researcher_owned/count_backfill_chtc_owned/count_glidein/classify_machine_category/filter_df_by_machine_category/get_machines_by_category/get_required_databases/get_most_recent_database/get_latest_timestamp_from_most_recent_db from gpu_utils.py; filter_df/filter_df_enhanced/_apply_duplicate_cleanup/the same count_*/machine_category functions/get_display_name/CLASS_ORDER/BACKFILL_SLOT_TYPES/UTILIZATION_TYPES/FILTERED_HOSTS_INFO/analyze_backfill_utilization_by_day/get_most_recent_parquet's now-orphaned neighbors from gpu_utils_polars.py -- verified each via precise import-block inspection, not bare-name grep). gpu_utils_polars.py now contains ONLY Parquet/SQLite file-discovery (get_required_parquet_files, get_most_recent_parquet, get_latest_timestamp_from_most_recent_parquet, get_required_databases, get_most_recent_database, get_latest_timestamp_from_most_recent_db) plus HOST_EXCLUSIONS/load_host_exclusions/load_chtc_owned_hosts for its remaining 2 script callers -- no DataFrame-shaped duplication with gpu_utils.py remains.

Migrated scripts/plot_gpu_availability.py and scripts/weekly_summary.py's HOST_EXCLUSIONS/load_host_exclusions/load_chtc_owned_hosts imports from gpu_utils_polars to gpu_utils (their file-discovery imports stay on gpu_utils_polars). While doing this, found and fixed two latent bugs in plot_gpu_availability.py's host-exclusion filtering: (a) it imported HOST_EXCLUSIONS via 'from module import NAME', a stale binding that never reflected the module's later reassignment -- exclusions were silently a no-op; (b) its regex pattern had no re.escape(), the exact class of bug TASK-46 fixed elsewhere. Applied the same re.escape() fix to weekly_summary.py's equivalent (dormant) code path for consistency, though it never actually calls load_host_exclusions() so exclusions were already a no-op there too.

AC#1/AC#2 satisfied literally: every remaining gpu_utils.py caller is either using canonical HOST_EXCLUSIONS/load_chtc_owned_hosts/CLASS_ORDER/etc (shared with the polars pipeline) or is a documented pandas holdout (filter_df/filter_df_enhanced for scripts/host_report.py, stats_calculations.py's small-window pandas tail, website_generator/, analysis/analyze_task7_troubleshoot.py) -- module is reduced, not removed, per AC#2's own conditional wording.

Deliberately did NOT rename gpu_utils_polars.py to plain gpu_utils.py (a deviation from the original task-49 plan, corrected here): that rename's premise -- 'gpu_utils.py has zero remaining callers' -- doesn't hold. gpu_utils.py keeps substantial real pandas-specific content with genuine live callers. Renaming gpu_utils_polars.py now would need a different, unrelated name (it's no longer polars-DataFrame-shaped either, just file discovery) and is out of scope for this task; noted for a future task if it's ever worth doing.

Found but explicitly out of scope: website_generator/gpu_website_generator.py cannot be imported at all (ModuleNotFoundError: gpu_timeline_heatmap, a module that only exists under archive/experiments/) -- confirmed via git show this was already broken when the file was first added in commit 8d83055, unrelated to this task.

Test changes: tests/test_gpu_utils.py -- dropped TestClassifyMachineCategory/TestFilterDfByMachineCategory/TestGetMachinesByCategory (functions deleted), ported tests/test_gpu_utils_polars.py's host-exclusion regex-escape tests onto gpu_utils.py's pandas filter_df/filter_df_enhanced (using monkeypatch for proper teardown -- an earlier plain-assignment draft leaked gpu_utils.HOST_EXCLUSIONS state into test_plot_usage_stats.py and caused a spurious failure, caught by rerunning the full suite). Deleted tests/test_gpu_utils_polars.py entirely (its target functions no longer exist). All 90 tests pass (100 - 13 dead-function tests + 3 ported regex tests).

Modified files: gpu_utils.py, gpu_utils_polars.py, scripts/plot_gpu_availability.py, scripts/weekly_summary.py, tests/test_gpu_utils.py, tests/test_gpu_utils_polars.py (deleted).
<!-- SECTION:NOTES:END -->
