---
id: TASK-49.4
title: Consolidate gpu_utils.py and gpu_utils_polars.py into a single utility module
status: To Do
assignee: []
created_date: '2026-07-31 21:00'
updated_date: '2026-08-06 21:11'
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
- [ ] #1 Every remaining caller of gpu_utils.py is identified and either migrated to gpu_utils_polars.py or documented as a deliberate pandas holdout
- [ ] #2 gpu_utils.py is removed if it has no remaining live callers, or reduced to only the functions that do
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
