---
id: TASK-52
title: Restructure into a src/gpureports/ package and clean up dead code
status: In Progress
assignee:
  - iross
created_date: '2026-08-07 17:11'
updated_date: '2026-08-07 19:16'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The repo is 17+ flat top-level .py files with no installable package (no [build-system] in pyproject.toml), and tests resort to sys.path.insert hacks to import root modules. Decided to move the canonical library code into a real src/gpureports/ package, while keeping production entrypoints (collector.py, get_job_pressure.py, usage_stats.py, report.py, emailer.sh/_emailer.sh) as thin root-level wrapper scripts with stable names/paths -- the k8s CronJob manifests that invoke these live outside this repo and override the container command by exact filename, so those names/paths must not move or change. Also decided to archive dead code found during the audit: website_generator/ (broken -- imports a module that only exists in archive/experiments/, zero callers) and templates/gpu_report.html (zero callers). And to relocate 6 orphaned top-level scripts with zero callers (analyze_pool_health.py, check_unused_gpus.py, migrate_job_pressure.py, open_cap_user_jobs.py, plot_wait_time_trend.py, draining_report.py) into scripts/ alongside the existing manual/analysis tooling -- draining_report.py specifically must be kept (not archived) since task-32 (in-progress) references it as a future refactor source.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 src/gpureports/ package exists containing the canonical pipeline modules (read_data.py, classify_slots.py, reporting.py, devices.py) and the dashboard/ subpackage
- [ ] #2 pyproject.toml has a build-system section making gpureports installable
- [x] #3 The package's modules are named to reflect what they actually do. Done at the flat top-level (not yet moved into src/): stats_data.py->read_data.py, stats_calculations.py->classify_slots.py, stats_reporting.py->reporting.py, device_name_mappings.py->devices.py, gpu_utils.py deleted with its content split -- load_chtc_owned_hosts/load_host_exclusions (config I/O) folded into read_data.py, filter_df/filter_df_enhanced/CLASS_ORDER/UTILIZATION_TYPES/BACKFILL_SLOT_TYPES/HOST_EXCLUSIONS/FILTERED_HOSTS_INFO/analyze_backfill_utilization_by_day (classification logic) folded into classify_slots.py, get_display_name/get_gpu_performance_tier (presentation-only) folded into reporting.py since they're only ever consumed there
- [ ] #4 collector.py, get_job_pressure.py, usage_stats.py, report.py, emailer.sh, _emailer.sh remain at the repo root with unchanged filenames and CLI invocation, as thin wrappers delegating into the package
- [ ] #5 Tests no longer need sys.path.insert to import project modules
- [ ] #6 website_generator/ and templates/gpu_report.html are moved into archive/
- [ ] #7 analyze_pool_health.py, check_unused_gpus.py, migrate_job_pressure.py, open_cap_user_jobs.py, plot_wait_time_trend.py, draining_report.py are relocated into scripts/
- [ ] #8 Dockerfile, Dockerfile.dashboard, justfile, emailer.sh, _emailer.sh, .pre-commit-config.yaml, and all internal imports are updated to match the new layout
- [ ] #9 Existing tests and linters pass
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Naming pass (done, see AC #3)
2. Move renamed modules into src/gpureports/, add pyproject.toml build-system
3. Add thin root-level wrapper scripts for collector.py/get_job_pressure.py/usage_stats.py/report.py that delegate into the package, keeping k8s-visible filenames stable
4. Update Dockerfile/Dockerfile.dashboard/justfile/.pre-commit-config.yaml for the new import paths
5. Relocate orphaned scripts and archive dead code (website_generator/, templates/gpu_report.html)
6. Remove sys.path.insert hacks from tests now that the package is properly importable
7. Run ruff/ty/pytest
<!-- SECTION:PLAN:END -->
