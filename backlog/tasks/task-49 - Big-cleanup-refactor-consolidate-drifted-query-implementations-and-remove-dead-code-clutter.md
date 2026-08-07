---
id: TASK-49
title: >-
  Big cleanup/refactor: consolidate drifted query implementations and remove
  dead code/clutter
status: Done
assignee:
  - '@claude'
created_date: '2026-07-31 21:00'
updated_date: '2026-08-07 16:00'
labels: []
dependencies:
  - TASK-45
  - TASK-46
  - TASK-47
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
This codebase has accumulated real bloat, mostly from logic that got copied and evolved independently rather than refactored: three separate, disagreeing implementations of the gpu_state query/classify/dedup pipeline (stats_data.py + stats_calculations.py, report.py + gpu_utils_polars.py, dashboard/data.py); two parallel utility modules (gpu_utils.py pandas vs gpu_utils_polars.py polars) that drift apart because fixes only land in one (e.g. the host-exclusion regex fix); a legacy SQLite collector (get_gpu_state.py) that's dead in production but still carries unused job_info collection code; stray leftover files (test.parquet, gpus_2025-02-27.parquet); and several one-off analysis report markdown files sitting at the repo root instead of somewhere structured. The targeted fixes already queued in TASK-45/46/47 patch specific drift symptoms without addressing the underlying duplication -- this task is the deferred, full consolidation pass. Because the email-report cron and dashboard both depend on parts of this code, this needs a deliberate, staged approach rather than a single sweeping rewrite -- see subtasks.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The codebase has one canonical query/classify/dedup implementation for gpu_state data, not three
- [x] #2 Dead legacy collector code and unused/stray data files are removed or clearly archived, not left live in the working tree
- [x] #3 Operational docs (OPERATIONS.md, dashboard/README.md) accurately describe the current collector.py/Parquet-based architecture
- [x] #4 No pandas/polars utility module exists that duplicates logic already covered by its counterpart
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Land TASK-50 (Dockerfile.dashboard hotfix) first, independent of this refactor.
2. Do TASK-49.2 (remove get_gpu_state.py) and the stray-file half of TASK-49.3 in parallel/either order -- both are cheap and fully independent.
3. Do TASK-49.1 (unify dashboard/data.py + report.py onto scan_time_filtered()/prepare_frames()) before TASK-49.4. prepare_frames() currently depends on gpu_utils.HOST_EXCLUSIONS/load_chtc_owned_hosts; consolidating gpu_utils first would make 49.1 chase a moving dependency target. Doing 49.1 first means gpu_utils.py stays fixed while dashboard/report.py converge onto the canonical pipeline, so 49.4 only has one remaining call-site pattern to migrate instead of several independently-evolved ones.
4. Do TASK-49.4 (consolidate gpu_utils.py into gpu_utils_polars.py, then rename to plain gpu_utils.py) after 49.1.
5. Do the doc-rewrite half of TASK-49.3 (OPERATIONS.md, dashboard/README.md, root README.md) last, once the final architecture (single pipeline, single utils module) actually exists to describe -- writing it earlier means rewriting it twice.

Risk notes: correcting an assumption from initial planning -- the live email-report cron (emailer.sh/_emailer.sh) invokes usage_stats.py, NOT report.py, and usage_stats.py's only cron-exercised path (group_by_device=True, the default, is what every emailer.sh call passes) is already built on scan_time_filtered()/prepare_frames(). report.py is a secondary, human-run-only CLI tool (just last-day/last-day-html/last-hour) with no cron caller, so 49.1's report.py changes are lower-stakes than initially assessed -- verify by manually running those just targets, not by touching the production cron. The live dashboard is still a real (lower-stakes, read-only) risk for 49.1's dashboard/data.py changes. 49.2's repo deletion doesn't guarantee a stale cron entry isn't still running get_gpu_state.py on the actual host outside this repo -- flag as an operational check, not assume it's covered.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
All four subtasks (49.1-49.4) done, in the planned order: Dockerfile.dashboard hotfix (TASK-50) -> 49.2/49.3a (dead code + stray files) -> 49.1 (canonical pipeline unification, verified via before/after snapshots on real data) -> 49.4 (gpu_utils consolidation, scope corrected mid-flight based on a precise caller audit) -> 49.3b (doc rewrite, based on concrete CI/decision-doc evidence).

One deliberate deviation from the original plan, documented in 49.1's and 49.4's own notes: dashboard/data.py keeps its own Parquet+SQLite mixed loader (_query_dbs) rather than fully collapsing onto stats_data.scan_time_filtered() (Parquet-only), since the dashboard's custom date-range picker can reach arbitrarily old SQLite-only months and dropping that silently wasn't part of this task's intent. report.py's equivalent loader WAS deleted since its only callers only ever query recent, Parquet-only windows. AC #1 is satisfied in substance (dedup/classify is canonical everywhere; only the load-path's SQLite fallback legitimately varies by caller need).

Second deviation, documented in 49.4's notes: gpu_utils_polars.py was NOT renamed to plain gpu_utils.py. That rename's premise (gpu_utils.py having zero remaining callers) doesn't hold -- gpu_utils.py keeps real pandas-specific content (filter_df/filter_df_enhanced) with genuine live callers that can't safely move (they read host-exclusion state as plain module globals). AC #4 is satisfied without the rename: no DataFrame-processing duplication remains between the two modules after removing gpu_utils_polars.py's dead functions.

All 90 tests pass. Live dashboard smoke-tested end-to-end (uvicorn + curl on all 3 API endpoints). report.py smoke-tested against real production data before/after.
<!-- SECTION:NOTES:END -->
