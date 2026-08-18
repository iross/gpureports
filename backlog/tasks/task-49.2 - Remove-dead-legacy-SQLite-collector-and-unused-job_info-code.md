---
id: TASK-49.2
title: Remove dead legacy SQLite collector and unused job_info code
status: Done
assignee:
  - '@claude'
created_date: '2026-07-31 21:00'
updated_date: '2026-08-07 14:31'
labels: []
dependencies:
  - TASK-39
parent_task_id: TASK-49
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
get_gpu_state.py (the pre-Parquet SQLite collector) is not part of the production Docker image and has been superseded by collector.py, but it's still present in the repo along with its now-orphaned collect_job_info()/_parse_schedd_from_job_id() logic (job_info collection has no live consumer once TASK-39 removes the Open Capacity Jobs dashboard tab). Remove or clearly archive this dead code so it doesn't get mistaken for a live code path.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 get_gpu_state.py is removed or moved to archive/, with justfile/docs no longer referencing it as the production collector
- [x] #2 No remaining code references collect_job_info() or job_info_*.db as a live data source
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Confirm (already verified during TASK-49 planning) that no live code reads job_info_*.db or calls collect_job_info() -- TASK-39 already removed the dashboard consumer.
2. Delete get_gpu_state.py.
3. Remove its references from README.md and OPERATIONS.md (crontab entries, log table, troubleshooting steps) and backlog/docs/get_gpu_state_comparison.md.
4. Flag as an operational follow-up (not a repo change): confirm no stale cron entry or k8s CronJob still invokes get_gpu_state.py on the actual deployment host -- the repo deletion alone doesn't guarantee that.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Deleted get_gpu_state.py -- confirmed zero live callers (no test, script, justfile, or cron entry references it or collect_job_info()/job_info_*.db; the only remaining reference was an already-archived one-off script, archive/experiments/analyze.py, left as-is since archive/ is not expected to stay runnable).

Updated README.md (dataflow diagram, Project Structure listing) and OPERATIONS.md (data-flow diagram, crontab block, log table, database-files section, troubleshooting bullets) to drop get_gpu_state.py and point at collector.py/Parquet instead. Did NOT attempt the full k8s-vs-cron architecture rewrite here -- backlog/decisions/task-48-dashboard-pvc-concurrency.md establishes that collector/emailer already run as k8s CronJobs on a PVC, which contradicts OPERATIONS.md's baremetal-crontab framing well beyond just the get_gpu_state.py line; reconciling that fully is TASK-49.3's job (deferred there on purpose, left a pointer comment in OPERATIONS.md).

Added a historical-record note to backlog/docs/get_gpu_state_comparison.md rather than deleting it -- it documents real design reasoning and this repo's convention (backlog/decisions/) is to keep historical writeups after supersession, just marked as such.

Correction to TASK-49/49.1's plans made in passing: while verifying get_gpu_state.py had no live callers, discovered the production email cron (emailer.sh/_emailer.sh) invokes usage_stats.py, not report.py -- and usage_stats.py's only cron-exercised path is already on stats_calculations.py's canonical prepare_frames() pipeline. report.py (TASK-49.1's other target) is a human-run-only CLI tool (just last-day/last-hour), not part of any automated path. Updated TASK-49's risk notes and TASK-49.1's plan to reflect this -- it lowers 49.1's risk profile (no live cron touches report.py) and identifies report.py's group_by_device=False branch as likely fully dead code.

All 96 existing tests pass unchanged.

Modified files: get_gpu_state.py (deleted), README.md, OPERATIONS.md, backlog/docs/get_gpu_state_comparison.md.
<!-- SECTION:NOTES:END -->
