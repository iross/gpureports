---
id: TASK-49.2
title: Remove dead legacy SQLite collector and unused job_info code
status: To Do
assignee: []
created_date: '2026-07-31 21:00'
updated_date: '2026-08-06 21:11'
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
- [ ] #1 get_gpu_state.py is removed or moved to archive/, with justfile/docs no longer referencing it as the production collector
- [ ] #2 No remaining code references collect_job_info() or job_info_*.db as a live data source
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Confirm (already verified during TASK-49 planning) that no live code reads job_info_*.db or calls collect_job_info() -- TASK-39 already removed the dashboard consumer.
2. Delete get_gpu_state.py.
3. Remove its references from README.md and OPERATIONS.md (crontab entries, log table, troubleshooting steps) and backlog/docs/get_gpu_state_comparison.md.
4. Flag as an operational follow-up (not a repo change): confirm no stale cron entry or k8s CronJob still invokes get_gpu_state.py on the actual deployment host -- the repo deletion alone doesn't guarantee that.
<!-- SECTION:PLAN:END -->
