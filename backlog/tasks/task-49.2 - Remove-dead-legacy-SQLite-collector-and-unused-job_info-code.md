---
id: TASK-49.2
title: Remove dead legacy SQLite collector and unused job_info code
status: To Do
assignee: []
created_date: '2026-07-31 21:00'
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
