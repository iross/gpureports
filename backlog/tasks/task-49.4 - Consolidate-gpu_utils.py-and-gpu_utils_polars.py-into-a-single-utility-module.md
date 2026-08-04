---
id: TASK-49.4
title: Consolidate gpu_utils.py and gpu_utils_polars.py into a single utility module
status: To Do
assignee: []
created_date: '2026-07-31 21:00'
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
