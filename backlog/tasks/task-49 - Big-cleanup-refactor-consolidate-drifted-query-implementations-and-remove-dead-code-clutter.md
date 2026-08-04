---
id: TASK-49
title: >-
  Big cleanup/refactor: consolidate drifted query implementations and remove
  dead code/clutter
status: To Do
assignee: []
created_date: '2026-07-31 21:00'
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
- [ ] #1 The codebase has one canonical query/classify/dedup implementation for gpu_state data, not three
- [ ] #2 Dead legacy collector code and unused/stray data files are removed or clearly archived, not left live in the working tree
- [ ] #3 Operational docs (OPERATIONS.md, dashboard/README.md) accurately describe the current collector.py/Parquet-based architecture
- [ ] #4 No pandas/polars utility module exists that duplicates logic already covered by its counterpart
<!-- AC:END -->
