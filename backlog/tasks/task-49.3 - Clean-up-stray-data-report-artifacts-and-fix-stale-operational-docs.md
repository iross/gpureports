---
id: TASK-49.3
title: Clean up stray data/report artifacts and fix stale operational docs
status: To Do
assignee: []
created_date: '2026-07-31 21:00'
labels: []
dependencies: []
parent_task_id: TASK-49
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The repo root has accumulated stray files not meant to be long-term artifacts: test.parquet (24MB test leftover), gpus_2025-02-27.parquet (one-off 12KB file), and several generated analysis reports (bhaskar_report.md, gitter_report.md, isye_report.md, isye_report_plan.md, sqlite_vs_parquet_report_comparison.md) sitting at the repo root instead of a scratch/output location. OPERATIONS.md and dashboard/README.md also still describe get_gpu_state.py/get_job_pressure.py as the production collectors run via crontab, which is stale relative to the actual collector.py Kubernetes CronJob deployment.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 test.parquet and gpus_2025-02-27.parquet are removed (or confirmed as needed and relocated to an appropriate fixtures/ directory)
- [ ] #2 Generated analysis report markdown files are moved out of the repo root (e.g. into a gitignored output directory or deleted if superseded) or documented as intentional, checked-in artifacts if they should stay
- [ ] #3 OPERATIONS.md and dashboard/README.md accurately describe the collector.py-based Kubernetes deployment instead of the legacy cron/get_gpu_state.py setup
<!-- AC:END -->
