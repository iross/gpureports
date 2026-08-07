---
id: TASK-49.3
title: Clean up stray data/report artifacts and fix stale operational docs
status: In Progress
assignee:
  - '@claude'
created_date: '2026-07-31 21:00'
updated_date: '2026-08-07 14:38'
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
- [x] #1 test.parquet and gpus_2025-02-27.parquet are removed (or confirmed as needed and relocated to an appropriate fixtures/ directory)
- [x] #2 Generated analysis report markdown files are moved out of the repo root (e.g. into a gitignored output directory or deleted if superseded) or documented as intentional, checked-in artifacts if they should stay
- [ ] #3 OPERATIONS.md and dashboard/README.md accurately describe the collector.py-based Kubernetes deployment instead of the legacy cron/get_gpu_state.py setup
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Part A (do early, independent of 49.1/49.4):
1. Delete test.parquet, gpus_2025-02-27.parquet, gpus_2025-02-27.csv (confirmed gitignored/untracked, no git history, zero code references by filename).
2. Move or delete the root-level one-off report markdown files (bhaskar_report.md, gitter_report.md, isye_report.md, isye_report_plan.md, sqlite_vs_parquet_report_comparison.md) -- relocate anything worth keeping into backlog/docs/ or backlog/decisions/, following the existing precedent there.
3. Also sweep the untracked _polars/_pandas comparison html artifacts at repo root (polars.html, pandas.html, last-day_polars.html, pandas_24h*.html) left over from the TASK-25/40 migration -- generated output, not source.

Part B (do last, after 49.1 and 49.4 land, so it describes the final architecture):
4. Rewrite OPERATIONS.md: replace the get_gpu_state.py/SQLite dataflow diagram, crontab entries, log table, and troubleshooting steps with collector.py's Parquet-based, k8s CronJob-deployed architecture (per backlog/decisions/task-48-dashboard-pvc-concurrency.md).
5. Rewrite dashboard/README.md: remove the stale 'reads gpu_state_YYYY-MM.db'/'not currently deployed' language; describe Parquet-primary with SQLite fallback and the actual current k8s deployment status.
6. Fix root README.md: remove references to usage_stats_polars.py and get_gpu_state_polars.py (neither file exists), point the CLI section at report.py, and update the dataflow diagram and Project Structure section to match the post-49.1/49.4 layout.
7. If the actual k8s manifest/deployment topology isn't concretely knowable from backlog/decisions/task-48 alone, split off a small investigation task rather than write speculative docs.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Part A done (stray-file cleanup): deleted test.parquet, gpus_2025-02-27.parquet/csv (all untracked, no git history, confirmed no code references by filename). Also swept the untracked _polars/_pandas comparison html artifacts at repo root (polars.html, pandas.html, last-day_polars.html, pandas_24h*.html) left over from the TASK-25/40 migration -- generated output, not source, same category as the AC's intent even though not literally markdown. Deleted all 5 root-level report markdown files (bhaskar_report.md, gitter_report.md, isye_report.md, isye_report_plan.md, sqlite_vs_parquet_report_comparison.md) per explicit user decision -- all untracked, reproducible (host reports) or superseded by shipped code/Done task history (plan doc, migration comparison).

Part B (OPERATIONS.md/dashboard/README.md doc rewrite to describe the current collector.py/k8s architecture) intentionally deferred -- per this task's plan, it needs to happen after TASK-49.1 and TASK-49.4 land so it describes the final architecture rather than an intermediate state. Status left as In Progress rather than Done.
<!-- SECTION:NOTES:END -->
