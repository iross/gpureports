---
id: task-27
title: Build live web dashboard for CHTC GPU state
status: In Progress
assignee:
  - '@claude'
created_date: '2026-02-24 19:29'
updated_date: '2026-02-24 21:24'
labels:
  - dashboard
  - web
  - kubernetes
dependencies:
  - task-2.3
priority: medium
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Once task-2.3 is complete, GPU state database files will be available on a Kubernetes cluster. Build a web application that reads directly from those .db files to provide a near-live view of GPU utilization, allocation, and health across CHTC. The dashboard should update frequently (e.g., every few minutes, matching the data collection cadence) and give operators a quick visual overview without needing to run CLI reports.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Dashboard displays current GPU state (claimed/unclaimed, slot type, device type) pulled live from the SQLite db files
- [ ] #2 Data refreshes automatically at an interval matching the collection cadence (no manual reload required)
- [ ] #3 View is filterable or navigable by GPU category (Prioritized, Open Capacity, Backfill) and/or host
- [ ] #4 Deployable as a Kubernetes workload (Deployment + Service) that mounts the db files as a volume
- [ ] #5 Page loads and renders in under 3 seconds for a typical dataset size
- [ ] #6 Dashboard includes total count charts, similar to those generated in scripts/plot_gpu_availability.py
- [ ] #7 A minimal DOckerfile should be created, with CI built so that when the src code is updated, the Docker image is rebuilt and pushed to a registry. This should only trigger if the src code for the dashboard is updated, not ALL pushes to main. 
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add _prepare_bucketed() helper to dashboard/data.py to share load+mask+dedup+classify logic\n2. Add get_counts_data() to dashboard/data.py for time-series GPU counts per category\n3. Add /api/counts endpoint to dashboard/server.py\n4. Add tab bar (Heatmap|Charts), category filter buttons, auto-refresh, and Chart.js count charts to frontend\n5. Update style.css for tabs, category filter, charts layout\n6. Create Dockerfile (local only, CI deferred until k8s piece is ready)
<!-- SECTION:PLAN:END -->
