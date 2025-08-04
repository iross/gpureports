---
id: task-10
title: Tools for examining backfill slot availability and eviction patterns
status: To Do
assignee: []
created_date: '2025-08-01'
updated_date: '2025-08-01'
labels: []
dependencies: []
---

## Description
We're interested in examining the state of backfill slots. Specifically, we want to know:
1. The length of time that backfill slots are typically available (e.g. when primary slots are idle)
2. The eviction pattern of jobs running on backfill slots

The first task should be achievable using methods and functions from the usage_stats (and plot_usage_stats) scripts. 


## Implementation Plan
- Reuse as much of the existing code (usage_stats.py first, plot_usage_stats.py second, and logic from gpu_playground.py, analyze.py, or figures.py if it looks especially relevant.)

## Acceptance Criteria

- [ ] Tool can measure average duration of backfill slot availability periods
- [ ] Tool can identify transitions from backfill available to claimed states
- [ ] Tool can calculate eviction frequency for jobs on backfill slots
- [ ] Tool generates summary statistics for backfill slot utilization patterns
- [ ] Tool reuses existing usage_stats.py and plot_usage_stats.py functions
- [ ] Output includes visualization or tabular summary of findings
