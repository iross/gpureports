---
id: task-10
title: Tools for examining backfill slot availability and eviction patterns
status: In Progress
assignee:
  - '@claude'
created_date: '2025-08-01'
updated_date: '2025-08-04'
labels: []
dependencies: []
---

## Description
We're interested in examining the state of backfill slots. Specifically, we want to know:
1. The length of time that backfill slots are typically available (e.g. when primary slots are idle)
2. The eviction pattern of jobs running on backfill slots

The first task should be achievable using methods and functions from the usage_stats (and plot_usage_stats) scripts. 


## Implementation Plan

### Phase 1: Data Acquisition and Preprocessing

1.  **Elasticsearch Job Data Loader (`es_data_loader.py`)**:
    *   Create a new Python module, `es_data_loader.py`, to house functions for querying Elasticsearch.
    *   This module will contain a function (e.g., `load_job_data(start_time, end_time)`) that wraps the Elasticsearch client and `es_query` logic from `scripts/gpu_playground.py`.
    *   It will fetch relevant job records (e.g., `JobStartDate`, `CompletionDate`, `AssignedGPUs`, `StartdName`, `JobStatus`, `GlobalJobId`, `RemoteOwner`) for a given time range, returning them as a Pandas DataFrame.

2.  **GPU State Data Loading**:
    *   Continue to use `usage_stats.get_time_filtered_data()` to load the 15-minute GPU state snapshots from the SQLite databases.

3.  **Data Harmonization and Linking**:
    *   Develop a new function (e.g., `link_gpu_state_with_jobs`) that takes both the `gpu_state` DataFrame and the `es_job_data` DataFrame.
    *   For each unique `AssignedGPUs` ID, it will process its `gpu_state` records chronologically.
    *   When a `gpu_state` record indicates a `Backfill` slot is `Claimed`, it will attempt to find the corresponding job in the `es_job_data` based on `AssignedGPUs`, `Machine`, and `timestamp` proximity (e.g., `JobStartDate` from ES is within the 15-minute window of the `gpu_state` snapshot).
    *   The goal is to enrich the `gpu_state` records with precise `JobStartDate` and `CompletionDate` for claimed backfill slots, allowing for more accurate duration calculations.

### Phase 2: Backfill Availability Duration Analysis

4.  **Availability Period Calculation**:
    *   Create a function (e.g., `calculate_backfill_availability_durations`) that iterates through the linked GPU state and job data for each unique GPU.
    *   It will identify periods where a GPU is in an "Unclaimed" state and is available for backfill (i.e., `Name` contains "backfill" and `State` is "Unclaimed").
    *   The start of an availability period is the timestamp of the `Unclaimed` record.
    *   The end of an availability period is the `JobStartDate` of the *next* linked backfill job, or the timestamp of the next `Claimed` record (if no job link is found), or the timestamp of the next `Unclaimed` record (if the slot becomes available again without being claimed), or the end of the analysis period.
    *   Calculate the duration of each identified availability period.

5.  **Statistical Aggregation**:
    *   Compute summary statistics for these durations: average, median, min, max, and standard deviation.
    *   Group these statistics by `GPUs_DeviceName` and potentially by machine type (priority vs. shared, derived from `PrioritizedProjects` in `gpu_state`).

### Phase 3: Reporting and Integration

6.  **Extend `usage_stats.py`**:
    *   Add a new analysis type (e.g., `--analysis-type backfill_availability_duration`) to the `main` function.
    *   Integrate the new `es_data_loader` and analysis functions.
    *   Present the results in a clear, tabular format (both text and HTML output).

7.  **Documentation Update**:
    *   Update `backlog/tasks/task-10.md` with the `Implementation Notes` once the work is complete.

## Acceptance Criteria

- [ ] Tool can measure average duration of backfill slot availability periods
- [ ] Tool can identify transitions from backfill available to claimed states
- [ ] Tool can calculate eviction frequency for jobs on backfill slots
- [ ] Tool generates summary statistics for backfill slot utilization patterns
- [ ] Tool reuses existing usage_stats.py and plot_usage_stats.py functions
- [ ] Output includes visualization or tabular summary of findings
