---
id: task-7
title: 'Bug: suspicious backfill numbers'
status: Done
assignee: []
created_date: '2025-07-28'
updated_date: '2025-07-30'
labels: []
dependencies: []
---

## Description
There are more backfill GPUs being listed as available than there are prioritized GPUs. Investigate the reason why.

**UPDATE** 2025-07-28 15:51:05 - Seems to be mostly attributed to cluster config -- https://opensciencegrid.atlassian.net/browse/INF-3139

## Analysis

Based on code review of the GPU monitoring system, here are potential causes for this suspicious behavior:

### **Root Cause Analysis**

**Expected Behavior:**
- Backfill GPUs should be a subset of idle Prioritized GPUs only
- Total backfill capacity should never exceed total Prioritized capacity
- Backfill represents opportunistic use of otherwise idle Prioritized resources (NOT Shared resources)

**Potential Issues Identified:**

1. **Data Collection Logic in `get_gpu_state.py`:**

        # Line 34: Backfill slots don't actually have these GPUs assigned, but for ease downstream, we'll pretend.
        df.loc[df['Name'].str.contains('backfill'), 'AssignedGPUs'] = df.loc[df['Name'].str.contains('backfill'), 'AvailableGPUs']

    - This assigns `AvailableGPUs` to `AssignedGPUs` for backfill slots
    - May be creating artificial GPU assignments that don't represent physical hardware

2. **Filter Logic in `filter_df()` Function:**

        # Priority filter (lines 77-80) includes backfill slots under certain conditions:
        elif state == "Unclaimed":
            df = df[((df['PrioritizedProjects'] != "") & ... & (~df['Name'].str.contains("backfill"))) |
                    ((df['PrioritizedProjects'] != "") & (df['State'] == "Claimed") & ... & (df['Name'].str.contains("backfill")))]

    - Priority category includes backfill slots when claimed by prioritized projects
    - This could cause double-counting or unexpected interactions

3. **Backfill Classification Logic:**

        # Line 55: Backfill filter
        if utilization == "Backfill":
            df = df[(df['State'] == state if state != "" else True) & ... & (df['Name'].str.contains("backfill"))]

    - Backfill is classified purely by slot name containing "backfill"
    - No validation that these slots correspond to actual idle Prioritized resources

### **Possible Scenarios Causing the Issue:**

1. **Virtual Backfill Slots:** HTCondor might be advertising more backfill slots than there are actual idle Prioritized GPUs, creating phantom capacity

2. **Timing Mismatches:** 15-minute data collection intervals might capture inconsistent states where backfill slots appear available but Priority slots show as busy

3. **Configuration Issues:** Cluster configuration might have dedicated backfill slots that aren't tied to Prioritized resources

4. **Data Processing Bug:** The DataFrame exploding logic (line 39) combined with backfill GPU assignment (line 34) might be creating duplicate or incorrect entries

### **Investigation Steps Needed:**

1. **Raw Data Examination:** 
    - Check HTCondor raw data to see actual slot configurations
    - Verify if backfill slots correspond to real physical GPUs
    - Look for slot naming patterns and their relationship to Priority/Shared slots

2. **Data Flow Tracing:**
    - Track how backfill GPU counts are calculated vs Prioritized counts specifically
    - Verify the DataFrame processing doesn't create artificial entries
    - Check if the same physical GPU appears in multiple categories

3. **Temporal Analysis:**
    - Check if the issue occurs consistently or only during specific time periods
    - Look for patterns related to cluster utilization levels

4. **Configuration Review:**
    - Examine HTCondor configuration for backfill vs prioritized slot relationships
    - Verify if the cluster has dedicated backfill hardware vs opportunistic use of idle prioritized resources

### **Expected Fix Categories:**

- **Data Collection:** Correct the backfill GPU assignment logic if it's creating phantom entries
- **Filtering Logic:** Ensure proper mutual exclusivity between categories
- **Validation:** Add sanity checks that backfill ≤ Prioritized idle capacity
- **Reporting:** Add debugging output to trace GPU counts through the calculation pipeline
