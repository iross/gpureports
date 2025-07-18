---
id: task-5
title: Add host-level summary table
status: Draft
assignee: []
created_date: '2025-07-18'
labels: []
dependencies: []
---

## Description

Add a host-level summary table to the GPU health reports to provide visibility into GPU allocation and utilization at the individual machine level. This will help administrators identify which hosts are heavily utilized vs underutilized, spot potential hardware issues, and make informed decisions about resource allocation and maintenance scheduling.

Currently, the reports show device-type summaries (e.g., RTX 4090, A100) grouped by allocation categories, but don't show which physical hosts these GPUs are located on or how individual machines are performing.

**Important**: Unlike the statistical aggregation tables that exclude certain hosts to maintain data quality, the host-level table should include ALL hosts in the cluster (including excluded ones) since operational visibility into every machine is important for administrators. Excluded hosts will be visually distinguished to indicate their special status.

## Acceptance Criteria

- [ ] Host summary table displays each machine in the cluster with its GPU allocation status
- [ ] Each host shows total GPU count, allocated (avg.), available (avg.), and allocation percentage
- [ ] Hosts are grouped by device type, then ordered alphabetically by hostname within each group
- [ ] Table shows device type as a group header with individual hosts listed under each type
- [ ] Host table appears below the "Cluster Summary" section but above the "Excluded Hosts" table
- [ ] Host summary includes ALL hosts (including those excluded from statistical aggregations)
- [ ] Excluded hosts are visually marked or styled differently to indicate their exclusion status
- [ ] Allocation percentages use the same calculation methodology as existing device tables
- [ ] Table styling matches existing report aesthetics (colors, fonts, alignment)
- [ ] Host data is included in email reports without overwhelming the message size

## Implementation Plan

1. **Create host-level calculation function**
   - Add `calculate_allocation_usage_by_host()` function in `usage_stats.py`
   - Group data first by device type (`GPUs_DeviceName`), then by `Machine` field
   - Aggregate GPU counts and calculate allocation percentages per host
   - Return data structured by device type with hosts nested within each type

2. **Integrate host calculations into analysis pipeline**
   - Modify the main analysis function to optionally include host-level stats
   - Add `host_stats` key to results dictionary alongside existing `device_stats`
   - Process ALL hosts (do NOT apply host exclusion filters for this table)
   - Mark excluded hosts in the data structure for visual styling

3. **Update HTML report generation**
   - Add new "Host Summary" section to `generate_html_report()` function
   - Position the table below "Cluster Summary" section but above "Excluded Hosts" table
   - Create grouped table with device type headers and host rows: Host, Allocated %, Allocated (avg.), Available (avg.)
   - Apply consistent styling and right-alignment for numeric columns
   - Style excluded hosts differently (e.g., muted colors, italics, or strikethrough)

4. **Handle device type grouping and exclusion styling**
   - Create device type group headers (e.g., "RTX 4090 Hosts", "A100 Hosts")
   - List individual hosts alphabetically under each device type group
   - Use sub-table or grouped row styling similar to existing device breakdown tables
   - Apply visual indicators for excluded hosts (background color, text styling, or annotation)

5. **Update email formatting**
   - Include host table in email reports
   - Consider adding a host count summary to email subject if requested
   - Ensure table remains readable in email clients

6. **Testing and validation**
   - Comprehensive testing will be covered in a separate testing task
   - Manual testing during development to verify basic functionality
   - Integration testing with existing report generation pipeline
