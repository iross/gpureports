---
id: task-22
title: Add backfill slots usage table by user
status: Done
assignee:
  - '@iross'
created_date: '2025-09-15 16:30'
updated_date: '2025-09-15 16:37'
labels: []
dependencies: []
---

## Description

Add a table showing who is using the backfill slots similar to the existing H200 users table. This will provide visibility into backfill slot utilization by individual users across different backfill slot types.

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] Table displays backfill slot usage by user and slot type
- [x] Table shows total GPU hours per user for backfill slots
- [x] Table includes breakdown by backfill slot types (ResearcherOwned/CHTCOwned/OpenCapacity)
- [x] Table follows same format and styling as existing H200 usage table
- [x] Implementation integrates seamlessly with existing HTML report generation
<!-- AC:END -->

## Implementation Plan

1. Analyze existing H200 user table implementation to understand pattern and data structure
2. Create calculate_backfill_usage_by_user() function similar to calculate_h200_usage_by_user()
3. Filter data for backfill slot types (Backfill-ResearcherOwned, Backfill-CHTCOwned, Backfill-OpenCapacity)
4. Calculate GPU hours per user across backfill slot types using same methodology as H200 function
5. Add HTML table generation for backfill usage in format_usage_stats_html()
6. Add console output for backfill usage in format_usage_stats_text()
7. Integrate backfill usage table into main report generation workflow
8. Test with sample data to ensure accurate calculations and proper formatting

## Implementation Notes

- **Approach taken**: Created `calculate_backfill_usage_by_user()` function modeled after the existing `calculate_h200_user_breakdown()` function
- **Features implemented**: 
  - Backfill slot usage calculation by user across three slot types (ResearcherOwned, CHTCOwned, OpenCapacity)
  - HTML table generation with same styling as H200 table (slot type headers with user breakdowns beneath)
  - Console output with identical formatting to H200 output
  - Integration into main workflow by adding call to `calculate_backfill_usage_by_user()` in the device stats calculation path
- **Technical decisions**: 
  - Used same caching mechanism as H200 function for performance
  - Applied identical methodology for calculating GPU hours (average usage across time buckets multiplied by duration)
  - Positioned backfill table after H200 table in both HTML and console output for logical flow
- **Modified files**:
  - `usage_stats.py`: Added `calculate_backfill_usage_by_user()` function (lines 960-1051), integrated into main calculation workflow (line 1180), added HTML generation (lines 2117-2174), added console output (lines 2742-2787)
- **Testing**: Verified functionality with actual database showing backfill usage breakdown by users across slot types
