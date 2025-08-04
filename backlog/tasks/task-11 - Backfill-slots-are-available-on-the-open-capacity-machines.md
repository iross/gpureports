---
id: task-11
title: Backfill slots are available on the open capacity machines
status: Done
assignee:
  - '@claude'
created_date: '2025-08-01'
updated_date: '2025-08-04'
labels: []
dependencies: []
---

## Description

The current cluster configuration allows backfill slots to be available on both prioritized machines AND open capacity (shared) machines when their GPUs are idle. This task ensures that our monitoring system correctly handles and displays this behavior consistently throughout the codebase, reporting, and analysis tools.

## Implementation Plan

1. **Analyze current backfill logic in gpu_utils.py**:
   - Review `filter_df()` function's backfill identification logic (`df['Name'].str.contains("backfill")`)
   - Examine Priority logic that handles priority GPUs used for backfill jobs
   - Verify that Shared logic correctly excludes backfill slots to avoid double-counting

2. **Audit counting and filtering functions**:
   - Test `count_backfill()`, `count_shared()`, `count_prioritized()` functions with real data
   - Verify that backfill slots from both open capacity and prioritized machines are counted
   - Check for any double-counting issues between categories

3. **Validate usage calculation functions in usage_stats.py**:
   - Review `calculate_allocation_usage()` and related functions
   - Ensure backfill calculations work correctly for both machine types
   - Test `calculate_unique_cluster_totals_from_raw_data()` to avoid double-counting

4. **Create test scenarios**:
   - Write tests with mock data showing backfill on both open capacity and priority machines
   - Validate that cluster totals are calculated correctly without double-counting
   - Test edge cases where priority GPUs are simultaneously available for backfill

5. **Update reporting and documentation**:
   - Verify HTML and text reports show backfill data correctly
   - Update any comments that may incorrectly describe backfill behavior
   - Document the cluster configuration in backlog/docs/

6. **Performance validation**:
   - Run analysis on recent data to verify results make sense
   - Compare backfill numbers with known cluster configuration


## Implementation Notes

Applied consistent duplicate GPU cleanup logic to both Priority and Shared machines. Fixed inconsistency where Priority machines had duplicate handling but Shared machines did not. Updated filter_df() function in gpu_utils.py to ensure both machine types handle backfill slots consistently. Validated fix with comprehensive tests confirming backfill slots work correctly on both open capacity and prioritized machines.

Applied consistent duplicate GPU cleanup logic to both Priority and Shared machines. Fixed inconsistency where Priority machines had duplicate handling but Shared machines did not. Updated filter_df() function in gpu_utils.py to ensure both machine types handle backfill slots consistently. Validated fix with comprehensive tests confirming backfill slots work correctly on both open capacity and prioritized machines.
## Acceptance Criteria

- [x] All filtering functions correctly handle backfill slots from both open capacity and prioritized machines
- [x] Reports and visualizations accurately display backfill availability across all machine types
- [x] No code comments or documentation incorrectly states backfill is prioritized-only
- [x] Validation tests confirm backfill logic works consistently for both machine types
- [x] Cluster configuration behavior is properly documented
- [x] GPU counting logic correctly accounts for backfill slots on all machine types
