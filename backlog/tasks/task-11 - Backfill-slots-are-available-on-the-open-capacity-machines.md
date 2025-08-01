---
id: task-11
title: Backfill slots are available on the open capacity machines
status: To Do
assignee: []
created_date: '2025-08-01'
updated_date: '2025-08-01'
labels: []
dependencies: []
---

## Description

The current cluster configuration allows backfill slots to be available on both prioritized machines AND open capacity (shared) machines when their GPUs are idle. This task ensures that our monitoring system correctly handles and displays this behavior consistently throughout the codebase, reporting, and analysis tools.

## Implementation Plan

1. Review current codebase to identify any logic that incorrectly assumes backfill slots only exist on prioritized machines
2. Audit filtering and counting functions to ensure they properly handle backfill slots from both open capacity and prioritized machines
3. Verify that reporting and visualization tools correctly display backfill availability across all machine types
4. Update any documentation or comments that incorrectly describe backfill as prioritized-only
5. Create validation tests to ensure backfill logic works consistently for both machine types
6. Document the correct cluster configuration behavior for future reference

## Acceptance Criteria

- [ ] All filtering functions correctly handle backfill slots from both open capacity and prioritized machines
- [ ] Reports and visualizations accurately display backfill availability across all machine types
- [ ] No code comments or documentation incorrectly states backfill is prioritized-only
- [ ] Validation tests confirm backfill logic works consistently for both machine types
- [ ] Cluster configuration behavior is properly documented
- [ ] GPU counting logic correctly accounts for backfill slots on all machine types
