---
id: task-16.5
title: Rename GlideIn to Backfill (Open capacity)
status: Done
assignee:
  - '@myself'
created_date: '2025-08-08'
updated_date: '2025-08-08'
labels: []
dependencies: []
parent_task_id: task-16
---

## Description

Rename the 'GlideIn' classification to 'Backfill (Open Capacity)' to provide more descriptive and consistent naming that clearly indicates these are backfill slots on open capacity machines

## Description

## Acceptance Criteria

- [ ] All code references to 'GlideIn' updated to 'Backfill-OpenCapacity'
- [ ] Display names updated to show 'Backfill (Open Capacity)'
- [ ] Documentation updated to reflect new naming
- [ ] Tests updated to use new classification name
- [ ] Consistent naming pattern maintained with other backfill categories

## Implementation Notes

Successfully renamed 'GlideIn' classification to 'Backfill (Open Capacity)' for more descriptive and consistent naming:

**Changes Made:**
- Updated all code references from 'GlideIn' to 'Backfill-OpenCapacity' internal naming
- Updated display name mapping to show 'Backfill (Open Capacity)' in output
- Updated methodology.md documentation to reflect new naming
- Updated function docstrings and comments

**Files Modified:**
- usage_stats.py: Updated utilization type lists and filter calls
- gpu_utils.py: Updated filter function docstring, condition checks, and display mapping
- methodology.md: Updated allocation categories and classification rules documentation

**Technical Details:**
- Internal code uses 'Backfill-OpenCapacity' to maintain consistent naming pattern
- Display output shows 'Backfill (Open Capacity)' for better readability
- Functionality preserved - all filtering and classification logic works correctly
- Consistent with other backfill categories naming pattern

**Verification:**
- Tested both regular and --group-by-device output modes
- Confirmed 'Backfill (Open Capacity)' appears correctly in all output sections
- All statistics and calculations working as expected
