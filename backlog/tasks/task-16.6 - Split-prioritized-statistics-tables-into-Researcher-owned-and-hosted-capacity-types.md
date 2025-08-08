---
id: task-16.6
title: >-
  Split prioritized statistics tables into Researcher owned and hosted capacity
  types
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

Split the 'Prioritized service' statistics into separate categories for researcher-owned machines and hosted capacity machines to provide more granular visibility into prioritized resource usage patterns

## Description

## Acceptance Criteria

- [ ] Prioritized service split into two separate categories
- [ ] Prioritized (Researcher Owned) shows stats for prioritized slots on researcher machines
- [ ] Prioritized (Hosted Capacity) shows stats for prioritized slots on hosted capacity machines
- [ ] Output tables display both categories with appropriate naming
- [ ] Documentation updated to reflect new prioritized subcategories
- [ ] Consistent with backfill classification pattern

## Implementation Notes

Successfully split 'Prioritized service' statistics into separate categories for researcher-owned and hosted capacity machines:

**Changes Made:**
- Added Priority-ResearcherOwned and Priority-HostedCapacity utilization types
- Updated filtering logic in filter_df_enhanced() to distinguish priority slots by machine type
- Priority-ResearcherOwned filters for non-empty PrioritizedProjects AND not in hosted capacity list
- Priority-HostedCapacity filters for non-empty PrioritizedProjects AND in hosted capacity list
- Updated display name mappings to show 'Prioritized (Researcher Owned)' and 'Prioritized (Hosted Capacity)'
- Updated all class_order arrays to include new priority subcategories
- Updated methodology.md documentation with new prioritized subcategories

**Files Modified:**
- usage_stats.py: Updated utilization_types arrays and class_order arrays
- gpu_utils.py: Added filtering logic and display names for new priority categories
- methodology.md: Updated allocation categories documentation

**Technical Details:**
- Maintains same deduplication logic as original Priority filtering
- Consistent with backfill classification pattern (HostedCapacity vs ResearcherOwned)
- Works in both device-grouped and allocation summary output modes
- Provides granular visibility into prioritized resource usage patterns

**Verification Results:**
- Device-grouped mode: Shows separate sections for each prioritized category with device breakdowns
- Allocation summary mode: Shows separate statistics for researcher owned vs hosted capacity priority slots
- All statistics calculated correctly using machine type filtering logic
