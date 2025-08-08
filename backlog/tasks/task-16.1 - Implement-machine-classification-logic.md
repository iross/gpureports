---
id: task-16.1
title: Implement machine classification logic
status: Done
assignee: []
created_date: '2025-08-08'
updated_date: '2025-08-08'
labels: []
dependencies: []
parent_task_id: task-16
---

## Description

Implement the core logic to classify machines as Hosted Capacity, Researcher Owned, or Open Capacity based on external list and PrioritizedProjects field

## Acceptance Criteria

- [x] External list loading mechanism implemented
- [x] PrioritizedProjects field checking implemented
- [x] Classification function returns correct categories
- [x] Unit tests verify classification accuracy

## Implementation Notes

Implemented core machine classification logic in gpu_utils.py:

- load_hosted_capacity_hosts(): Loads and caches hosted capacity machines from external file
- classify_machine_category(): Classifies individual machines into Hosted Capacity, Researcher Owned, or Open Capacity based on hosted_capacity file and PrioritizedProjects field
- filter_df_by_machine_category(): Filters DataFrame by machine category  
- get_machines_by_category(): Returns organized lists of machines by category

Added comprehensive unit tests in tests/test_gpu_utils.py covering all classification scenarios. Functions use efficient caching for hosted capacity list and follow existing code patterns.
