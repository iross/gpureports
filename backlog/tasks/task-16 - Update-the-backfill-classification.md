---
id: task-16
title: Update the prioritized host classifications
status: Done
assignee: []
created_date: '2025-08-07'
labels: []
dependencies: []
---

## Description
- Currently, the "backfill" designation is applied to slots on three kinds of machines:
    1. Prioritized
    2. Open capacity

- This ticket is meant to clarify the reporting around the "backfill" model, creating two categories of backfill that run on "Prioritized" machines:
    1. Researcher-owned
    2. Hosted capacity

- Classification logic will be:
    - **Hosted Capacity**: Machines provided via an external list of "Hosted Capacity" machines
    - **Researcher Owned**: Any machines with non-empty PrioritizedProjects field

- Additionally, "backfill" on _open capacity_ should be re-classified as "GlideIn". 

- New tables and updated rows in the cluster summaries will need to be created.
- An additional output table will list machines in each category (Hosted Capacity, Researcher Owned, Open Capacity).
- I'll want to provide additional information in the "methods" defining these categories and the logic used to filter.

## Acceptance Criteria

- [ ] All subtasks (16.1, 16.2, 16.3) are completed
- [ ] Machine classification system correctly categorizes all machines
- [ ] Reporting outputs include new categories and machine listing table
- [ ] Documentation reflects new classification approach
- [ ] All existing functionality continues to work
- [ ] Integration tests pass with new classification system

## Implementation Plan (the how)

This task has been broken down into three subtasks:

1. **task-16.1**: Implement machine classification logic
   - Core classification algorithms for Hosted Capacity vs Researcher Owned
   - External list integration for hosted capacity machines
   - PrioritizedProjects field checking for researcher owned machines

2. **task-16.2**: Update reporting and create machine category tables
   - Reclassify backfill on open capacity as GlideIn
   - Update cluster summaries with new categories
   - Create additional output table listing machines by category

3. **task-16.3**: Update documentation for new machine classifications
   - Update methodology.md with new category definitions
   - Document classification logic and filtering rules

## Subtasks

- task-16.1 - Implement machine classification logic
- task-16.2 - Update reporting and create machine category tables  
- task-16.3 - Update documentation for new machine classifications
- task-16.4
- task-16.5
