---
id: task-19
title: Format updates 2025-08-14
status: To Do
assignee: []
created_date: '2025-08-14'
updated_date: '2025-08-14'
labels: []
dependencies: []
---

## Description
The bosses have a few additional formatting requests:
- Rename "Hosted Capacity" to "CHTC Owned"
- Move the "Cluster Summary" to the top
- Create a map of the DeviceName to something a bit more Human-readable (ask me to approve these mappings)
- Simplify the "Period" at the top of the email

## Implementation Plan

1. Rename all instances of 'Hosted Capacity' to 'CHTC Owned' in code and templates
2. Move Cluster Summary section to appear first after metadata in HTML template
3. Extract current DeviceNames from database and create human-readable mapping dictionary
4. Simplify Period display format in HTML template and usage_stats.py
5. Update all relevant files and test changes

## Acceptance Criteria

- [ ] All instances of 'Hosted Capacity' are renamed to 'CHTC Owned' in codebase
- [ ] Cluster Summary section appears immediately after Report Information metadata
- [ ] DeviceName mapping dictionary is created with human-readable names for approval
- [ ] Period format is simplified to be more concise and readable
- [ ] All formatting changes are applied consistently across templates and code
- [ ] Changes preserve existing functionality and styling
