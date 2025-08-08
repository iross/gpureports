---
id: task-16.3
title: Update documentation for new machine classifications
status: Done
assignee: []
created_date: '2025-08-08'
updated_date: '2025-08-08'
labels: []
dependencies: []
parent_task_id: task-16
---

## Description

Update methodology.md and related documentation to define the new classification categories and document the logic used for filtering and classification

## Acceptance Criteria

- [x] Methodology.md updated with new category definitions
- [x] Classification logic is clearly documented
- [x] Examples of each category are provided
- [x] Documentation explains external list integration
- [x] All classification rules are documented
- [x] List of researcher-owned machines documented in methodology
- [x] List of hosted capacity machines documented in methodology

## Implementation Notes

Updated methodology.md with comprehensive documentation for enhanced machine classifications:

**New Sections Added:**
- Enhanced Classification Categories: Added detailed definitions for new backfill categories (Hosted Capacity, Researcher Owned, GlideIn)
- Machine Classification Logic: Documented the external list integration and PrioritizedProjects field logic
- Classification Rules: Clear rules for categorizing machines and backfill slots
- Machine Classifications: Listed all hosted capacity machines with date stamp
- External List Integration: Explained how the hosted_capacity file is used

**Key Documentation:**
- All 7 hosted capacity machines explicitly listed
- Researcher owned machines defined as dynamic category based on PrioritizedProjects field
- Clear distinction between standard and enhanced classification modes
- Comprehensive examples and integration explanation
- GlideIn category properly documented as reclassified backfill on open capacity

The methodology now provides complete documentation for understanding the new classification system and serves as reference for users and maintainers.
