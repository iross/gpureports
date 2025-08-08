---
id: task-16.2
title: Update reporting and create machine category tables
status: Done
assignee: []
created_date: '2025-08-08'
updated_date: '2025-08-08'
labels: []
dependencies:
  - task-16.1
parent_task_id: task-16
---

## Description

Update cluster summaries with new classification categories, reclassify backfill as GlideIn, and create additional output table listing machines by category

## Acceptance Criteria

- [x] Separate backfill (researcher owned) and backfill (hosted capacity), with more emphasis placed on hosted capacity
- [x] Backfill on open capacity reclassified as GlideIn
- [x] Cluster summaries include new categories
- [x] Additional table lists machines by category
- [x] Existing report generation works with new classifications
- [x] Output tables are correctly formatted

## Implementation Notes

Implemented enhanced reporting with new backfill classification:

**gpu_utils.py enhancements:**
- filter_df_enhanced(): New filtering function supporting Backfill-ResearcherOwned, Backfill-HostedCapacity, and GlideIn categories
- count_backfill_researcher_owned(), count_backfill_hosted_capacity(), count_glidein(): New counting functions
- Enhanced display names for new categories

**usage_stats.py enhancements:**
- calculate_allocation_usage_enhanced(): New calculation function using enhanced categories
- Updated run_analysis() with use_enhanced_classification parameter
- Enhanced HTML report generation with new allocation summary table
- Machine categories table showing machines in each category (Hosted Capacity, Researcher Owned, Open Capacity)
- New CLI option --enhanced-classification to enable enhanced mode

**Key features:**
- Separates backfill into researcher owned vs hosted capacity with emphasis on hosted capacity
- Reclassifies backfill on open capacity machines as GlideIn
- Maintains backward compatibility with existing reports
- Order in reports emphasizes hosted capacity as specified
