---
id: task-21
title: >-
  Add an additional table for Real slots that shows device memory breakdowns
  instead of device names
status: Done
assignee:
  - '@iross'
created_date: '2025-08-18 15:35'
updated_date: '2025-08-18 15:56'
labels: []
dependencies: []
---

## Description

Add an additional table for Real slots (Priority + Shared GPU classes) that groups GPUs by memory capacity instead of device names. This will provide insights into memory resource allocation patterns and help users understand GPU usage from a memory perspective rather than device model perspective.

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] Table displays memory capacity categories instead of device names
- [x] Table shows same metrics as existing Real slots table (Allocated %, Allocated avg, Available avg)
- [x] Memory categories are logical and comprehensive
- [x] Table integrates seamlessly with existing report output
- [x] Memory breakdown totals match device-based totals when aggregated
- [x] Table works with both HTML and text output formats
<!-- AC:END -->

## Implementation Notes

Successfully implemented memory breakdown table for Real slots with the following features:

**Memory Categorization System:**
- Added DEVICE_MEMORY_MAPPINGS with GPU memory capacities (11GB-141GB range)
- Created MEMORY_CATEGORIES for logical grouping: 10-12GB, 16GB, 24GB, 40GB, 48GB, 80GB, 140GB+
- Implemented get_device_memory_gb() and get_memory_category() helper functions

**Core Implementation:**
- Created calculate_allocation_usage_by_memory() function that groups Real slots by memory capacity
- Function calculates same metrics as device-based tables: avg_claimed, avg_total_available, allocation_usage_percent
- Only processes Priority + Shared classes (Real slots), excluding Backfill

**Integration:**
- Added 'Real Slots by Memory Category' tables to both HTML report formats
- Added text output sections showing memory breakdown
- Tables appear right after existing Real Slots tables
- Consistent styling and formatting with existing tables

**Features:**
- Memory categories sorted logically (10GB to 140GB+)
- TOTAL row shows aggregated memory category statistics  
- Handles unknown devices with fallback categorization
- Works with both single-host and multi-host reports

**Files Modified:**
- device_name_mappings.py: Added memory mappings and helper functions
- usage_stats.py: Added calculation function and integrated tables into all report formats

All acceptance criteria met - memory breakdown totals match device-based totals when aggregated, seamless integration with existing output formats.
