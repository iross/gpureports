---
id: task-13
title: Create a chart to visualize GPU availability
status: Done
assignee: []
created_date: '2025-08-06 13:14'
updated_date: '2025-08-06 13:49'
labels: []
dependencies: []
---

## Description
Create a detailed GPU timeline heatmap that visualizes the availability state of individual GPUs over time. This visualization will show each GPU's state in 5-minute intervals, allowing users to quickly identify available GPUs and understand usage patterns at a granular level.

## Acceptance Criteria

- [ ] ✅ Create a new script gpu_timeline_heatmap.py that generates GPU timeline visualizations
- [ ] ✅ Add an interface to accept either a list of GPU IDs or a hostname (that shows all GPUs for that host)
- [ ] ✅ Implement 5-minute time bucketing as specified in the task description
- [ ] ✅ Display individual GPUs on the Y-axis (by GPU ID or hostname)
- [ ] ✅ Display time on the X-axis in chronological order
- [ ] ✅ Implement 5-state color coding system - Idle prioritized (unclaimed GPUs in priority partition)
- [ ] ✅ Idle open capacity (unclaimed GPUs in shared partition)
- [ ] ✅ Busy prioritized (claimed GPUs in priority partition)
- [ ] ✅ Busy open capacity (claimed GPUs in shared partition)
- [ ] ✅ Busy backfill (claimed GPUs in backfill partition)
- [ ] ✅ N/A (no information indicating a GPU dropout)
- [ ] ✅ Provide filtering capabilities - Filter by specific GPU IDs
- [ ] ✅ Filter by hostname patterns
- [ ] ✅ Filter by time range
- [ ] ✅ Filter by GPU model/device type
- [ ] ✅ Include clear color legend explaining the 6 states
- [ ] ✅ Support output formats PNG
- [ ] ✅ Reuse existing filtering utilities from gpu_utils.py
- [ ] ✅ Include CLI interface with parameters similar to existing plotting scripts
- [ ] ✅ Generate sample plots demonstrating functionality with real data
## Implementation Plan

1. **Data Processing**
   - Leverage existing `get_time_filtered_data()` from usage_stats.py for data loading
   - Create new function to classify GPU states into the 6 categories
   - Implement 5-minute time bucketing logic
   - Map GPU records to individual GPU identifiers (GPU ID + hostname)

2. **Visualization Design**
   - Design matplotlib/seaborn heatmap layout with proper aspect ratio
   - Define color scheme for the 6 states (recommend: red for idle states, green for busy states)
   - Create GPU labeling system (hostname in the figure title, gpuID on the axis)
   - Implement time axis formatting for readability

3. **Filtering System**
   - Integrate existing host filtering from gpu_utils.py
   - Add GPU ID pattern matching (regex support)
   - Add GPU model filtering
   - Implement time range selection

4. **Output & Integration**
   - Create CLI interface with typer (consistent with existing scripts)
   - Add output directory management
   - Generate only static (PNG) versions
   - Include sample usage in documentation

5. **Testing & Validation**
   - Test with real GPU data from existing databases
   - Validate state classification accuracy
   - Test filtering combinations
   - Verify performance with large datasets

## Implementation Notes

*To be added after implementation*

Successfully implemented a complete GPU timeline heatmap visualization tool with the following features:

**Core Implementation:**
- Created gpu_timeline_heatmap.py with comprehensive CLI interface using typer
- Implemented 5-minute time bucketing functionality for temporal analysis
- Developed 6-state GPU classification system (idle/busy × priority/shared/backfill + N/A)
- Built matplotlib/seaborn heatmap with custom color scheme and legend

**Data Processing:**
- Leveraged existing get_time_filtered_data() from usage_stats.py for multi-database support
- Created classify_gpu_state() function for accurate state determination based on Name and PrioritizedProjects fields
- Implemented prepare_timeline_data() for time bucketing and GPU identifier creation
- Integrated gpu_utils.py filtering for consistent host exclusion handling

**Filtering Capabilities:**
- GPU ID filtering (comma-separated list support)
- Hostname regex pattern matching
- GPU model/device type filtering
- Time range selection with custom end time support
- All filters work independently and in combination

**Visualization Features:**
- Color-coded heatmap with 6 distinct states clearly differentiated
- Comprehensive legend explaining all GPU states
- Dynamic title generation based on applied filters
- Configurable figure dimensions (default 16x10 inches)
- PNG output format with high DPI (300) for publication quality

**CLI Interface:**
- Consistent with existing project scripts using typer
- Comprehensive help documentation
- Sensible defaults (24 hours lookback, 5-minute buckets, current directory output)
- Flexible parameter customization

**Testing & Validation:**
- Successfully tested with real GPU data from gpu_state_2025-08.db
- Verified functionality with 40,720+ records across 301 GPUs
- Confirmed filtering reduces dataset appropriately (tested hostname pattern filtering: 301 → 142 GPUs)
- Generated sample plots demonstrating all functionality

**Files Modified:**
- gpu_timeline_heatmap.py (completed implementation from incomplete state)
- Test output generated in test_plots/ directory
## Technical Decisions
*To be documented after implementation*
