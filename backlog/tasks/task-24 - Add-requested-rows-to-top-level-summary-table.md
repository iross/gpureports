---
id: task-24
title: Add requested rows to top-level summary table
status: Done
assignee: []
created_date: '2025-11-11 20:34'
updated_date: '2025-11-17 17:24'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The Real Slots summary table currently shows a single 'Open Capacity' row and excludes backfill information. This task will:

1. Break down Open Capacity into meaningful subcategories for better visibility
2. Add backfill slot totals/breakdown to the top-level summary table

Current structure:
- **Real Slots** table: TOTAL, Prioritized (Researcher Owned), Prioritized (CHTC Owned), Open Capacity
- **Backfill Slots** table (separate): TOTAL, Backfill (Researcher Owned), Backfill (CHTC Owned), Backfill (Open Capacity)

Goal:
Create a comprehensive top-level summary table that includes both real slots AND backfill information in one unified view.

Proposed categorization options are documented in Implementation Notes below.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Top-level summary table includes Open Capacity subcategory breakdown
- [x] #2 Top-level summary table includes backfill slot information
- [x] #3 Open Capacity subcategories sum to match original Open Capacity total
- [x] #4 Backfill totals match the original separate backfill table
- [x] #5 HTML table styling is consistent throughout the unified table
- [x] #6 New category names are clear and descriptive
- [x] #7 Code changes maintain backward compatibility

- [x] #8 Monthly report generation completes without errors
- [x] #9 All existing tests pass
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Research and understand current data structure
   - Review how Open Capacity (Shared) slots are classified in gpu_utils.py
   - Examine existing device_stats and memory_stats breakdowns
   - Understand current separation between Real Slots and Backfill Slots tables
   - Review backfill classification (Backfill-ResearcherOwned, Backfill-CHTCOwned, Backfill-OpenCapacity)

2. Design unified table structure
   - Decide on categorization for Open Capacity split (performance tier recommended)
   - Determine how to integrate backfill information into the top-level table
   - Options: (a) Single unified table with both real and backfill, (b) Add backfill subtotals to real slots table
   - Ensure visual clarity to distinguish real slots from backfill slots

3. Implement data collection for new table structure
   - Modify calculate_allocation_usage_enhanced() or create new function
   - Add filtering logic to separate Open Capacity into subcategories
   - Ensure backfill data is properly aggregated for top-level view
   - Maintain backward compatibility with existing device breakdown tables

4. Update HTML generation in usage_stats.py
   - Modify generate_html_report() around line 1940-2045
   - Replace single "Open Capacity" row with subcategory rows
   - Integrate backfill information into top-level summary table
   - Add visual separators/styling to distinguish real vs backfill slots
   - Consider adding subtotal rows for clarity

5. Update terminology and display names
   - Update get_display_name() function with new category names
   - Add new categories to CLASS_ORDER if needed
   - Document new naming conventions for both Open Capacity and unified table

6. Test and validate
   - Run usage_stats.py with sample data
   - Verify HTML output matches expected format
   - Ensure totals still calculate correctly (real + backfill)
   - Verify Open Capacity subcategories sum correctly
   - Verify backfill totals match original separate table
   - Check that new structure appears correctly in monthly reports
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
## Proposed Open Capacity Categorization Options

After analyzing the codebase and existing GPU classification system, here are several options for breaking down the Open Capacity category:

### RECOMMENDED: Option 1 - By Performance Tier

Split Open Capacity into three performance-based tiers:

**Categories:**
- **Open Capacity - Flagship** (H200, H100 80GB)
  - Latest generation, highest memory, best performance
  - Premium GPUs with limited availability
  
- **Open Capacity - High Performance** (A100 80GB, A100 40GB)
  - Proven workhorses for ML/AI workloads
  - Good memory and compute balance
  
- **Open Capacity - Standard** (L40S, L40, RTX 2080 Ti)
  - General purpose GPUs
  - Good for development and smaller workloads

**Pros:**
- Aligns with how users think about GPU selection
- Clear progression from standard to flagship
- Easy to understand and communicate to stakeholders
- Groups GPUs by practical use cases

**Cons:**
- Requires maintaining performance tier classifications
- May need updates as new GPU models are added

**Implementation approach:**
- Add device_performance_tier mapping in gpu_utils.py
- Filter Open Capacity (Shared) class by tier
- Calculate separate stats for each tier
- Update HTML generation to show three rows instead of one

### Option 2 - By Memory Class (Align with existing memory table)

Use the same memory categorization as the "Real Slots by Memory Category" table:

**Categories:**
- **Open Capacity - <48GB** (RTX 2080 Ti, A100 40GB)
- **Open Capacity - 48GB** (L40, L40S)
- **Open Capacity - 80GB** (A100 80GB, H100 80GB)
- **Open Capacity - >80GB** (H200)

**Pros:**
- Perfectly consistent with existing memory breakdown table
- Memory is often the primary constraint for jobs
- Minimal new code required (leverage existing memory_category logic)
- Users already familiar with these categories

**Cons:**
- Duplicates information already in memory table
- May be redundant with "Real Slots by Memory Category"
- Doesn't distinguish between device performance within same memory class

### Option 3 - By GPU Architecture/Generation

Split by NVIDIA architecture generation:

**Categories:**
- **Open Capacity - Hopper** (H200, H100)
  - Latest architecture (2023+)
  
- **Open Capacity - Ampere** (A100 variants)
  - 2020-2022 generation
  
- **Open Capacity - Ada/Turing** (L40S, L40, RTX 2080 Ti)
  - Turing (2018-2019) and Ada (2022) architectures

**Pros:**
- Technical accuracy
- Clear generation boundaries
- Reflects actual hardware capabilities

**Cons:**
- Requires users to know architecture names
- May be too technical for non-GPU experts
- Less intuitive than performance-based naming

### Option 4 - Simple Two-Tier System

Minimal split into just two categories:

**Categories:**
- **Open Capacity - Premium** (H200, H100, A100 80GB)
  - High-memory, high-performance GPUs
  
- **Open Capacity - Standard** (A100 40GB, L40S, L40, RTX 2080 Ti, others)
  - General availability GPUs

**Pros:**
- Simplest to implement and understand
- Clear distinction between premium and standard
- Minimal code changes required

**Cons:**
- Less granularity than other options
- Large standard category may still be too broad
- Doesn't reflect full diversity of GPU types

### Option 5 - By Demand/Utilization Pattern

Dynamic categorization based on actual usage:

**Categories:**
- **Open Capacity - High Demand** (utilization >70%)
- **Open Capacity - Medium Demand** (utilization 40-70%)
- **Open Capacity - Low Demand** (utilization <40%)

**Pros:**
- Shows actual usage patterns
- Helps identify underutilized resources
- Dynamic based on real data

**Cons:**
- Categories change over time
- More complex to implement
- May confuse users expecting static categories
- Doesn't help with GPU selection

## FINAL RECOMMENDATION

**Use Option 1 (Performance Tier)** for the following reasons:

1. **User-Centric**: Matches how researchers think about GPU selection
2. **Actionable**: Helps users understand what tier suits their workload
3. **Stable**: Won't change frequently like utilization-based approach
4. **Clear**: Easy to communicate in reports and documentation
5. **Scalable**: Easy to add new GPUs to appropriate tier

**Improved Naming Convention:**
Instead of "Open Capacity - X", use more concise naming:
- **Flagship Open Capacity** or **Open Capacity: Flagship GPUs**
- **Premium Open Capacity** or **Open Capacity: Premium GPUs**
- **Standard Open Capacity** or **Open Capacity: Standard GPUs**

This provides a clear hierarchy while maintaining the "Open Capacity" branding that stakeholders recognize.

**Device Classification:**
```
Flagship: [H200, H100 80GB]
Premium: [A100 80GB, A100 40GB]
Standard: [L40S, L40, RTX 2080 Ti, RTX 1080 Ti, P100, A30, A40]
```

---

## Integrating Backfill into Top-Level Summary Table

### Design Question: How to present real slots + backfill together?

**Option A: Unified Single Table**
Present all slots (real and backfill) in one table with visual separation:

```
┌─────────────────────────────────────────────────────────────┐
│ GPU ALLOCATION SUMMARY                                       │
├─────────────────────────────────────────────────────────────┤
│ TOTAL (All Slots)          │ 50% │ 150.0 │ 300.0           │
├─────────────────────────────────────────────────────────────┤
│ REAL SLOTS SUBTOTAL        │ 42% │ 110.5 │ 260.3           │
│   Prioritized (Researcher) │ 22% │  24.8 │ 111.5           │
│   Prioritized (CHTC)       │ 27% │   9.1 │  33.8           │
│   Open Capacity - Flagship │ 85% │  10.0 │  12.0           │
│   Open Capacity - Premium  │ 70% │  40.0 │  58.0           │
│   Open Capacity - Standard │ 55% │  26.6 │  44.9           │
├─────────────────────────────────────────────────────────────┤
│ BACKFILL SLOTS SUBTOTAL    │ 11% │  17.4 │ 163.4           │
│   Backfill (Researcher)    │  9% │   8.5 │  94.5           │
│   Backfill (CHTC)          │ 20% │   6.5 │  32.2           │
│   Backfill (Open Capacity) │  7% │   2.5 │  36.7           │
└─────────────────────────────────────────────────────────────┘
```

**Pros:**
- Single comprehensive view of all GPU allocation
- Easy to compare real vs backfill utilization
- Shows full picture in one place

**Cons:**
- Longer table (more rows)
- Mixing real and backfill concepts may confuse some users
- Need clear visual separators

**Option B: Enhanced Real Slots Table + Backfill Summary Row**
Keep Real Slots as primary focus, add backfill as summary:

```
┌─────────────────────────────────────────────────────────────┐
│ REAL SLOTS                                                   │
├─────────────────────────────────────────────────────────────┤
│ TOTAL (Real Slots)         │ 42% │ 110.5 │ 260.3           │
│   Prioritized (Researcher) │ 22% │  24.8 │ 111.5           │
│   Prioritized (CHTC)       │ 27% │   9.1 │  33.8           │
│   Open Capacity - Flagship │ 85% │  10.0 │  12.0           │
│   Open Capacity - Premium  │ 70% │  40.0 │  58.0           │
│   Open Capacity - Standard │ 55% │  26.6 │  44.9           │
├─────────────────────────────────────────────────────────────┤
│ BACKFILL SLOTS (All)       │ 11% │  17.4 │ 163.4           │
└─────────────────────────────────────────────────────────────┘
```

**Pros:**
- Cleaner, more compact
- Maintains focus on real slots
- Still provides backfill visibility

**Cons:**
- Less detail on backfill breakdown
- Doesn't show backfill by category

**Option C: Side-by-Side Columns**
Show real and backfill metrics in parallel columns:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Category            │ Real Slots      │ Backfill Slots  │ Combined        │
│                     │ Alloc │ Avg     │ Alloc │ Avg     │ Alloc │ Avg     │
├─────────────────────────────────────────────────────────────────────────────┤
│ TOTAL              │  42%  │ 110.5   │  11%  │  17.4   │  35%  │ 127.9   │
│ Researcher Owned   │  22%  │  24.8   │   9%  │   8.5   │  17%  │  33.3   │
│ CHTC Owned         │  27%  │   9.1   │  20%  │   6.5   │  25%  │  15.6   │
│ Open - Flagship    │  85%  │  10.0   │   -   │   -     │  85%  │  10.0   │
│ Open - Premium     │  70%  │  40.0   │   -   │   -     │  70%  │  40.0   │
│ Open - Standard    │  55%  │  26.6   │   7%  │   2.5   │  52%  │  29.1   │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Pros:**
- Direct comparison between real and backfill
- Compact representation
- Shows relationships clearly

**Cons:**
- More complex table structure
- May be harder to read for some users
- Wider table may not fit well on all displays

## RECOMMENDED APPROACH: Option A (Unified Single Table)

For maximum clarity and completeness, use **Option A** with the following enhancements:

1. **Visual Hierarchy:**
   - Use background shading to separate sections (light gray for subtotals, white for items)
   - Bold text for TOTAL and SUBTOTAL rows
   - Indent subcategories slightly

2. **Clear Labeling:**
   - "TOTAL (All GPU Slots)" at the top
   - "Real Slots Subtotal" as section header
   - "Backfill Slots Subtotal" as section header

3. **Open Capacity Breakdown:**
   - Use the recommended Performance Tier categorization:
     - Open Capacity - Flagship
     - Open Capacity - Premium
     - Open Capacity - Standard

4. **Backfill Breakdown:**
   - Keep existing three categories:
     - Backfill (Researcher Owned)
     - Backfill (CHTC Owned)
     - Backfill (Open Capacity)

This provides the most comprehensive view while maintaining clarity through visual design.

## Final Implementation

### Approach Taken
Implemented a two-tier classification system for Open Capacity GPUs based on user specifications:
- **Flagship tier**: H100, H200, and A100 80GB GPUs
- **Standard tier**: All other GPU types (L40S, L40, RTX 2080 Ti, etc.)

Integrated backfill slot information directly into the Real Slots summary table, eliminating the separate Backfill Slots table for a unified view.

### Features Implemented
1. **GPU Performance Tier Classification** (`get_gpu_performance_tier()` in `gpu_utils.py`)
   - Pattern-based matching for Flagship GPUs (H100, H200, A100-SXM4-80GB, A100 80GB)
   - All non-matching GPUs classified as Standard

2. **Unified Summary Table**
   - Real Slots section with TOTAL row
   - Prioritized (Researcher Owned) and Prioritized (CHTC Owned) rows
   - Open Capacity broken into Flagship and Standard tiers
   - Visual separator between real slots and backfill slots
   - Backfill section with TOTAL and individual breakdowns

### Technical Decisions
- Used simple pattern matching rather than a comprehensive device registry for maintainability
- Kept backfill breakdown (Researcher Owned, CHTC Owned, Open Capacity) consistent with existing structure
- Added visual separator row between real and backfill sections for clarity
- Removed separate Backfill Slots table to avoid redundancy

### Modified Files
- `gpu_utils.py`: Added `get_gpu_performance_tier()` function and new display names
- `usage_stats.py`: Updated import, modified `generate_html_report()` to split Open Capacity by tier and integrate backfill info

### Verification
- Generated test HTML report successfully
- Open Capacity subcategories sum correctly (Flagship + Standard = Total)
- Backfill information properly integrated into unified table
- No separate Backfill Slots section exists
- All imports work correctly
<!-- SECTION:NOTES:END -->
