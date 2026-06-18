# Backfill Slot Classification Fix

## Problem
Backfill slots were being misclassified based on the `PrioritizedProjects` field in the backfill slot itself, rather than based on the machine's primary ownership. This caused:

- ~103 researcher-owned backfill GPUs incorrectly appearing as **Backfill-OpenCapacity** 
- Numbers not aligning with machine ownership policy

### Example
A researcher-owned machine with backfill slots would have its backfill slots classified as "Backfill-OpenCapacity" if the backfill slot entry happened to have an empty `PrioritizedProjects` field, even though the machine itself was researcher-owned (identified by non-empty `PrioritizedProjects` in its primary slots).

## Solution
Refactored `filter_df_enhanced()` in both **pandas** (`gpu_utils.py`) and **polars** (`gpu_utils_polars.py`) versions to:

1. **Identify researcher-owned machines** by scanning primary (non-backfill) slots for any non-empty `PrioritizedProjects` on researcher-owned machines
2. **Classify backfill slots** based on which machine they belong to:
   - `Backfill-ResearcherOwned`: Backfill slots on machines with primary slots that have `PrioritizedProjects` (non-CHTC)
   - `Backfill-CHTCOwned`: Backfill slots on CHTC-owned machines
   - `Backfill-OpenCapacity`: Backfill slots on machines with no researchers (no `PrioritizedProjects`, not CHTC)

## Changes Made

### `gpu_utils.py` (lines 504–532)
- Combined backfill classification into single block
- First identifies researcher-owned machines from primary slots
- Then filters backfill slots and classifies by machine membership

### `gpu_utils_polars.py` (lines 653–682)  
- Applied identical logic using Polars expressions
- Maintains performance with vectorized operations

## Impact
Backfill slots will now correctly report their ownership based on machine characteristics rather than slot-level metadata, aligning numbers with the actual machine ownership policy.
