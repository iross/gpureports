---
id: TASK-37
title: Fold PreventJobsReason into slot dedup ranking
status: Done
assignee:
  - '@claude'
created_date: '2026-07-07 14:31'
updated_date: '2026-07-07 15:57'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The Prevented column currently uses a side-channel check against the raw dataframe (claimed-anywhere pairs) to decide whether a prevented GPU is idle. Folding PreventJobsReason into the duplicate-cleanup slot ranking lets the table accounting resolve multi-slot GPUs directly: a GPU counts as Prevented only if its representative slot is idle with PreventJobsReason set.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A GPU is counted in the Prevented column only when it is idle and has PreventJobsReason set
- [x] #2 A prevented GPU still running a job (Claimed on any slot) is counted as Allocated rather than Prevented
- [x] #3 Slot dedup ranking treats idle prevented primary slots as lower rank than Claimed backfill slots
- [x] #4 Existing report equivalence tests pass
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Extract the 5 identical dedup rank blocks in gpu_utils.py into _apply_duplicate_cleanup with a new prevented tier (idle+PreventJobsReason primary slots rank below Claimed backfill slots)
2. Simplify calculate_prevent_jobs_stats: drop the claimed-pairs side channel; count Prevented as deduped class rows with State != Claimed and PreventJobsReason set
3. Mirror the rank change in gpu_utils_polars._apply_duplicate_cleanup
4. Update tests and the report footnote
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Extracted the six identical dedup rank blocks in gpu_utils.py (filter_df Shared/Priority, filter_df_enhanced Priority-ResearcherOwned/Priority-CHTCOwned/Shared/Priority) into a single _apply_duplicate_cleanup helper, matching the existing pattern in gpu_utils_polars.py, and added a prevented tier to the ranking: a primary slot that is idle with PreventJobsReason set (rank 2) loses to a Claimed backfill slot (rank 3) but still beats idle backfill slots. Mirrored the same ranking in gpu_utils_polars._apply_duplicate_cleanup, with a guard for dataframes lacking the PreventJobsReason column.

calculate_prevent_jobs_stats now counts Prevented directly from the deduped class data (PreventJobsReason set AND State != Claimed) instead of the previous claimed-anywhere-pairs side channel, so a GPU finishing a backfill job resolves to its Claimed backfill row and drops out of the primary class (counted Allocated in the backfill class), while a genuinely idle prevented GPU keeps its prevented primary row.

Trade-off: the idle check is now per representative row per timestamp rather than per 15-minute bucket, so a GPU claimed early in a bucket and idle-prevented later counts as prevented for that bucket — finer-grained than before. Corrected the report footnote: idle prevented GPUs remain part of the class totals in the Available (avg.) column.

Modified: gpu_utils.py, gpu_utils_polars.py, stats_calculations.py, stats_reporting.py, tests/test_report_equivalence.py (new test: an idle backfill duplicate row must not displace the prevented primary row). 78 tests pass; the one failure (dashboard sqlite fallback, missing connectorx) is pre-existing on this branch.
<!-- SECTION:NOTES:END -->
