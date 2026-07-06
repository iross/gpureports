# task-36 - Rename and fix PreventJobsReason column in Real Slots table

## Description

The "Blocked (avg.)" column added to the Real Slots table has two problems that need
to be addressed together:

1. **Column name**: "Blocked" is not descriptive. A better label should be chosen
   (e.g. "Prevented (avg.)" or similar) that makes clear the column counts GPUs with
   `PreventJobsReason` set.

2. **Under-counting**: At 2026-07-03 15:57, a live `condor_status` run showed 29 slots
   (20 Owner + 9 Claimed) with `PreventJobsReason != ""` and `PrioritizedProjects == ""`
   (i.e. the Shared/Open Capacity class). The report at that time showed only ~3.5 avg.
   GPUs for Open Capacity — far fewer than expected. The root cause is unknown and needs
   investigation.

   Machines visible in condor_status at that time (all Shared class, PreventJobsReason set):
   - btellman-jsullivangpu4000 (8× L40S, Owner)
   - dbrundage-chtcgpu5000 (12× H200, Owner)
   - dbrundagegpu5000 (8× L40S — 1 Owner + 7 Claimed/backfill)
   - dgx-spark2 (2× GB10, Owner)
   - gpu4006 (16× L40S, Owner)
   - gpu5000 (16× H200, Owner)
   - gpu5001 (16× H200, Owner)
   - ukamilovgpu5000 (8× RTX PRO 6000, Owner)
   - ukamilovgpu5001 (8× RTX PRO 6000, Owner)
   - vetsigian0000 (12× RTX 2080 — Owner + Claimed)
   - xhuanggpu4000 (8× A40, Owner)
   - zliu-chtcgpu5000 (14× L40S, Owner)

## Acceptance Criteria

- [x] Column is renamed to something more descriptive than "Blocked (avg.)"
- [x] The per-class averages in the column match what `condor_status -gpus -constraint 'PreventJobsReason!=""'` would suggest for the reporting window
- [x] Owner-state slots with PreventJobsReason are correctly captured by the collector and included in counts (verified `filter_df_enhanced` with `state=""` includes Owner state rows)
- [x] Any systematic under-counting is identified: root cause was 24h averaging diluting newly-added PreventJobsReason data (collector field was recently added; only 3 of 85 15-min buckets had PJ data)
- [x] If the column is counting only a subset, that is either fixed or clearly documented — fixed by switching to point-in-time counts

## Implementation Plan

1. Check whether Owner-state rows are being written to Parquet at all — run a spot check against a recent Parquet file for a known Owner machine (e.g. `gpu5000`)
2. Verify `filter_df_enhanced("Shared", state="")` does in fact include Owner state rows (read the filter logic in `gpu_utils.py`)
3. Compare unique GPU counts from Parquet vs condor_status for the machines listed above
4. Check whether the `Cb` (Claimed/backfill) slots on dbrundagegpu5000 and vetsigian0000 land in "Shared" or "Backfill" class — they have `PrioritizedProjects==""` but are running backfill jobs
5. Decide on column name and rename in `stats_reporting.py`
6. Fix any counting bugs found and update tests

## Implementation Notes

**Root cause of under-counting**: pure dilution. `PreventJobsReason` was only recently added to the collector projection, so the July 2026 Parquet file had PJ data in only 3 of the 85 fifteen-minute buckets in the 24h window. Dividing by 85 turned 42 real GPUs into 1.5 in the report.

**Fix**: switched the Real Slots table column from the 24h per-class average (`per_class_avg`) to a point-in-time count (`per_class_current`) taken from the most recent 15-min bucket that has any PreventJobsReason data. This matches what `condor_status` shows.

- Both `per_class_current` (integer count) and `per_class_device_current` (per device type, for Flagship/Standard tier split) are now computed in `calculate_prevent_jobs_stats`
- When PJ data covers < 25% of the window, a `*` appears on the column header and a footnote explains why
- Column renamed from "Blocked (avg.)" to "Prevented" (no "(avg.)" since it is a snapshot)

**Verified class classification**: `filter_df_enhanced` with `state=""` does include Owner-state rows — the state filter only applies when `state != ""`. Backfill slots (name contains "backfill") are excluded from Shared class and go to Backfill-ResearcherOwned / Backfill-CHTCOwned / Backfill-OpenCapacity as expected.

**Point-in-time counts at the time of investigation (2026-07-03)**:
- Shared: 42 GPUs
- Priority-ResearcherOwned: 38 GPUs
- Priority-CHTCOwned: 8 GPUs
- Backfill-ResearcherOwned: 48 GPUs

**Modified files**: `stats_calculations.py`, `stats_reporting.py`
