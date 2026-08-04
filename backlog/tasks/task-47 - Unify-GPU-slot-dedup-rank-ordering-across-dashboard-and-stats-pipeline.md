---
id: TASK-47
title: Unify GPU slot dedup rank ordering across dashboard and stats pipeline
status: Done
assignee:
  - iaross@wisc.edu
created_date: '2026-07-31 20:48'
updated_date: '2026-08-04 19:46'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
There are three different, disagreeing implementations of 'which slot wins when a GPU has multiple concurrent rows in the same time bucket': stats_calculations.py's prepare_frames() (the canonical, parity-tested ranking, updated in task-37 to include PreventJobsReason), dashboard/data.py's _dedup_and_bucket() (a simpler 4-tier ranking that disagrees with the canonical one on whether claimed+backfill or unclaimed+primary should win), and a third, separate inline ranking inside get_opencap_users_data(). This can make the dashboard heatmap disagree with the email reports for the same GPU/timestamp. Extract the canonical rank expression into a standalone, reusable function and use it everywhere instead of hand-copying similar-but-different logic.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The rank pl.Expr used in stats_calculations.py's prepare_frames() is extracted into a standalone function with no change in prepare_frames()'s output (parity tests still pass)
- [x] #2 dashboard/data.py's _dedup_and_bucket() and get_opencap_users_data()'s inline dedup both use the extracted, canonical rank function instead of their own separate rank logic
- [x] #3 dashboard/data.py selects PreventJobsReason so the canonical rank's prev_idle tier has the column it needs
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Extract the rank pl.Expr from stats_calculations.py's prepare_frames() into a standalone function\n2. Confirm prepare_frames() output is unchanged (parity tests still pass)\n3. Update dashboard/data.py's _dedup_and_bucket() to use the extracted rank function\n4. Update dashboard/data.py's get_opencap_users_data() inline dedup to use it too\n5. Add PreventJobsReason to dashboard's COLUMNS/opencap_cols selection so the prev_idle rank tier has the column it needs\n6. Add/extend tests covering a case where the old dashboard ranking and canonical ranking disagreed
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Extracted the rank pl.Expr from stats_calculations.py's prepare_frames() into a standalone slot_dedup_rank(is_bf, state, prev_idle) function; prepare_frames() now calls it with the same pre-computed _is_bf/_prev_idle columns instead of inlining the when-chain. No output change: all 20 existing parity/equivalence tests (test_pandas_polars_parity.py, test_report_equivalence.py) still pass unmodified.

Added PreventJobsReason to dashboard/data.py's COLUMNS and get_opencap_users_data's opencap_cols so the canonical rank's prev_idle tier has the column it needs (task 45's schema hardening means this is always present, null-filled, even for older files missing the column).

Replaced both of dashboard/data.py's own hand-rolled rank implementations (_dedup_and_bucket's 4-tier rank, and get_opencap_users_data's separate 4-tier rank) with calls to the same slot_dedup_rank, via a small _rank_inputs() helper that derives is_bf/state/prev_idle from Name/State/PreventJobsReason the same way prepare_frames() does.

Added tests/test_dashboard_data.py::test_dedup_and_bucket_matches_canonical_rank_on_backfill_vs_primary, reproducing the exact disagreement the task described (unclaimed-primary vs claimed-backfill): confirmed by reverting only dashboard/data.py that the old dashboard rank picked the backfill row (backwards from canonical), then passes once the shared rank function is wired in.
<!-- SECTION:NOTES:END -->
