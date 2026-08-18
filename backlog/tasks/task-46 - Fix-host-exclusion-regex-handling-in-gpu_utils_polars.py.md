---
id: TASK-46
title: Fix host-exclusion regex handling in gpu_utils_polars.py
status: Done
assignee:
  - iaross@wisc.edu
created_date: '2026-07-31 20:48'
updated_date: '2026-08-04 19:41'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Commit b6e23a1 fixed gpu_utils.py's host-exclusion filtering (filter_df / filter_df_enhanced) to combine all excluded hosts into a single re.escape()'d regex alternation scanned once, instead of looping per host. gpu_utils_polars.py -- used by the dashboard -- still has the old per-host loop with no re.escape, at two call sites. This is both an O(n_hosts) perf issue and a latent regex-injection risk if an excluded hostname contains regex metacharacters.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Both host-exclusion filter call sites in gpu_utils_polars.py use a single combined, re.escape()'d regex scan instead of a per-host loop, matching the pandas implementation in gpu_utils.py
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Look at the pandas fix in gpu_utils.py (commit b6e23a1) for the reference implementation\n2. Find both host-exclusion call sites in gpu_utils_polars.py that still loop per-host\n3. Replace with a single re.escape()'d regex alternation, matching gpu_utils.py\n4. Verify with a test that a hostname containing regex metacharacters is excluded correctly
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Applied the same fix as commit b6e23a1 (gpu_utils.py's pandas version) to both host-exclusion call sites in gpu_utils_polars.py (filter_df, filter_df_enhanced): replaced the per-host str.contains() loop with a single re.escape()'d regex alternation scanned once.

Added tests/test_gpu_utils_polars.py covering both functions with a hostname containing an unbalanced parenthesis. Confirmed the test fails without the fix (polars.exceptions.ComputeError: unclosed group -- the exact regex-injection risk the task described) and passes with it, including a multi-host exclusion case exercising the single combined scan.
<!-- SECTION:NOTES:END -->
