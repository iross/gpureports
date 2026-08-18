---
id: TASK-38
title: Show window-average Prevented and fix Prevented total double-count
status: Done
assignee:
  - '@claude'
created_date: '2026-07-07 16:25'
updated_date: '2026-07-07 16:35'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The Prevented column is point-in-time, so a reason lifted before the report end renders as — even when hosts were prevented for most of the lookback window; the per-host table gives no hint the reason was lifted; and the TOTAL row sums Priority and Backfill classes, double-counting physical GPUs. Surface the already-computed window average in the table, add current-status context to the per-host table, and make totals count physical GPUs.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Real Slots Prevented column shows the window average of idle prevented GPUs
- [x] #2 Per-host PreventJobsReason table indicates whether the reason is still set and when it was last seen
- [x] #3 Prevented totals count physical GPUs once (no Priority+Backfill double-count)
- [x] #4 Point-in-time by-class section still shows current idle prevented GPUs
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. calculate_prevent_jobs_stats: add last_seen and active (still set in last PJ bucket) to per_host entries
2. stats_reporting Real Slots table: render per_class_avg as Prevented (avg.); tier rows use per_class_device_avg; totals use Priority+Shared only (physical GPUs)
3. Per-host table: add Status (active/lifted) and Last seen columns
4. Update footnotes and tests; verify against fresh July parquet
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Real Slots table Prevented column now renders per_class_avg (window average of idle prevented GPUs per 15-min bucket), renamed Prevented (avg.) to match the other columns; tier rows use per_class_device_avg. Totals count physical GPUs by summing only the Priority classes plus Shared — the Backfill classes run on the same GPUs, matching the convention the Available column already uses; the Secondary (Backfill) rows keep their own class values.

calculate_prevent_jobs_stats per_host entries gain last_seen (timestamp of the host's most recent PreventJobsReason row, minute precision) and active (whether the reason is still set in the most recent bucket with PJ data). The per-host HTML table shows Status (Active/Lifted) and Last seen columns with an explanatory footnote; the console summary prints the same. The point-in-time by-class section is unchanged and its footnote now explains how it differs from the averaged column.

Verified against the fresh 2026-07 parquet with a 24h window ending after INF-3809 was lifted: Open Capacity shows Prevented (avg.) 34.1 (Flagship 12.6 / Standard 21.5) where the point-in-time view showed only 8, per-host marks the INF-3809 hosts Lifted with lift times, and TOTAL is 72.2 instead of the double-counted 120.

Modified: stats_calculations.py, stats_reporting.py, tests/test_report_equivalence.py (new test: lifted reason marked inactive but kept in window average; extended HTML rendering assertions).
<!-- SECTION:NOTES:END -->
