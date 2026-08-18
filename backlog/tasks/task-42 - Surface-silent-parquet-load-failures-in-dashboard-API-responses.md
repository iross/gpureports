---
id: TASK-42
title: Surface silent parquet load failures in dashboard API responses
status: Done
assignee:
  - '@claude'
created_date: '2026-07-31 20:48'
updated_date: '2026-08-04 18:23'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
dashboard/data.py's per-file read loops (_query_dbs, get_opencap_users_data) wrap each monthly parquet/sqlite read in a bare except that only print()s a warning to server stdout — a corrupted or mid-write file silently drops that month's data with the API returning what looks like a complete, valid (but truncated) response. This masks real data-loss bugs, including the collector temp-file glob race fixed in e6bbbb4.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Per-file read failures are logged via the logging module instead of print()
- [x] #2 API responses from /api/heatmap, /api/counts, and /api/opencap_users include a 'warnings' field listing any months that failed to load
- [x] #3 The dashboard UI shows a non-blocking indicator when a response includes warnings, instead of silently rendering a truncated range as complete
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Per-file read failures in _query_dbs and get_opencap_users_data now go through logger.warning() instead of print(), and both functions return a warnings list alongside their data. get_heatmap_data, get_counts_data, and get_opencap_users_data (including their empty-response helpers) all surface this as a top-level "warnings" field — including a previously-missed early-return branch in get_opencap_users_data (data.py ~line 523) that dropped accumulated warnings when the post-filter dataframe was empty.

On the frontend, app.js collects and de-dupes warnings from the heatmap/counts/users responses after each fetch and renders them in a new non-blocking #warningsBanner bar (index.html/style.css) below the topbar; it's hidden whenever there are no warnings.

Verified by pointing get_heatmap_data/get_counts_data/get_opencap_users_data at a scratch directory containing one valid parquet file and one corrupted parquet file: all three returned partial data plus a descriptive warning naming the failed month. Also loaded the live dashboard (real data, uvicorn + Playwright/Chromium) to confirm the banner stays hidden with clean data and appears correctly when warnings are injected, with no console errors and no regression to the heatmap/legend.

Modified files: dashboard/data.py, dashboard/static/app.js, dashboard/static/style.css, dashboard/templates/index.html.
<!-- SECTION:NOTES:END -->
