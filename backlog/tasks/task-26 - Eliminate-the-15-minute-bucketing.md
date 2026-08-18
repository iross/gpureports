---
id: task-26
title: Eliminate the 15 minute bucketing
status: To Do
assignee: []
created_date: '2026-01-02 15:35'
updated_date: '2026-07-31 20:47'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The heatmap's time-bucket size is a 3-tier step function capped at 60 minutes (bucketForRange in app.js), and the custom date-range path ignores it entirely, hardcoding bucket_minutes=15. For large ranges (multi-week/multi-month) this produces thousands of thin heatmap columns, which is both slow to render and can approach browser canvas width limits. Replace this with a bucket size that scales continuously with the requested range so the heatmap stays a bounded, readable width regardless of range length, using the existing claimed > backfill > idle dedup rank hierarchy to decide which state a widened bucket displays (no new aggregation logic needed — dashboard/data.py already truncates timestamps to bucket_minutes before ranking, so a wider bucket naturally rolls up more raw samples per box).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 bucketForRange() picks bucket size from a 'nice' size ladder so total column count stays under a fixed target for any requested range, replacing the current 3-tier step function
- [ ] #2 The custom date-range 'Go' handler uses the same bucket-sizing function as the preset buttons instead of hardcoding 15 minutes
- [ ] #3 A GPU that is busy for only part of a wide time bucket still displays as busy in that bucket, per the existing claimed/backfill/idle rank hierarchy
- [ ] #4 Time-axis tick labels remain readable (not crowded or too sparse) across the full range of bucket sizes, from 5 minutes to multi-day
- [ ] #5 Manual QA: a multi-month custom range renders with a bounded, reasonable column count instead of thousands of columns
<!-- AC:END -->
