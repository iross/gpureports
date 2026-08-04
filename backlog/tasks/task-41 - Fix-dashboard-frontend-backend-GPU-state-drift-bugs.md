---
id: TASK-41
title: Fix dashboard frontend/backend GPU-state drift bugs
status: Done
assignee:
  - '@claude'
created_date: '2026-07-31 20:48'
updated_date: '2026-08-04 00:00'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
dashboard/static/app.js hardcodes its own copies of two things the backend already computes correctly, and they've drifted: the 'Backfill' category filter (CATEGORY_CODES) only includes state 4, missing idle_backfill (state 6) that the backend's _CATEGORY_CODES and the Charts tab both count as backfill; and STATE_COLORS[6] is set to the same orange as state 1, while the backend returns a distinct dark blue for state 6. Both cause idle-backfill GPUs to be mis-filtered or mis-colored on the heatmap.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Selecting the 'Backfill' category filter includes idle_backfill (state 6) GPUs, matching the backend's category definition
- [x] #2 Idle-backfill GPUs render in a visually distinct color from idle-open-capacity GPUs, matching the backend's state_colors
- [x] #3 app.js reads state labels/colors from the /api/heatmap response (state_map/state_colors) instead of maintaining a separate hardcoded copy that can drift again
<!-- AC:END -->

## Implementation Notes

- `CATEGORY_CODES.backfill` in app.js now includes state 6, matching the backend's `_CATEGORY_CODES`.
- Removed the hardcoded `STATE_LABELS`/`STATE_COLORS` globals in app.js and replaced them with `stateLabel()`/`stateColor()` helpers that read from the `/api/heatmap` response's `state_map`/`state_colors` fields (verified against a live server response that the derived labels match the old hardcoded text exactly, and that state 6 now gets its own distinct color `#334499` instead of colliding with state 1's `#ff8800`).
- Found and fixed a third hardcoded copy in `dashboard/templates/index.html` (the heatmap legend), which was also missing state 6 entirely. Replaced the static legend markup with an empty container populated by a new `renderLegend()` function driven by the same API data, so there's a single source of truth instead of three.
- Modified files: `dashboard/static/app.js`, `dashboard/templates/index.html`.
