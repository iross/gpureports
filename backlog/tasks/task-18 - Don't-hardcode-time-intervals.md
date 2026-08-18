---
id: task-18
title: Don't hardcode time intervals
status: To Do
assignee: []
created_date: '2025-08-08'
updated_date: '2026-07-31 20:47'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The dashboard's date-range controls don't behave consistently: auto-refresh silently discards a user-selected custom range and reverts to a hardcoded 24-hour window, and the custom range form gives no feedback on invalid/incomplete input. This is part of the root cause behind the reported "picking a large date range only pulls in ~72 hours" symptom.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Auto-refresh preserves the user's active custom start/end instead of silently falling back to a hardcoded 24h window
- [ ] #2 Selecting an invalid range (start >= end, or one field empty) shows inline feedback instead of silently no-oping
- [ ] #3 Manual QA: set a multi-day custom range, wait through one auto-refresh cycle, and confirm the range is unchanged
<!-- AC:END -->

## Description
