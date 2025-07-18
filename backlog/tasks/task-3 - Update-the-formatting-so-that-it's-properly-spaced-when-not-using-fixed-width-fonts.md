---
id: task-3
title: >-
  Update the formatting so that it's properly spaced when not using fixed-width
  fonts
status: Done
assignee:
  - '@iross'
created_date: '2025-07-14'
updated_date: '2025-07-14'
labels: []
dependencies: []
---

## Description

## Implementation Plan

1. Identify all print statements using fixed-width formatting patterns in usage_stats.py\n2. Replace fixed-width alignment with proper spacing for proportional fonts\n3. Use consistent indentation and spacing that works with both font types\n4. Test the output formatting

## Implementation Notes

Updated formatting in usage_stats.py to work better with proportional fonts by:\n- Removing fixed-width alignment directives like :>10, :6.1f that only work with monospace fonts\n- Using consistent indentation (2 spaces, 4 spaces for nested items)\n- Replacing columnar layouts with pipe-separated format for tables\n- Simplifying numeric formatting to remove unnecessary padding\n- Using simple dashes for separators instead of complex spacing\n\nThe output now displays properly in both monospace and proportional fonts.
