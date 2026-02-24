---
id: task-23
title: Refactor usage_stats.py
status: Done
assignee: []
created_date: '2025-09-15 16:36'
updated_date: '2026-02-24 20:12'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
- `usage_stats.py` has become an enormous file, and it can likely benefit from a re-visit and some refactoring.  This probably dovetails (or overlaps entirely) task 20's cleanup focus, but that was more used for the performance improvements. This task focuses on refactoring the code to improve readability and maintainability.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 usage_stats.py reduced to ~305 lines (orchestration + CLI only),stats_data.py created with data loading/caching functions (282 lines),stats_calculations.py created with all calculate_* and GPU model functions (1370 lines),stats_reporting.py created with HTML/text/email output functions (1647 lines),All 49 tests pass,ruff check passes on all modules,Dependent scripts updated to use new import paths
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Read usage_stats.py in sections to map function locations\n2. Create stats_data.py with caching globals and data loading functions\n3. Create stats_calculations.py with all calculate_* and GPU model analysis functions\n4. Create stats_reporting.py with HTML generation, text output, and email functions\n5. Rewrite usage_stats.py as thin orchestrator (run_analysis + main)\n6. Update tests/test_usage_stats.py imports\n7. Update dependent scripts (plot_usage_stats.py, etc.)\n8. Verify all 49 tests pass and ruff is clean
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Split 3552-line usage_stats.py into four focused modules:\n- stats_data.py (282 lines): _dataframe_cache, _filtered_cache globals; get_preprocessed_dataframe, get_cached_filtered_dataframe, clear_dataframe_cache, get_time_filtered_data, get_multi_db_data, get_time_filtered_data_multi_db\n- stats_calculations.py (1370 lines): all calculate_* functions and GPU model analysis\n- stats_reporting.py (1647 lines): print_gpu_model_analysis, send_email_report, simple_markdown_to_html, load_methodology, generate_html_report, print_analysis_results\n- usage_stats.py (305 lines): run_analysis() orchestration + main() Typer CLI\nAlso updated imports in scripts/plot_usage_stats.py, scripts/plot_example.py, scripts/investigate_backfill_usage.py, analysis/analyze_task7_troubleshoot.py, usage_stats_polars.py. All 49 tests pass, ruff clean.
<!-- SECTION:NOTES:END -->
