---
id: task-1
title: Add host-level filtration with support for filtration message propagation
status: Done
assignee: []
created_date: '2025-07-08'
updated_date: '2025-07-08'
labels: []
dependencies: []
---

## Description
Certain hosts are showing up in the reports from usage_stats.py in incorrect categories due to having inconsistent configuration compared to the cluster's standard. It would be nice to be able to add some masking for specific hosts, with reasons for the masking being propagated into the final report.

## Acceptance Criteria

- [x] Host filtration can be configured to exclude specific hosts from analysis
- [x] Reasons for host exclusion are stored and accessible
- [x] Filtered hosts are noted in the analysis output with their exclusion reasons
- [x] Existing functionality remains unchanged when no hosts are filtered
- [x] Command-line option allows specifying hosts to exclude
- [x] Configuration supports multiple hosts and multiple exclusion reasons
- [x] Add functionality to read host masks from a yaml file
## Implementation Plan

1. Add command-line option for host exclusion configuration (JSON format or config file)\n2. Implement host filtration logic in filter_df and related functions\n3. Add exclusion message tracking and propagation through analysis functions\n4. Update print_analysis_results to display filtered host information\n5. Add configuration validation and error handling\n6. Update documentation and help text

## Implementation Notes

Successfully implemented host-level filtration with message propagation. Added --exclude-hosts CLI option that accepts JSON configuration. Implemented filtering logic in filter_df function that excludes hosts by machine name. Added metadata tracking and display of excluded hosts with reasons in reports. All existing functionality preserved when no exclusions are configured. Tested with multiple hosts and reasons. Files modified: usage_stats.py (added load_host_exclusions function, updated filter_df, run_analysis, print_analysis_results, and main functions).

Successfully implemented host-level filtration with message propagation. Added --exclude-hosts CLI option that accepts JSON configuration. Implemented filtering logic in filter_df function that excludes hosts by machine name. Added metadata tracking and display of excluded hosts with reasons in reports. All existing functionality preserved when no exclusions are configured. Tested with multiple hosts and reasons. Added YAML support with --exclude-hosts-yaml option for reading exclusions from YAML files. Added validation to prevent using both JSON and YAML options simultaneously. Files modified: usage_stats.py (added load_host_exclusions function with YAML support, updated filter_df, run_analysis, print_analysis_results, and main functions).
