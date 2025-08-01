---
id: task-6
title: Comprehensive test suite for GPU monitoring system
status: Draft
assignee: []
created_date: '2025-07-18'
labels: []
dependencies: []
---

## Description

Create a comprehensive test suite for the GPU health monitoring system to ensure reliability, accuracy, and performance across all functionality. Currently, the system has grown organically with limited formal testing, making it difficult to verify correctness and catch regressions when making changes.

This task will establish a robust testing framework covering data collection, calculations, HTML generation, email functionality, and the new host-level features. The test suite should provide confidence for future development and help identify edge cases or performance issues.

## Acceptance Criteria

- [ ] Unit tests cover all core calculation functions with edge cases
- [ ] Integration tests validate end-to-end report generation pipeline  
- [ ] Email functionality is thoroughly tested across different scenarios
- [ ] Host exclusion logic is validated for both statistical and operational use cases
- [ ] HTML generation produces correct output with proper styling and positioning
- [ ] Performance tests ensure scalability with large datasets
- [ ] Regression tests protect against breaking changes to existing functionality
- [ ] Test data fixtures provide consistent, reproducible test scenarios
- [ ] Test documentation explains how to run tests and interpret results

## Implementation Plan

### 1. Test Infrastructure Setup
- Set up pytest framework with appropriate configuration
- Create test data fixtures for consistent testing scenarios
- Set up test database with sample HTCondor data
- Configure test environment with mock email capabilities
- Create helper functions for common test operations

### 2. Core Calculation Function Tests

**Data Collection Tests:**
- [ ] Test `get_gpus()` HTCondor data collection with various cluster states
- [ ] Test GPU data cleaning and normalization (GPU-/GPU_ replacement, backfill assignment)
- [ ] Test DataFrame exploding and timestamp assignment
- [ ] Test handling of missing or malformed HTCondor data

**Device-Level Calculation Tests:**
- [ ] Test `calculate_allocation_usage_by_device()` with normal scenarios
- [ ] Test with different device types (RTX 4090, A100, older GPUs)
- [ ] Test with various GPU counts per device type (1, 4, 8, 16 GPUs)
- [ ] Test Priority/Shared/Backfill classification logic
- [ ] Test 15-minute interval bucketing and averaging
- [ ] Test with all claimed, all unclaimed, and mixed scenarios
- [ ] Test percentage calculations and edge cases (division by zero)

**Filter Function Tests:**
- [ ] Test `filter_df()` for each utilization type (Priority, Shared, Backfill)
- [ ] Test Priority duplicate handling logic for claimed/unclaimed overlap
- [ ] Test host filtering and exclusion application
- [ ] Test edge cases with empty DataFrames
- [ ] Test state filtering (Claimed vs Unclaimed)

**Host Exclusion Tests:**
- [ ] Test host exclusion loading from JSON and YAML formats
- [ ] Test exclusion application in statistical calculations
- [ ] Test exclusion tracking for reporting
- [ ] Test exclusion bypass for operational visibility

### 3. New Host-Level Functionality Tests

**Host Calculation Function Tests:**
- [ ] Test `calculate_allocation_usage_by_host()` with normal data
- [ ] Test with hosts having different GPU counts (4 GPUs, 8 GPUs, etc.)
- [ ] Test with mixed device types across different hosts
- [ ] Test with all GPUs claimed vs all unclaimed scenarios
- [ ] Test with partial GPU allocation (50%, 75%, etc.)
- [ ] Test exclusion marking - verify excluded hosts are flagged correctly
- [ ] Test alphabetical sorting within device type groups
- [ ] Test empty data handling (no hosts, no GPUs)

**Data Structure Tests:**
- [ ] Verify returned data structure matches expected format
- [ ] Test device type grouping is correct
- [ ] Test host metadata (exclusion status) is preserved
- [ ] Test calculation consistency with existing device-level calculations

### 4. HTML Generation Tests

**Report Structure Tests:**
- [ ] Test overall HTML report structure and sections
- [ ] Test table positioning (Cluster Summary → Host Summary → Device Types → Excluded Hosts)
- [ ] Test CSS styling consistency across tables
- [ ] Test responsive design with various data sizes

**Host Summary Table Tests:**
- [ ] Test host table appears in correct position
- [ ] Test device type group headers render correctly
- [ ] Test host rows render with proper data
- [ ] Test excluded host styling is applied
- [ ] Test right-alignment of numeric columns
- [ ] Test alphabetical sorting within device groups

**Device Type Table Tests:**
- [ ] Test device tables render with correct data
- [ ] Test TOTAL row calculations (including double-counting fix)
- [ ] Test terminology updates (Allocation vs Utilization)
- [ ] Test class name display (Prioritized service, Open Capacity, Backfill)
- [ ] Test column separation (Allocated vs Available)
- [ ] Test alphabetical device ordering

**Methodology Section Tests:**
- [ ] Test methodology.md loading and HTML conversion
- [ ] Test methodology section positioning
- [ ] Test markdown formatting preservation

### 5. Email Integration Tests

**Email Generation Tests:**
- [ ] Test email subject generation with usage percentages
- [ ] Test lookback period formatting (24h, 2d, 3w)
- [ ] Test email HTML content generation
- [ ] Test email size limits with large host tables
- [ ] Test email client compatibility (Gmail, Outlook, Apple Mail)
- [ ] Test plain text email fallback

**Email Configuration Tests:**
- [ ] Test SMTP configuration and connection
- [ ] Test email sending with various recipient configurations
- [ ] Test email error handling and retry logic

### 6. Integration Tests

**End-to-End Pipeline Tests:**
- [ ] Test complete analysis pipeline from data loading to report generation
- [ ] Test with various time periods (1d, 7d, 30d)
- [ ] Test with different analysis types (allocation, timeseries)
- [ ] Test command-line interface with various options
- [ ] Test file I/O (database connections, file writing)

**Cross-Component Integration:**
- [ ] Test data flow between calculation functions and HTML generation
- [ ] Test consistency between statistical tables and host tables
- [ ] Test host exclusion behavior across all components

### 7. Performance and Scalability Tests

**Large Dataset Tests:**
- [ ] Test performance with 100+ hosts
- [ ] Test memory usage with large datasets (months of data)
- [ ] Test HTML rendering time with many hosts and device types
- [ ] Test email generation time with large reports
- [ ] Test database query performance with large time ranges

**Resource Usage Tests:**
- [ ] Test memory consumption during calculation phases
- [ ] Test CPU usage with complex calculations
- [ ] Test file system usage for temporary files

### 8. Edge Cases and Error Handling Tests

**Data Quality Tests:**
- [ ] Test with missing GPU device names
- [ ] Test with inconsistent data across time intervals
- [ ] Test with malformed HTCondor data
- [ ] Test with zero GPU scenarios
- [ ] Test with hosts that appear/disappear during time periods

**Error Handling Tests:**
- [ ] Test database connection failures
- [ ] Test email sending failures
- [ ] Test file system errors (permissions, disk space)
- [ ] Test invalid configuration scenarios

### 9. Regression Tests

**Task-4 Implementation Protection:**
- [ ] Test terminology changes are preserved (Allocation vs Utilization)
- [ ] Test table ordering (Open Capacity, Prioritized service, Backfill)
- [ ] Test column alignment (right-aligned numbers)
- [ ] Test email subject enhancements
- [ ] Test column separation (Allocated/Available)
- [ ] Test methodology section inclusion
- [ ] Test alphabetical device ordering
- [ ] Test double-counting fix in TOTAL rows

**Backward Compatibility Tests:**
- [ ] Test existing command-line interfaces still work
- [ ] Test existing configuration file formats
- [ ] Test existing database schema compatibility

### 10. Test Data and Documentation

**Test Fixtures:**
- [ ] Create representative HTCondor data samples
- [ ] Create edge case datasets (empty, malformed, large)
- [ ] Create known-good calculation results for validation
- [ ] Create sample configuration files

**Documentation:**
- [ ] Write test running instructions
- [ ] Document test data creation process
- [ ] Create troubleshooting guide for test failures
- [ ] Document performance benchmarks and thresholds
