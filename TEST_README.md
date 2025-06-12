# GPU Usage Statistics - Test Suite

This directory contains comprehensive unit tests for the GPU usage statistics calculator.

## Files

- `test_usage_stats.py` - Main test suite
- `run_tests.py` - Test runner script
- `TEST_README.md` - This documentation

## Running Tests

### Prerequisites

Install pytest if not already available:
```bash
pip install pytest
```

### Run All Tests

```bash
# Using pytest directly
python -m pytest test_usage_stats.py -v

# Using the test runner
python run_tests.py -v
```

### Run Specific Test Categories

```bash
# Filter tests only
python -m pytest test_usage_stats.py::TestFilterFunctions -v

# Calculation tests only
python -m pytest test_usage_stats.py::TestCalculationFunctions -v

# Database tests only
python -m pytest test_usage_stats.py::TestDatabaseFunctions -v

# Integration tests only
python -m pytest test_usage_stats.py::TestIntegrationFunctions -v

# Edge case tests only
python -m pytest test_usage_stats.py::TestEdgeCases -v
```

### Run Individual Tests

```bash
# Test specific filtering functionality
python -m pytest test_usage_stats.py::TestFilterFunctions::test_filter_priority_claimed -v

# Test allocation calculations
python -m pytest test_usage_stats.py::TestCalculationFunctions::test_calculate_allocation_usage -v
```

## Test Coverage

The test suite covers:

### Core Filtering Functions
- `filter_df()` - Tests all GPU class filtering (Priority, Shared, Backfill)
- Host pattern filtering
- GPU conflict resolution for duplicate assignments
- State filtering (Claimed/Unclaimed)

### Calculation Functions
- `calculate_allocation_usage()` - Allocation-based usage percentages
- `calculate_time_series_usage()` - Time series analysis with bucketing
- `calculate_allocation_usage_by_device()` - Device-grouped analysis
- Device type filtering (old vs. new GPUs)

### Database Functions
- `get_time_filtered_data()` - Time range filtering from SQLite
- Database connection handling
- Empty database scenarios
- Time window calculations

### Integration Functions
- `run_analysis()` - End-to-end analysis pipeline
- Different analysis types (allocation, timeseries, device grouping)
- Result structure validation
- Error handling

### Edge Cases & Error Conditions
- Empty DataFrames
- Missing required columns
- Invalid utilization values
- Malformed timestamps
- Database connection errors

## Test Data

Tests use synthetic GPU data that includes:

- **Priority slots**: Slots with `PrioritizedProjects` set, not containing "backfill"
- **Shared slots**: Slots with empty `PrioritizedProjects`, not containing "backfill"  
- **Backfill slots**: Slots with "backfill" in the slot name
- **Multiple GPU types**: Tesla V100-SXM2-32GB, Tesla A100-SXM4-40GB
- **Different states**: Claimed, Unclaimed
- **Time series data**: Multiple 15-minute intervals for testing bucketing
- **GPU conflicts**: Same GPU assigned to multiple slots (for conflict resolution testing)

## Key Test Scenarios

### GPU Class Classification
Tests verify that GPUs are correctly classified into Priority, Shared, and Backfill categories based on:
- `PrioritizedProjects` field content
- Slot name patterns (backfill detection)
- State information

### Usage Calculations
Tests verify that usage percentages are calculated correctly:
- Proper counting of unique GPUs per time interval
- Averaging across multiple time buckets
- Handling of missing or invalid data

### Device Grouping
Tests verify device-specific analysis:
- Correct grouping by GPU model
- Optional filtering of old GPU types
- Grand total calculations

### Time Series Analysis
Tests verify time-based analysis:
- Correct 15-minute bucketing
- Unique GPU counting per interval
- Time series data structure

## Continuous Integration

The test suite is designed to:
- Run quickly (< 1 minute for full suite)
- Use temporary databases for isolation
- Clean up resources automatically
- Provide clear failure messages
- Support both local development and CI environments

## Adding New Tests

When adding new functionality to `usage_stats.py`, add corresponding tests:

1. **Unit tests** for individual functions
2. **Integration tests** for end-to-end workflows  
3. **Edge case tests** for error conditions
4. **Data validation tests** for input/output formats

Follow the existing test patterns and use descriptive test names that explain what is being tested.