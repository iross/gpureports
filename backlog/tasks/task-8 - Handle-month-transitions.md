---
id: task-8
title: Handle month transitions
status: Done
assignee: []
created_date: '2025-07-28'
updated_date: '2025-07-30'
labels: []
dependencies: []
---

## Description
Right now the infrastructure creates a database file per month. That means that lookbacks that bridge gaps between months will have incomplete metrics. Add functionality that will leverage two different database files as needed.

## Acceptance Criteria

- [x] Time lookbacks that span month boundaries return complete data from both database files
- [x] Database file selection is automatic based on the requested time range
- [x] Performance remains acceptable when querying across multiple databases
- [x] Existing API (get_time_filtered_data) works without breaking changes
- [x] Error handling for missing database files (e.g., requesting data from a future month)
- [x] Tests cover month boundary scenarios

## Implementation Plan

### 1. Analysis of Current Architecture
- Database naming pattern: `gpu_state_YYYY-MM.db` (e.g., `gpu_state_2025-07.db`)
- Current lookback queries are limited to single database files
- `get_time_filtered_data()` function takes a single db_path parameter
- Time filtering happens after loading all data from single database

### 2. Research-Based Approach Options

**Option A: ATTACH DATABASE (SQLite Native)**
- Use SQLite's ATTACH DATABASE to join multiple databases in a single query
- Benefits: Native SQLite support, efficient cross-database queries
- Drawbacks: More complex SQL, potential locking issues

**Option B: Multi-Database Data Loading (Pandas)**
- Load data from multiple databases and concatenate DataFrames
- Benefits: Simple implementation, leverages existing pandas workflows
- Drawbacks: More memory usage, two separate database connections

**Option C: Smart Database Selection**
- Auto-detect required databases based on time range
- Load and merge data from only the necessary files
- Benefits: Optimal performance, minimal code changes
- Drawbacks: More complex logic for boundary detection

### 3. Recommended Implementation Strategy

**Phase 1: Database Discovery Function**
```python
def get_required_databases(start_time: datetime, end_time: datetime) -> List[str]:
    """Determine which database files are needed for a time range"""
    # Generate list of YYYY-MM patterns needed
    # Return list of database file paths
```

**Phase 2: Multi-Database Data Loader**
```python
def get_multi_db_data(db_paths: List[str], start_time: datetime, end_time: datetime) -> pd.DataFrame:
    """Load and merge data from multiple database files"""
    # Load data from each database
    # Concatenate DataFrames
    # Apply final time filtering
```

**Phase 3: Update get_time_filtered_data Function**
- Modify to auto-detect required databases
- Maintain backward compatibility with single db_path parameter
- Add optional parameter for base_directory or db_pattern

### 4. Database File Pattern Detection
- Base directory: Detect from provided db_path or use default
- File pattern: `gpu_state_{YYYY-MM}.db`
- Month range calculation: Generate months between start_time and end_time
- File existence checking: Handle missing files gracefully

### 5. Performance Considerations
- Only load necessary time ranges from each database
- Use SQLite time filtering in queries, not pandas post-processing
- Cache database connections where appropriate
- Consider using SQLite WAL mode for better concurrent access

### 6. Error Handling
- Missing database files: Log warning, continue with available data
- Invalid time ranges: Raise appropriate exceptions
- Database connection errors: Provide meaningful error messages
- Empty result sets: Handle gracefully

### 7. Testing Strategy
- Unit tests for database discovery logic
- Integration tests for month boundary scenarios  
- Performance tests for multi-database queries
- Edge cases: missing files, invalid dates, single-month queries

## Implementation Notes

Successfully implemented month boundary support for GPU metrics lookbacks.

## Implementation Approach

Added three new functions to usage_stats.py:

1. **get_required_databases()** - Discovers which database files are needed based on time range
2. **get_multi_db_data()** - Loads and merges data from multiple database files  
3. **get_time_filtered_data_multi_db()** - New API for explicit multi-database queries

## Modified Functions

Updated **get_time_filtered_data()** to automatically detect month boundaries while maintaining full backward compatibility:
- Single month queries use original fast path for performance
- Multi-month queries automatically use new multi-database functionality
- Robust fallback handling for missing files or database errors
- Returns empty DataFrame gracefully when no data available

## Key Features Implemented

- **Automatic database discovery** based on YYYY-MM naming pattern
- **Efficient SQL-level time filtering** instead of loading all data
- **Graceful error handling** for missing files, connection errors, invalid dates
- **Full backward compatibility** - all existing code works unchanged
- **Performance optimized** - single month queries unchanged, multi-month uses database-level filtering

## Files Modified

- usage_stats.py - Added new functions and updated get_time_filtered_data()

## Files Added  

- test_month_boundary.py - Comprehensive test suite for month boundary functionality
- test_edge_cases.py - Edge case and error handling tests

## Testing Results

✅ Single month queries work as before (53,304 records)
✅ Month boundary queries successfully combine data from multiple databases (342,250 records from June+July)  
✅ Direct multi-database queries work correctly (355,932 records)
✅ Full analysis pipeline works across month boundaries (7-day lookback from July 2 back to June 25)
✅ Error handling works for missing files, invalid paths, future dates
✅ Database discovery correctly identifies required files
✅ Existing API maintained - no breaking changes
