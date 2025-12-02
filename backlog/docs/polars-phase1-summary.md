# Polars Migration - Phase 1 Summary

**Task:** task-25 - Change dataframe backend from pandas to polars (experiment)  
**Phase:** Phase 1 - Research and Setup  
**Date Completed:** 2025-12-02  
**Status:** ✅ Complete

## Objectives

Phase 1 focused on research, setup, and creating infrastructure to support the Polars migration experiment:

1. Research Polars API compatibility with pandas operations
2. Add Polars to project dependencies
3. Install and verify Polars in the development environment
4. Create benchmarking infrastructure
5. Create memory profiling infrastructure

## Deliverables

### 1. API Mapping Documentation

**File:** `backlog/docs/polars-migration-api-mapping.md`

Comprehensive documentation covering:
- Core DataFrame operations (creation, copying, shape)
- Data type operations (datetime conversion, type checking)
- Filtering and boolean indexing
- String operations
- Sorting and deduplication
- Aggregation and grouping
- Column operations
- Datetime operations
- DataFrame combining (concat, merge/join)
- Data access patterns
- SQL integration

**Key findings:**
- Most pandas operations have direct Polars equivalents
- Main API differences:
  - Boolean indexing: `df[condition]` → `df.filter(condition)`
  - GroupBy: `.groupby()` → `.group_by()` (underscore)
  - Deduplication: `.drop_duplicates()` → `.unique()`
  - Sorting: `.sort_values()` → `.sort()`
  - Copy: `.copy()` → `.clone()`
  - DateTime bucketing: `"15min"` → `"15m"`
  - String contains (case-insensitive): use regex `(?i)`

**Identified challenges:**
- Index concept: Polars doesn't have row indices like pandas
- Null handling: Different semantics (NaN vs null)
- SQL reading: May need conversion step
- Lazy vs eager evaluation: Need to use eager API for compatibility

### 2. Dependency Updates

**File:** `pyproject.toml`

Added dependencies:
- `polars>=0.20.0` - Main DataFrame library
- `psutil>=5.9.0` - For accurate memory profiling

**Installation:**
- Verified Polars 1.35.2 installed successfully
- Tested basic DataFrame creation and operations
- Confirmed compatibility with Python 3.11+

### 3. Benchmarking Infrastructure

**File:** `scripts/benchmark_polars.py`

Comprehensive benchmarking script that measures:

**Operations tested:**
1. Data loading from SQLite
2. Datetime conversion
3. Boolean filtering (multi-condition)
4. String operations (case-insensitive contains)
5. Deduplication (drop_duplicates/unique)
6. Sorting (multi-column)
7. GroupBy + aggregation
8. Datetime bucketing (15-minute intervals)
9. Null handling/filtering

**Features:**
- Multiple iterations with averaging
- Configurable database path and row limit
- Detailed per-operation results
- Overall statistics (total time, average speedup, median speedup)
- Best/worst performer identification
- Speedup calculation in both ratio and percentage

**Usage:**
```bash
uv run python scripts/benchmark_polars.py --limit 10000 --iterations 3
```

### 4. Memory Profiling Infrastructure

**File:** `scripts/profile_memory.py`

Memory profiling script that measures:

**Operations profiled:**
1. Data loading from SQLite
2. Datetime conversion
3. Boolean filtering
4. Deduplication
5. GroupBy + aggregation
6. Multiple copy operations

**Features:**
- Uses `psutil` for accurate RSS memory measurement
- Measures baseline and peak memory for each operation
- Calculates memory delta and savings
- Garbage collection before each measurement
- Detailed per-operation memory breakdown
- Overall statistics and best/worst performers

**Usage:**
```bash
uv run python scripts/profile_memory.py --limit 10000
```

## Pandas Operations Inventory

### gpu_utils.py
- Type hints: `pd.DataFrame`
- Boolean filtering with multiple conditions
- String operations: `.str.contains()` with case-insensitivity
- Duplicate detection: `.duplicated(keep=False)`
- Sorting: `.sort_values()` with multiple columns
- Deduplication: `.drop_duplicates(subset=[], keep="first")`
- Column operations: adding/dropping columns
- Datetime operations: `.dt.date`, `.dt.floor()`
- Aggregation: `.groupby().first().reset_index()`
- SQL reading: `pd.read_sql_query()`
- Datetime conversion: `pd.to_datetime()`
- Unique counting: `.nunique()`

### usage_stats.py
- DataFrame caching in dictionaries
- Type checking: `pd.api.types.is_datetime64_any_dtype()`
- Datetime conversion: `pd.to_datetime()`
- Datetime bucketing: `.dt.floor("15min")`
- SQL operations: `pd.read_sql_query()` with parameters
- Concatenation: `pd.concat()` with `ignore_index=True`
- Empty DataFrame creation: `pd.DataFrame()`
- Null checking: `pd.isna()`
- Data access: `.iloc[0]`

## Key Technical Decisions

### 1. Eager API Usage
**Decision:** Use Polars eager API instead of lazy API for initial migration  
**Rationale:** Maintains similar behavior to pandas, easier migration path  
**Future:** Can optimize with lazy API after migration is complete

### 2. Conversion Strategy for SQL
**Decision:** Convert pandas → Polars after reading from SQLite  
**Rationale:** Polars SQL support requires connection strings; pandas has mature sqlite3 integration  
**Future:** Explore direct Polars database reading with connectorx

### 3. Benchmark Methodology
**Decision:** Run multiple iterations and average results  
**Rationale:** Reduces variance from system noise, provides more reliable measurements  
**Implementation:** Default 3 iterations, configurable

### 4. Memory Profiling Approach
**Decision:** Use psutil for RSS memory measurement  
**Rationale:** Most accurate method, measures actual process memory  
**Fallback:** Basic tracking if psutil not available

## Expected Performance Gains

Based on Polars benchmarks from similar workloads:

- **Filtering operations:** 2-10x faster
- **GroupBy operations:** 5-20x faster
- **String operations:** 2-5x faster
- **Memory usage:** 30-50% reduction

Actual results will be measured in Phase 5 with real GPU state data.

## Next Steps (Phase 2)

Phase 2 will focus on migrating core utilities in `gpu_utils.py`:

1. Migrate simple utility functions (no DataFrame operations)
2. Migrate filtering functions (`filter_df`, `filter_df_enhanced`)
3. Migrate count functions
4. Implement duplicate detection and ranking logic
5. Test each function with sample data
6. Handle SQLite data loading

**Estimated effort:** 3 days

## Files Modified

- `pyproject.toml` - Added polars and psutil dependencies
- `backlog/docs/polars-migration-api-mapping.md` - New API mapping documentation
- `scripts/benchmark_polars.py` - New benchmarking script
- `scripts/profile_memory.py` - New memory profiling script
- `backlog/docs/polars-phase1-summary.md` - This summary document

## Important Findings

### Datetime String Conversion Issue

**Issue Discovered:** Polars `.cast(pl.Datetime)` fails when converting string timestamps, requiring `.str.strptime()` instead.

**Error encountered:**
```
InvalidOperationError: conversion from `str` to `datetime[μs]` failed
```

**Solution implemented:**
```python
# Check schema and use appropriate conversion method
if df.schema["timestamp"] == pl.Utf8:
    df = df.with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f")
    )
else:
    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime))
```

**Impact:**
- Fixed in both benchmark and memory profiling scripts
- Documented in API mapping with examples
- This pattern will be critical for Phase 2 migration

### Benchmarking Methodology Issues

**Issue Discovered:** Initial benchmark design unfairly penalized Polars by counting conversion overhead in every test.

**Problems identified:**
1. Every Polars test included `pl.from_pandas()` conversion time
2. Data loading benchmark double-counted pandas work
3. Dataset size too small (10K rows) - favors pandas over Polars
4. Single iterations don't amortize Polars compilation costs

**Solution created:**
- New fair benchmark script: `scripts/benchmark_polars_fair.py`
- Pre-converts data to both formats before timing
- Only measures actual operation time (no conversion overhead)
- Uses realistic dataset size (50K rows default)
- Multiple iterations (5) to reduce variance
- Separate benchmarks for loading vs operations

**Expected performance with fair benchmarks:**
- GroupBy: 2-10x faster
- Sorting: 1.5-5x faster  
- String ops: 1.5-3x faster
- Filtering: 1.5-4x faster
- SQLite loading: ~20% slower (one-time cost, amortized over operations)

**Documentation:**
- Created `backlog/docs/polars-benchmarking-methodology.md`
- Explains methodology issues and fixes
- Provides interpretation guidelines
- Sets recommendation thresholds (20% improvement target)

## Risks and Mitigations

### Identified Risks

1. **API Differences:** Some pandas operations don't have direct equivalents
   - **Mitigation:** Comprehensive API mapping document created
   - **Action:** Discovered datetime conversion gotcha and documented solution

2. **Learning Curve:** Team needs to learn Polars idioms
   - **Mitigation:** Detailed documentation and examples provided

3. **SQL Integration:** Conversion overhead from pandas → Polars
   - **Mitigation:** Benchmark will measure overhead; future optimization possible

4. **Downstream Dependencies:** Plotting libraries may need pandas
   - **Mitigation:** Can convert back with `.to_pandas()` only for visualization

5. **Index Semantics:** Polars has no row index concept
   - **Mitigation:** Current code doesn't rely heavily on pandas index

6. **Datetime Type Handling:** String timestamps require special handling
   - **Mitigation:** Schema checking pattern documented and implemented

### Risk Assessment

**Overall risk level:** LOW to MEDIUM

The infrastructure and research completed in Phase 1 significantly reduces technical risk. The main remaining risks are:
- Performance gains may not meet 20% target (will know after Phase 5)
- Effort required for full migration may be higher than estimated

## Conclusion

Phase 1 is complete and successful. All objectives were met:

✅ Comprehensive API mapping documentation created  
✅ Polars added to dependencies and verified working  
✅ Robust benchmarking infrastructure created  
✅ Memory profiling infrastructure created  
✅ pandas operations inventory completed  
✅ Technical decisions documented

The project is well-positioned to proceed to Phase 2 (Core Utilities Migration).

## Recommendations

1. **Run benchmarks early:** Execute benchmarking script with production data to validate expected performance gains
2. **Incremental migration:** Migrate one module at a time with full test coverage
3. **Maintain compatibility:** Keep both pandas and Polars working during experimentation
4. **Document learnings:** Update API mapping document as new patterns are discovered
5. **Performance validation:** Benchmark each migrated function to ensure no regressions

---

**Phase 1 Status:** ✅ COMPLETE  
**Ready for Phase 2:** YES  
**Blocking Issues:** NONE
