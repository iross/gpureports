# Phase 3 Strategy: Usage Statistics Migration

## Analysis

**File:** `usage_stats.py`  
**Size:** 3,182 lines, 28 functions  
**Complexity:** Very high - includes data loading, caching, calculations, reporting, and email

## Function Categories

### Core Data Processing (Priority 1) - 8 functions
1. `get_preprocessed_dataframe()` - Timestamp conversion and bucketing
2. `get_cached_filtered_dataframe()` - Caching layer
3. `get_time_filtered_data()` - Load data from single DB
4. `get_multi_db_data()` - Load from multiple DBs
5. `calculate_allocation_usage()` - Basic allocation calculations
6. `calculate_allocation_usage_enhanced()` - Enhanced allocation
7. `calculate_time_series_usage()` - Time series data
8. `calculate_unique_cluster_totals_from_raw_data()` - Cluster totals

### Device/Memory Breakdown (Priority 2) - 4 functions
9. `calculate_allocation_usage_by_device()` - By GPU model
10. `calculate_allocation_usage_by_device_enhanced()` - Enhanced by device
11. `calculate_allocation_usage_by_memory()` - By memory size
12. `calculate_performance_usage()` - Performance metrics

### User Analysis (Priority 3) - 2 functions
13. `calculate_h200_user_breakdown()` - H200 user stats
14. `calculate_backfill_usage_by_user()` - Backfill user stats

### Analysis & Time-Point (Priority 3) - 4 functions
15. `get_gpu_models_at_time()` - Models at specific time
16. `get_gpu_model_activity_at_time()` - Activity at time
17. `analyze_gpu_model_at_time()` - Analyze at time
18. `print_gpu_model_analysis()` - Print analysis

### Reporting & Output (Priority 4) - 6 functions
19. `run_analysis()` - Main analysis runner
20. `calculate_monthly_summary()` - Monthly summaries
21. `generate_html_report()` - HTML generation
22. `print_analysis_results()` - Print results
23. `send_email_report()` - Email sending
24. `simple_markdown_to_html()` - Markdown conversion

### Utility (Priority 4) - 4 functions
25. `clear_dataframe_cache()` - Cache management
26. `get_time_filtered_data_multi_db()` - Multi-DB wrapper
27. `load_methodology()` - Load docs
28. `main()` - CLI entry point

## Recommended Phase 3 Approach

### Option A: Incremental Migration (Recommended)

**Week 1: Core Data Processing**
- Migrate Priority 1 functions (8 functions)
- Create `usage_stats_polars.py` with core calculations
- Test with production data
- **Deliverable:** Working Polars version for basic usage stats

**Week 2: Enhanced Calculations**
- Migrate Priority 2 functions (4 functions)
- Add device/memory breakdown support
- **Deliverable:** Full feature parity for calculations

**Week 3: Integration & Testing**
- Migrate Priority 3 functions as needed (6 functions)
- Create compatibility layer for scripts
- Performance benchmarks
- **Deliverable:** Production-ready replacement

**Leave Priority 4:** Keep in pandas for now (reporting/email)

### Option B: Hybrid Approach (Faster to Production)

**Keep pandas for:**
- Reporting (HTML generation, email)
- CLI interface
- Caching layer

**Migrate to Polars:**
- Data loading (`get_time_filtered_data`, `get_multi_db_data`)
- Core calculations (allocation, time-series)
- Convert to pandas only for output

**Benefits:**
- Faster implementation (focus on performance-critical parts)
- Lower risk (keep working reporting intact)
- Get performance gains where they matter most

### Option C: API Wrapper (Quickest Win)

Create thin wrapper functions:
```python
def calculate_allocation_usage(df: pl.DataFrame, host: str = "") -> dict:
    # Use Polars for calculations
    # Convert result to dict (no pandas needed)
    return results
```

**Benefits:**
- Minimal code changes
- Performance gains on calculations
- Keep existing interfaces
- Easy to test/validate

## Recommended Next Steps for This Session

Given context limits and file size, I recommend:

1. **Create `usage_stats_polars.py`** with 5 core functions:
   - `get_preprocessed_dataframe()` - Polars preprocessing
   - `get_time_filtered_data()` - Load from SQLite
   - `get_multi_db_data()` - Load multiple DBs
   - `calculate_allocation_usage()` - Basic allocation
   - `calculate_time_series_usage()` - Time series

2. **Document migration patterns** for remaining functions

3. **Update task status** to reflect partial Phase 3

4. **Create comparison benchmarks** for the 5 migrated functions

This gives immediate performance wins on the hottest code paths while keeping scope manageable.

## Performance Impact Estimate

### High Impact (Must Migrate)
- `get_multi_db_data()` - **Loads large datasets (10-100x faster with Polars)**
- `calculate_allocation_usage()` - **Called frequently (5-10x faster)**
- `calculate_time_series_usage()` - **GroupBy heavy (5-15x faster)**

### Medium Impact
- `calculate_allocation_usage_by_device()` - GroupBy operations
- `calculate_performance_usage()` - Filtering and aggregation

### Low Impact
- Reporting functions (already fast, mostly string operations)
- Email functions (network bound, not CPU bound)

## Decision

**Proceed with Option B (Hybrid) + 5 core functions**

This balances:
- ✅ Immediate performance gains
- ✅ Manageable scope
- ✅ Lower risk
- ✅ Easy to validate
