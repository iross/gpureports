# Polars Migration API Mapping

This document maps pandas operations used in the GPU health monitoring codebase to their Polars equivalents.

## Overview

The codebase uses pandas extensively for data manipulation, filtering, and analysis. This document provides a comprehensive mapping of pandas operations to Polars to facilitate the migration.

## Core Operations

### DataFrame Creation and Basic Operations

| Pandas | Polars | Notes |
|--------|--------|-------|
| `pd.DataFrame(data)` | `pl.DataFrame(data)` | Direct equivalent |
| `df.copy()` | `df.clone()` | Polars uses `clone()` instead of `copy()` |
| `df.shape` | `df.shape` | Same API |
| `df.columns` | `df.columns` | Same API |
| `len(df)` | `len(df)` or `df.height` | Polars also provides `.height` property |

### Data Type Operations

| Pandas | Polars | Notes |
|--------|--------|-------|
| `pd.to_datetime(series)` | `pl.col().str.strptime(pl.Datetime, format)` | For string timestamps, use strptime |
| `pd.to_datetime(series)` | `pl.col().cast(pl.Datetime)` | For already-datetime types |
| `pd.api.types.is_datetime64_any_dtype()` | `df.schema[col] == pl.Datetime` | Check column type in schema |
| `pd.isna()` | `pl.col().is_null()` | Polars uses null instead of NA |
| `.isna()` | `.is_null()` | Method on expressions |
| `.notna()` | `.is_not_null()` | Method on expressions |

**IMPORTANT:** When converting string timestamps to datetime in Polars:
```python
# If timestamp is a string column (pl.Utf8)
df = df.with_columns(
    pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f")
)

# If timestamp is already a compatible type
df = df.with_columns(
    pl.col("timestamp").cast(pl.Datetime)
)

# Safe approach - check schema first
if df.schema["timestamp"] == pl.Utf8:
    df = df.with_columns(pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f"))
else:
    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime))
```

### Filtering and Boolean Indexing

| Pandas | Polars | Notes |
|--------|--------|-------|
| `df[condition]` | `df.filter(condition)` | Polars uses explicit `.filter()` method |
| `df[df["col"] == value]` | `df.filter(pl.col("col") == value)` | Must use `pl.col()` in expressions |
| `df[~condition]` | `df.filter(~condition)` | Negation works the same |
| `df[(cond1) & (cond2)]` | `df.filter(cond1 & cond2)` | Logical AND |
| `df[(cond1) \| (cond2)]` | `df.filter(cond1 \| cond2)` | Logical OR |
| `.isin(values)` | `.is_in(values)` | Method name change |

### String Operations

| Pandas | Polars | Notes |
|--------|--------|-------|
| `.str.contains(pattern)` | `.str.contains(pattern)` | Similar API |
| `.str.contains(pat, case=False)` | `.str.contains(f"(?i){pat}")` | Use regex for case-insensitive |
| `.str.contains(pat, na=False)` | `.str.contains(pat).fill_null(False)` | Handle nulls explicitly |

### Sorting and Deduplication

| Pandas | Polars | Notes |
|--------|--------|-------|
| `.sort_values(col)` | `.sort(col)` | Simpler method name |
| `.sort_values([col1, col2])` | `.sort([col1, col2])` | Multiple columns |
| `.sort_values(col, ascending=False)` | `.sort(col, descending=True)` | Parameter name change |
| `.drop_duplicates()` | `.unique()` | Method name change |
| `.drop_duplicates(subset=[cols])` | `.unique(subset=[cols])` | Subset parameter same |
| `.drop_duplicates(keep="first")` | `.unique(keep="first")` | Keep parameter same |
| `.duplicated()` | `.is_duplicated()` | Method name change |
| `.duplicated(keep=False)` | `.is_duplicated()` | Polars default is keep=False |

### Aggregation and Grouping

| Pandas | Polars | Notes |
|--------|--------|-------|
| `.groupby(col)` | `.group_by(col)` | Method name change (underscore) |
| `.groupby([cols])` | `.group_by([cols])` | Multiple columns |
| `.agg(func)` | `.agg(func)` | Same method |
| `.nunique()` | `.n_unique()` | Method name change |
| `.unique()` | `.unique()` | Same API |
| `.first()` | `.first()` | Same API |
| `.reset_index()` | No direct equivalent | Polars doesn't have index concept |

### Column Operations

| Pandas | Polars | Notes |
|--------|--------|-------|
| `df["col"]` | `df["col"]` or `df.select("col")` | Selection works similarly |
| `df[["col1", "col2"]]` | `df.select(["col1", "col2"])` | Multiple columns |
| `df.drop(columns=[cols])` | `df.drop(cols)` | No need for `columns=` parameter |
| `df["new_col"] = values` | `df.with_columns(pl.Series("new_col", values))` | Use `with_columns()` for assignment |

### Datetime Operations

| Pandas | Polars | Notes |
|--------|--------|-------|
| `.dt.floor("15min")` | `.dt.truncate("15m")` | Method and format string differ |
| `.dt.date` | `.dt.date()` | Polars uses method call |
| `.dt.hour` | `.dt.hour()` | Polars uses method call |
| `.dt.strftime()` | `.dt.strftime()` | Same API |

### Combining DataFrames

| Pandas | Polars | Notes |
|--------|--------|-------|
| `pd.concat([df1, df2])` | `pl.concat([df1, df2])` | Similar API |
| `pd.concat(..., ignore_index=True)` | `pl.concat(...)` | Polars has no index |
| `df.merge(other)` | `df.join(other)` | Method name change |
| `.merge(on=col)` | `.join(on=col, how="inner")` | Must specify `how` parameter |

### Data Access

| Pandas | Polars | Notes |
|--------|--------|-------|
| `.iloc[0]` | `.row(0)` or `[0]` | Polars uses `.row()` for row access |
| `.iloc[0]["col"]` | `[0, "col"]` | Direct tuple indexing |
| `.loc[condition]` | `.filter(condition)` | Use filter instead of loc |

### SQL Integration

| Pandas | Polars | Notes |
|--------|--------|-------|
| `pd.read_sql_query(query, conn)` | `pl.read_database(query, conn)` | Different method name |
| `pd.read_sql_query(..., params=[])` | Use parameterized query in SQL | Handle params in query string |

## Common Patterns in This Codebase

### Pattern 1: String Timestamp to Datetime

```python
# Pandas
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Polars (when timestamp is string)
df = df.with_columns(
    pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f")
)

# Polars (when timestamp is already datetime-like)
df = df.with_columns(pl.col("timestamp").cast(pl.Datetime))
```

### Pattern 2: Complex Boolean Filtering

```python
# Pandas
df = df[
    (df["State"] == "Claimed")
    & (df["PrioritizedProjects"] != "")
    & (~df["Name"].str.contains("backfill", case=False, na=False))
]

# Polars
df = df.filter(
    (pl.col("State") == "Claimed")
    & (pl.col("PrioritizedProjects") != "")
    & (~pl.col("Name").str.contains("(?i)backfill").fill_null(False))
)
```

### Pattern 3: Duplicate Detection with Ranking

```python
# Pandas
df["_rank"] = 0
df.loc[condition, "_rank"] = 1
df = df.sort_values(["AssignedGPUs", "_rank"], ascending=[True, False])
df = df.drop_duplicates(subset=["timestamp", "AssignedGPUs"], keep="first")
df = df.drop(columns=["_rank"])

# Polars
df = df.with_columns(
    pl.when(condition).then(1).otherwise(0).alias("_rank")
)
df = df.sort(["AssignedGPUs", "_rank"], descending=[False, True])
df = df.unique(subset=["timestamp", "AssignedGPUs"], keep="first")
df = df.drop("_rank")
```

### Pattern 4: GroupBy with Date Extraction

```python
# Pandas
df["date"] = df["timestamp"].dt.date
result = df.groupby("date")["AssignedGPUs"].nunique()

# Polars
df = df.with_columns(pl.col("timestamp").dt.date().alias("date"))
result = df.group_by("date").agg(pl.col("AssignedGPUs").n_unique())
```

## Key Differences and Gotchas

### 1. String Datetime Conversion
- **Issue:** `.cast(pl.Datetime)` fails on string timestamps
- **Solution:** Use `.str.strptime(pl.Datetime, format)` for strings
- **Best Practice:** Check schema type before conversion

### 2. Lazy vs Eager Evaluation
- **Pandas:** Eager evaluation (operations execute immediately)
- **Polars:** Supports both lazy (`scan_*`) and eager (`read_*`) APIs
- **Impact:** For this migration, we'll use eager API to maintain similar behavior

### 3. Index Concept
- **Pandas:** Has row index concept
- **Polars:** No index concept, uses row numbers
- **Impact:** Operations like `.reset_index()` don't exist

### 4. Method Chaining
- **Polars:** Heavily optimized for method chaining
- **Best Practice:** Chain operations when possible for better performance

### 5. Column Selection
- **Polars:** Uses `pl.col()` expressions for column operations
- **Best Practice:** Use `pl.col()` consistently in filter and select operations

### 6. Null Handling
- **Pandas:** Uses `NaN` for missing values
- **Polars:** Uses `null` for missing values
- **Impact:** Different behavior in comparisons and aggregations

### 7. String Operations Case Sensitivity
- **Pandas:** Has explicit `case=` parameter
- **Polars:** Use regex with `(?i)` flag for case-insensitive matching

### 8. Copy Semantics
- **Pandas:** Often creates views; requires explicit `.copy()`
- **Polars:** Copy-on-write semantics; uses `.clone()` when needed

### 9. Datetime Formatting
- **Pandas:** Uses "15min" for 15-minute periods
- **Polars:** Uses "15m" for 15-minute periods

## Migration Checklist

When migrating a function:

- [ ] Replace `df.copy()` with `df.clone()`
- [ ] Replace `.groupby()` with `.group_by()`
- [ ] Replace `.sort_values()` with `.sort()`
- [ ] Replace boolean indexing `df[condition]` with `df.filter(condition)`
- [ ] Replace string contains with regex for case-insensitive
- [ ] Check datetime column type before conversion
- [ ] Use `.str.strptime()` for string timestamps
- [ ] Replace `.dt.floor()` with `.dt.truncate()`
- [ ] Change datetime format strings ("15min" → "15m")
- [ ] Replace `.nunique()` with `.n_unique()`
- [ ] Handle nulls explicitly with `.fill_null()`
- [ ] Test with empty DataFrames
- [ ] Test with null values
- [ ] Verify output matches pandas version

## References

- [Polars User Guide](https://pola-rs.github.io/polars-book/)
- [Polars API Reference](https://pola-rs.github.io/polars/py-polars/html/reference/)
- [Pandas to Polars Migration Guide](https://pola-rs.github.io/polars-book/user-guide/migration/pandas/)
