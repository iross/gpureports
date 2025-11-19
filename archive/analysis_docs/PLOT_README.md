# GPU Usage Statistics Plotter

This module provides comprehensive visualization capabilities for GPU usage statistics, creating various plots and charts to analyze GPU utilization patterns over time.

## Overview

The `plot_usage_stats.py` script builds on the `usage_stats.py` functionality to create visual representations of GPU usage data in 15-minute intervals. It provides multiple plot types to analyze different aspects of GPU utilization.

## Features

### Plot Types

1. **Timeline Plot** - Line chart showing usage percentages over time for all GPU classes
2. **GPU Count Plot** - Stacked area charts showing claimed vs unclaimed GPUs over time  
3. **Device Usage Heatmap** - Heatmap showing usage percentages by device type and GPU class
4. **Usage Distribution** - Box plots showing distribution of usage percentages
5. **Summary Dashboard** - Comprehensive multi-panel view combining multiple visualizations

### Key Capabilities

- **15-minute interval analysis** with time series visualization
- **Multiple GPU classes**: Priority, Shared, and Backfill
- **Device-specific breakdowns** by GPU model (Tesla V100, A100, etc.)
- **Flexible time ranges** (hours to days of data)
- **High-quality output** with 300 DPI PNG files
- **Interactive display** option for real-time analysis
- **Batch processing** for multiple plot types

## Usage

### Basic Usage

```bash
# Create all plot types for the last 24 hours
python plot_usage_stats.py

# Create timeline plot for last 6 hours
python plot_usage_stats.py --hours-back 6 --plot-types timeline

# Create dashboard for specific time period
python plot_usage_stats.py --hours-back 12 --plot-types dashboard --output-dir my_plots
```

### Advanced Options

```bash
# Filter by host and create all plots
python plot_usage_stats.py --host gpu2000 --plot-types all

# Custom time range with specific end time
python plot_usage_stats.py --end-time "2025-06-12 15:00:00" --hours-back 8

# Create specific plot types
python plot_usage_stats.py --plot-types "timeline,heatmap,dashboard"

# Display plots interactively (requires display)
python plot_usage_stats.py --show-plots --plot-types timeline

# Include all device types in heatmap
python plot_usage_stats.py --all-devices --plot-types heatmap
```

## Command Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--hours-back` | 24 | Number of hours to analyze |
| `--host` | "" | Host name to filter results |
| `--db-path` | "gpu_state_2025-06.db" | Path to SQLite database |
| `--bucket-minutes` | 15 | Time bucket size in minutes |
| `--end-time` | None | End time (YYYY-MM-DD HH:MM:SS) |
| `--output-dir` | "plots" | Directory to save plots |
| `--plot-types` | "all" | Plot types to create |
| `--show-plots` | False | Display plots interactively |
| `--all-devices` | False | Include all device types |

### Plot Types Options

- `all` - Create all plot types
- `timeline` - Usage timeline only
- `counts` - GPU count plots only  
- `heatmap` - Device usage heatmap only
- `distribution` - Usage distribution only
- `dashboard` - Summary dashboard only
- Multiple types: `"timeline,heatmap,dashboard"`

## Plot Descriptions

### 1. Timeline Plot (`gpu_usage_timeline.png`)

**Purpose**: Shows usage percentage trends over time for all GPU classes

**Features**:
- Line chart with markers for each GPU class
- Time on x-axis, usage percentage (0-100%) on y-axis
- Color-coded lines: Blue (Priority), Green (Shared), Red (Backfill)
- Grid and legend for easy reading

**Use Cases**:
- Identify usage patterns and trends
- Spot peak usage periods
- Compare relative utilization across GPU classes
- Monitor system load over time

### 2. GPU Count Plot (`gpu_counts_over_time.png`)

**Purpose**: Shows actual GPU counts (claimed vs unclaimed) over time

**Features**:
- Three stacked area subplots (one per GPU class)
- Claimed GPUs in darker color, unclaimed in lighter shade
- Dashed line showing total available GPUs
- Separate y-axis scaling per subplot

**Use Cases**:
- Understand absolute resource utilization
- See capacity vs demand patterns
- Identify periods of high contention
- Monitor cluster scaling needs

### 3. Device Usage Heatmap (`device_usage_heatmap.png`)

**Purpose**: Shows usage percentages broken down by GPU device type and class

**Features**:
- 2D heatmap with device types on y-axis, GPU classes on x-axis
- Color intensity represents usage percentage (0-100%)
- Numeric annotations showing exact percentages
- Automatic filtering of old/uncommon GPU types (unless `--all-devices`)

**Use Cases**:
- Compare utilization across different GPU models
- Identify underutilized hardware
- Plan hardware upgrades/replacements
- Understand device-specific usage patterns

### 4. Usage Distribution (`usage_distribution.png`)

**Purpose**: Shows statistical distribution of usage percentages

**Features**:
- Box plots showing median, quartiles, and outliers
- One box per GPU class
- Whiskers showing min/max values
- Color-coded boxes matching other plots

**Use Cases**:
- Understand usage variability
- Identify consistent vs sporadic usage patterns
- Spot outliers and anomalies
- Statistical analysis of utilization

### 5. Summary Dashboard (`gpu_usage_dashboard.png`)

**Purpose**: Comprehensive overview combining multiple visualizations

**Layout**:
- **Top row**: Timeline plot (2/3 width) + Distribution plot (1/3 width)
- **Middle row**: Three GPU count plots (one per class)
- **Bottom row**: Average usage summary bar chart

**Features**:
- Multiple coordinated views in single image
- Summary statistics and averages
- Consistent time axis across temporal plots
- Bar chart showing overall averages with exact values

**Use Cases**:
- Executive summaries and reports
- Quick comprehensive overview
- Stakeholder presentations
- System health monitoring

## Technical Details

### Dependencies

**Required**:
- `pandas` - Data manipulation
- `matplotlib` - Core plotting functionality
- `typer` - Command line interface

**Optional**:
- `seaborn` - Enhanced styling (graceful fallback to matplotlib)

### Data Processing

1. **Time Series Generation**: Uses `calculate_time_series_usage()` from `usage_stats.py`
2. **15-minute Bucketing**: Groups data into discrete time intervals
3. **Unique GPU Counting**: Counts distinct GPUs per interval, then averages
4. **Device Grouping**: Uses `calculate_allocation_usage_by_device()` for heatmap data

### Output Quality

- **High Resolution**: 300 DPI PNG files suitable for publications
- **Professional Styling**: Clean, publication-ready plots
- **Consistent Color Scheme**: Blue/Green/Red for Priority/Shared/Backfill
- **Proper Typography**: Clear labels, titles, and legends

### Error Handling

- **Missing Data**: Graceful handling of empty datasets
- **Invalid Paths**: Safe file operations with error messages
- **Column Validation**: Robust handling of missing/renamed columns
- **Memory Management**: Automatic plot cleanup to prevent memory leaks

## Integration with Analysis Pipeline

### With `usage_stats.py`

```python
from usage_stats import run_analysis
from plot_usage_stats import create_summary_dashboard

# Get analysis results
results = run_analysis("gpu_state_2025-06.db", hours_back=24)

# Create plots from results
if "timeseries_data" in results:
    fig = create_summary_dashboard(
        df_raw, results["timeseries_data"], 
        "Analysis Period", "dashboard.png"
    )
```

### Automated Reporting

```bash
#!/bin/bash
# Daily GPU utilization report
python plot_usage_stats.py --hours-back 24 --output-dir daily_reports/$(date +%Y-%m-%d)
python usage_stats.py --hours-back 24 > daily_reports/$(date +%Y-%m-%d)/summary.txt
```

## Testing

Comprehensive test suite available in `test_plot_usage_stats.py`:

```bash
# Run all plotting tests
python -m pytest test_plot_usage_stats.py -v

# Test specific functionality
python -m pytest test_plot_usage_stats.py::TestPlotCreation -v
```

**Test Coverage**:
- Plot creation and rendering
- Data handling edge cases
- File saving operations
- Error handling scenarios
- Plot formatting validation

## Performance Considerations

- **Large Datasets**: Efficient pandas operations for large time series
- **Memory Usage**: Automatic plot cleanup prevents memory accumulation
- **File I/O**: Batch processing minimizes disk operations
- **Rendering**: Vector graphics with bitmap output for best quality/size balance

## Customization

### Adding New Plot Types

1. Create new plotting function following existing patterns
2. Add to main() function with appropriate command line option
3. Add tests to test suite
4. Update documentation

### Styling Modifications

- Modify color schemes in plot creation functions
- Adjust figure sizes and DPI settings
- Customize fonts and styling via matplotlib rcParams

### Output Formats

Currently supports PNG. Easy to extend to other formats:

```python
# In plot creation functions, change:
plt.savefig(save_path, dpi=300, bbox_inches='tight', format='pdf')
```

## Common Use Cases

### System Monitoring
- Daily/weekly utilization reports
- Capacity planning analysis
- Performance trend monitoring

### Research Analysis
- Workload characterization studies
- Resource allocation optimization
- Scheduling algorithm evaluation

### Operations
- Troubleshooting utilization issues
- Hardware planning decisions
- User education about peak times

## Troubleshooting

### Common Issues

1. **Empty plots**: Check database has data for specified time range
2. **Missing dependencies**: Install matplotlib/pandas if not available
3. **File permissions**: Ensure write access to output directory
4. **Memory errors**: Reduce time range for very large datasets

### Debug Mode

```bash
# Add verbose output for debugging
python plot_usage_stats.py --hours-back 1 --plot-types timeline --show-plots
```

This will display plots interactively and show any matplotlib warnings or errors.