#!/usr/bin/env python3
"""
Unit tests for GPU Usage Statistics Plotter

Tests the plotting functionality of plot_usage_stats.py including
plot generation and data visualization functions.
"""

import pytest
import pandas as pd
import matplotlib.pyplot as plt
import tempfile
import os
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts'))

# Import the functions we want to test
from plot_usage_stats import (
    create_usage_timeline_plot,
    create_gpu_count_plot,
    create_device_usage_heatmap,
    create_utilization_distribution_plot,
    create_summary_dashboard
)


@pytest.fixture
def sample_timeseries_data():
    """Create sample time series data for testing."""
    data = []
    timestamps = pd.date_range('2025-01-01 10:00:00', periods=8, freq='15min')

    for i, ts in enumerate(timestamps):
        data.append({
            'timestamp': ts,
            'priority_claimed': 10 + i,
            'priority_total': 20 + i,
            'priority_usage_percent': ((10 + i) / (20 + i)) * 100,
            'shared_claimed': 5 + i,
            'shared_total': 15 + i,
            'shared_usage_percent': ((5 + i) / (15 + i)) * 100,
            'backfill_claimed': 2 + i,
            'backfill_total': 8 + i,
            'backfill_usage_percent': ((2 + i) / (8 + i)) * 100
        })

    return pd.DataFrame(data)


@pytest.fixture
def sample_raw_gpu_data():
    """Create sample raw GPU data for testing."""
    data = []
    timestamps = pd.date_range('2025-01-01 10:00:00', periods=4, freq='15min')

    for ts in timestamps:
        # Priority slots
        data.append({
            'Name': 'slot1@host1.domain.com',
            'AssignedGPUs': 'GPU-001',
            'State': 'Claimed',
            'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
            'PrioritizedProjects': 'project1',
            'timestamp': ts
        })
        data.append({
            'Name': 'slot2@host1.domain.com',
            'AssignedGPUs': 'GPU-002',
            'State': 'Unclaimed',
            'GPUs_DeviceName': 'Tesla A100-SXM4-40GB',
            'PrioritizedProjects': 'project1',
            'timestamp': ts
        })

        # Shared slots
        data.append({
            'Name': 'slot3@host2.domain.com',
            'AssignedGPUs': 'GPU-003',
            'State': 'Claimed',
            'GPUs_DeviceName': 'Tesla A100-SXM4-40GB',
            'PrioritizedProjects': '',
            'timestamp': ts
        })

        # Backfill slots
        data.append({
            'Name': 'slot1_backfill@host1.domain.com',
            'AssignedGPUs': 'GPU-004',
            'State': 'Claimed',
            'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
            'PrioritizedProjects': '',
            'timestamp': ts
        })

    return pd.DataFrame(data)


class TestPlotCreation:
    """Test the plot creation functions."""

    def test_create_usage_timeline_plot(self, sample_timeseries_data):
        """Test timeline plot creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            save_path = Path(tmpdir) / "timeline_test.png"

            fig, ax = create_usage_timeline_plot(
                sample_timeseries_data,
                "Test Timeline",
                str(save_path)
            )

            # Check that plot was created
            assert fig is not None
            assert ax is not None
            assert save_path.exists()

            # Check plot properties
            assert ax.get_title() == "Test Timeline"
            assert ax.get_ylabel() == "Usage Percentage (%)"
            assert ax.get_ylim() == (0, 105)

            # Check that lines were plotted
            lines = ax.get_lines()
            assert len(lines) == 3  # Priority, Shared, Backfill

            plt.close(fig)

    def test_create_gpu_count_plot(self, sample_timeseries_data):
        """Test GPU count plot creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            save_path = Path(tmpdir) / "counts_test.png"

            fig, axes = create_gpu_count_plot(
                sample_timeseries_data,
                "Test GPU Counts",
                str(save_path)
            )

            # Check that plot was created
            assert fig is not None
            assert len(axes) == 3  # Three subplots
            assert save_path.exists()

            # Check subplot properties
            for i, ax in enumerate(axes):
                expected_titles = ['Priority GPU Usage', 'Shared GPU Usage', 'Backfill GPU Usage']
                assert ax.get_title() == expected_titles[i]
                assert 'GPU Count' in ax.get_ylabel()

            plt.close(fig)

    def test_create_device_usage_heatmap(self, sample_raw_gpu_data):
        """Test device usage heatmap creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            save_path = Path(tmpdir) / "heatmap_test.png"

            fig, ax = create_device_usage_heatmap(
                sample_raw_gpu_data,
                "Test Heatmap",
                str(save_path)
            )

            # Check that plot was created
            assert fig is not None
            assert ax is not None
            assert save_path.exists()

            # Check plot properties
            assert ax.get_title() == "Test Heatmap"
            assert ax.get_xlabel() == "GPU Class"
            assert ax.get_ylabel() == "Device Type"

            plt.close(fig)

    def test_create_utilization_distribution_plot(self, sample_timeseries_data):
        """Test utilization distribution plot creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            save_path = Path(tmpdir) / "distribution_test.png"

            fig, ax = create_utilization_distribution_plot(
                sample_timeseries_data,
                "Test Distribution",
                str(save_path)
            )

            # Check that plot was created
            assert fig is not None
            assert ax is not None
            assert save_path.exists()

            # Check plot properties
            assert ax.get_title() == "Test Distribution"
            assert ax.get_ylabel() == "Usage Percentage (%)"
            assert ax.get_ylim() == (0, 105)

            plt.close(fig)

    def test_create_summary_dashboard(self, sample_raw_gpu_data, sample_timeseries_data):
        """Test summary dashboard creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            save_path = Path(tmpdir) / "dashboard_test.png"

            fig = create_summary_dashboard(
                sample_raw_gpu_data,
                sample_timeseries_data,
                "Test Period",
                str(save_path)
            )

            # Check that plot was created
            assert fig is not None
            assert save_path.exists()

            # Check that multiple subplots were created
            axes = fig.get_axes()
            assert len(axes) >= 6  # Should have multiple subplots

            plt.close(fig)


class TestPlotDataHandling:
    """Test data handling in plot functions."""

    def test_empty_timeseries_data(self):
        """Test handling of empty time series data."""
        empty_df = pd.DataFrame(columns=['timestamp', 'priority_usage_percent',
                                        'shared_usage_percent', 'backfill_usage_percent'])

        fig, ax = create_usage_timeline_plot(empty_df, "Empty Data Test")

        # Should still create a plot, but with no data
        assert fig is not None
        assert ax is not None

        # Should have 3 lines (priority, shared, backfill) but with no data points
        lines = ax.get_lines()
        assert len(lines) == 3
        # Each line should have 0 data points
        for line in lines:
            assert len(line.get_xdata()) == 0

        plt.close(fig)

    def test_missing_columns(self, sample_timeseries_data):
        """Test handling of missing columns in time series data."""
        # Remove some columns
        incomplete_df = sample_timeseries_data.drop(columns=['shared_usage_percent', 'shared_claimed', 'shared_total'])

        fig, ax = create_usage_timeline_plot(incomplete_df, "Missing Columns Test")

        # Should still create a plot with available data
        assert fig is not None
        assert ax is not None

        # Should have fewer lines plotted
        lines = ax.get_lines()
        assert len(lines) == 2  # Only Priority and Backfill

        plt.close(fig)

    def test_device_heatmap_no_data(self):
        """Test device heatmap with no device data."""
        empty_df = pd.DataFrame(columns=['Name', 'AssignedGPUs', 'State',
                                        'GPUs_DeviceName', 'PrioritizedProjects', 'timestamp'])

        # Mock the calculate_allocation_usage_by_device function to return empty data
        with patch('plot_usage_stats.calculate_allocation_usage_by_device') as mock_calc:
            mock_calc.return_value = {'Priority': {}, 'Shared': {}, 'Backfill': {}}

            result = create_device_usage_heatmap(empty_df, "No Data Test")

            # Should return None when no data available
            assert result == (None, None)


class TestPlotSaving:
    """Test plot saving functionality."""

    def test_plot_saving_without_path(self, sample_timeseries_data):
        """Test plot creation without saving."""
        fig, ax = create_usage_timeline_plot(sample_timeseries_data, "No Save Test")

        # Should create plot without saving
        assert fig is not None
        assert ax is not None

        plt.close(fig)

    def test_plot_saving_with_invalid_path(self, sample_timeseries_data):
        """Test plot saving with invalid path."""
        # Try to save to a non-existent directory
        invalid_path = "/nonexistent/directory/test.png"

        # Should raise an exception when trying to save to invalid path
        with pytest.raises((FileNotFoundError, OSError)):
            fig, ax = create_usage_timeline_plot(sample_timeseries_data, "Invalid Path Test", invalid_path)


class TestPlotFormatting:
    """Test plot formatting and styling."""

    def test_plot_titles_and_labels(self, sample_timeseries_data):
        """Test that plots have proper titles and labels."""
        custom_title = "Custom Test Title"
        fig, ax = create_usage_timeline_plot(sample_timeseries_data, custom_title)

        assert ax.get_title() == custom_title
        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Usage Percentage (%)"

        # Check legend
        legend = ax.get_legend()
        assert legend is not None

        plt.close(fig)

    def test_plot_grid_and_limits(self, sample_timeseries_data):
        """Test that plots have proper grid and axis limits."""
        fig, ax = create_usage_timeline_plot(sample_timeseries_data, "Grid Test")

        # Check y-axis limits
        assert ax.get_ylim() == (0, 105)

        # Check that grid is enabled
        assert ax.grid

        plt.close(fig)

    def test_color_consistency(self, sample_timeseries_data):
        """Test that plots use consistent colors across different plot types."""
        # Test timeline plot colors
        fig1, ax1 = create_usage_timeline_plot(sample_timeseries_data, "Color Test")
        lines = ax1.get_lines()

        # Should have consistent color scheme
        if len(lines) >= 3:
            # Check that different lines have different colors
            colors = [line.get_color() for line in lines]
            assert len(set(colors)) == len(colors)  # All colors should be unique

        plt.close(fig1)


class TestErrorHandling:
    """Test error handling in plot functions."""

    def test_invalid_dataframe_structure(self):
        """Test handling of DataFrames with wrong structure."""
        # Create DataFrame with wrong column names
        wrong_df = pd.DataFrame({
            'wrong_timestamp': pd.date_range('2025-01-01', periods=5, freq='15min'),
            'wrong_data': [1, 2, 3, 4, 5]
        })

        # Should handle gracefully
        fig, ax = create_usage_timeline_plot(wrong_df, "Wrong Structure Test")

        assert fig is not None
        assert ax is not None

        # Should have no lines plotted due to missing columns
        lines = ax.get_lines()
        assert len(lines) == 0

        plt.close(fig)

    def test_data_with_nan_values(self, sample_timeseries_data):
        """Test handling of data with NaN values."""
        # Introduce some NaN values
        nan_df = sample_timeseries_data.copy()
        nan_df.loc[2:4, 'priority_usage_percent'] = float('nan')

        fig, ax = create_usage_timeline_plot(nan_df, "NaN Test")

        # Should still create plot
        assert fig is not None
        assert ax is not None

        # Should still have lines (matplotlib handles NaN values)
        lines = ax.get_lines()
        assert len(lines) == 3

        plt.close(fig)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
