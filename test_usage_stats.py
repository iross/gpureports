#!/usr/bin/env python3
"""
Unit tests for GPU Usage Statistics Calculator

Tests the core functionality of usage_stats.py including filtering,
calculations, and data processing functions.
"""

import pytest
import pandas as pd
import datetime
import sqlite3
import tempfile
import os
from unittest.mock import patch, Mock
from pathlib import Path

# Import the functions we want to test
from usage_stats import (
    filter_df,
    calculate_allocation_usage,
    calculate_time_series_usage,
    calculate_allocation_usage_by_device,
    get_time_filtered_data,
    run_analysis
)


@pytest.fixture
def sample_gpu_data():
    """Create sample GPU data for testing."""
    data = [
        # Priority slots
        {
            'Name': 'slot1@host1.domain.com',
            'AssignedGPUs': 'GPU-001',
            'State': 'Claimed',
            'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
            'PrioritizedProjects': 'project1,project2',
            'GPUsAverageUsage': 0.85,
            'timestamp': pd.Timestamp('2025-01-01 10:00:00')
        },
        {
            'Name': 'slot2@host1.domain.com',
            'AssignedGPUs': 'GPU-002',
            'State': 'Unclaimed',
            'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
            'PrioritizedProjects': 'project1,project2',
            'GPUsAverageUsage': None,
            'timestamp': pd.Timestamp('2025-01-01 10:00:00')
        },
        # Shared slots
        {
            'Name': 'slot3@host2.domain.com',
            'AssignedGPUs': 'GPU-003',
            'State': 'Claimed',
            'GPUs_DeviceName': 'Tesla A100-SXM4-40GB',
            'PrioritizedProjects': '',
            'GPUsAverageUsage': 0.65,
            'timestamp': pd.Timestamp('2025-01-01 10:00:00')
        },
        {
            'Name': 'slot4@host2.domain.com',
            'AssignedGPUs': 'GPU-004',
            'State': 'Unclaimed',
            'GPUs_DeviceName': 'Tesla A100-SXM4-40GB',
            'PrioritizedProjects': '',
            'GPUsAverageUsage': None,
            'timestamp': pd.Timestamp('2025-01-01 10:00:00')
        },
        # Backfill slots
        {
            'Name': 'slot1_backfill@host1.domain.com',
            'AssignedGPUs': 'GPU-005',
            'State': 'Claimed',
            'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
            'PrioritizedProjects': '',
            'GPUsAverageUsage': 0.45,
            'timestamp': pd.Timestamp('2025-01-01 10:00:00')
        },
        {
            'Name': 'slot2_backfill@host1.domain.com',
            'AssignedGPUs': 'GPU-006',
            'State': 'Unclaimed',
            'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
            'PrioritizedProjects': '',
            'GPUsAverageUsage': None,
            'timestamp': pd.Timestamp('2025-01-01 10:00:00')
        },
        # Data for time series testing (15 minutes later)
        {
            'Name': 'slot1@host1.domain.com',
            'AssignedGPUs': 'GPU-001',
            'State': 'Unclaimed',
            'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
            'PrioritizedProjects': 'project1,project2',
            'GPUsAverageUsage': None,
            'timestamp': pd.Timestamp('2025-01-01 10:15:00')
        },
        {
            'Name': 'slot3@host2.domain.com',
            'AssignedGPUs': 'GPU-003',
            'State': 'Claimed',
            'GPUs_DeviceName': 'Tesla A100-SXM4-40GB',
            'PrioritizedProjects': '',
            'GPUsAverageUsage': 0.75,
            'timestamp': pd.Timestamp('2025-01-01 10:15:00')
        }
    ]
    return pd.DataFrame(data)


@pytest.fixture
def temp_db_with_data(sample_gpu_data):
    """Create a temporary database with sample data."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    conn = sqlite3.connect(db_path)
    sample_gpu_data.to_sql('gpu_state', conn, index=False, if_exists='replace')
    conn.close()
    
    yield db_path
    
    # Cleanup
    os.unlink(db_path)


class TestFilterFunctions:
    """Test the GPU filtering functions."""
    
    def test_filter_priority_claimed(self, sample_gpu_data):
        """Test filtering for Priority/Claimed slots."""
        result = filter_df(sample_gpu_data, "Priority", "Claimed", "")
        
        assert len(result) == 1
        assert result.iloc[0]['Name'] == 'slot1@host1.domain.com'
        assert result.iloc[0]['State'] == 'Claimed'
        assert result.iloc[0]['PrioritizedProjects'] != ''
        assert 'backfill' not in result.iloc[0]['Name']
    
    def test_filter_priority_unclaimed(self, sample_gpu_data):
        """Test filtering for Priority/Unclaimed slots."""
        result = filter_df(sample_gpu_data, "Priority", "Unclaimed", "")
        
        assert len(result) == 1
        assert result.iloc[0]['Name'] == 'slot2@host1.domain.com'
        assert result.iloc[0]['State'] == 'Unclaimed'
        assert result.iloc[0]['PrioritizedProjects'] != ''
    
    def test_filter_shared_claimed(self, sample_gpu_data):
        """Test filtering for Shared/Claimed slots."""
        result = filter_df(sample_gpu_data, "Shared", "Claimed", "")
        
        # Should find shared slots across both time intervals
        assert len(result) == 2  # Two time intervals with shared claimed slots
        assert all(row['State'] == 'Claimed' for _, row in result.iterrows())
        assert all(row['PrioritizedProjects'] == '' for _, row in result.iterrows())
        assert all('backfill' not in row['Name'] for _, row in result.iterrows())
    
    def test_filter_backfill_claimed(self, sample_gpu_data):
        """Test filtering for Backfill/Claimed slots."""
        result = filter_df(sample_gpu_data, "Backfill", "Claimed", "")
        
        assert len(result) == 1
        assert result.iloc[0]['Name'] == 'slot1_backfill@host1.domain.com'
        assert result.iloc[0]['State'] == 'Claimed'
        assert 'backfill' in result.iloc[0]['Name']
    
    def test_filter_by_host(self, sample_gpu_data):
        """Test filtering by host pattern with a specific utilization type and state."""
        # Test host filtering for Priority slots with Claimed state
        result = filter_df(sample_gpu_data, "Priority", "Claimed", "host1")
        
        # Should include only claimed priority slots from host1
        assert len(result) == 1  # slot1 at 10:00 (claimed)
        assert all('host1' in name for name in result['Name'])
        assert all(row['PrioritizedProjects'] != '' for _, row in result.iterrows())
        assert all(row['State'] == 'Claimed' for _, row in result.iterrows())
        
        # Test host filtering for Backfill slots  
        backfill_result = filter_df(sample_gpu_data, "Backfill", "", "host1")
        assert len(backfill_result) == 2  # Two backfill slots from host1
        assert all('host1' in name for name in backfill_result['Name'])
        assert all('backfill' in name for name in backfill_result['Name'])
    
    def test_gpu_conflict_resolution(self):
        """Test resolution of duplicate GPU assignments."""
        # Create test data with duplicate GPU assignments where primary slot has higher priority
        conflict_data = pd.DataFrame([
            {
                'Name': 'slot1@host1.domain.com',
                'AssignedGPUs': 'GPU-001',
                'State': 'Claimed',  # Primary claimed has highest priority
                'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
                'PrioritizedProjects': 'project1',
                'timestamp': pd.Timestamp('2025-01-01 10:00:00')
            },
            {
                'Name': 'slot1_backfill@host1.domain.com',
                'AssignedGPUs': 'GPU-001',  # Same GPU
                'State': 'Claimed',
                'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
                'PrioritizedProjects': 'project1',
                'timestamp': pd.Timestamp('2025-01-01 10:00:00')
            }
        ])
        
        result = filter_df(conflict_data, "Priority", "", "")
        
        # Should keep only the primary slot (higher priority than backfill)
        assert len(result) == 1
        assert 'backfill' not in result.iloc[0]['Name']


class TestCalculationFunctions:
    """Test the usage calculation functions."""
    
    def test_calculate_allocation_usage(self, sample_gpu_data):
        """Test allocation usage calculation."""
        stats = calculate_allocation_usage(sample_gpu_data)
        
        # Check that we have stats for all three types
        assert 'Priority' in stats
        assert 'Shared' in stats
        assert 'Backfill' in stats
        
        # Priority: Interval 1: 1 claimed, 2 total; Interval 2: 0 claimed, 1 total
        priority_stats = stats['Priority']
        assert priority_stats['avg_claimed'] == 0.5  # (1+0)/2 = 0.5
        assert priority_stats['avg_total_available'] == 1.5  # (2+1)/2 = 1.5
        assert abs(priority_stats['allocation_usage_percent'] - 25.0) < 0.1  # (50+0)/2 = 25%
        
        # Shared: Interval 1: 1 claimed, 2 total; Interval 2: 1 claimed, 1 total  
        shared_stats = stats['Shared']
        assert shared_stats['avg_claimed'] == 1.0  # (1+1)/2 = 1.0
        assert shared_stats['avg_total_available'] == 1.5  # (2+1)/2 = 1.5
        assert abs(shared_stats['allocation_usage_percent'] - 75.0) < 0.1  # (50+100)/2 = 75%
    
    def test_calculate_time_series_usage(self, sample_gpu_data):
        """Test time series usage calculation."""
        ts_df = calculate_time_series_usage(sample_gpu_data, bucket_minutes=15)
        
        # Should have 2 time buckets (10:00 and 10:15)
        assert len(ts_df) == 2
        
        # Check columns exist
        expected_cols = ['timestamp', 'priority_claimed', 'priority_total', 'priority_usage_percent',
                        'shared_claimed', 'shared_total', 'shared_usage_percent',
                        'backfill_claimed', 'backfill_total', 'backfill_usage_percent']
        for col in expected_cols:
            assert col in ts_df.columns
        
        # Check first bucket (10:00)
        first_bucket = ts_df[ts_df['timestamp'] == pd.Timestamp('2025-01-01 10:00:00')]
        assert len(first_bucket) == 1
        assert first_bucket.iloc[0]['priority_claimed'] == 1
        assert first_bucket.iloc[0]['priority_total'] == 2
        assert first_bucket.iloc[0]['priority_usage_percent'] == 50.0
    
    def test_calculate_allocation_usage_by_device(self, sample_gpu_data):
        """Test device-grouped allocation usage calculation."""
        stats = calculate_allocation_usage_by_device(sample_gpu_data, include_all_devices=True)
        
        # Should have stats for both device types
        assert 'Priority' in stats
        assert 'Shared' in stats
        assert 'Backfill' in stats
        
        # Check Priority stats have both device types
        priority_stats = stats['Priority']
        assert 'Tesla V100-SXM2-32GB' in priority_stats
        
        # V100 Priority: Interval 1: 1 claimed, 2 total; Interval 2: 0 claimed, 1 total
        v100_stats = priority_stats['Tesla V100-SXM2-32GB']
        assert v100_stats['avg_claimed'] == 0.5  # (1+0)/2 = 0.5
        assert v100_stats['avg_total_available'] == 1.5  # (2+1)/2 = 1.5
        assert abs(v100_stats['allocation_usage_percent'] - 25.0) < 0.1  # (50+0)/2 = 25%
    
    def test_device_filtering(self, sample_gpu_data):
        """Test filtering out old device types."""
        # Add some old GPU types to test data
        old_gpu_data = sample_gpu_data.copy()
        old_gpu_row = old_gpu_data.iloc[0].copy()
        old_gpu_row['GPUs_DeviceName'] = 'GTX 1080 Ti'
        old_gpu_row['AssignedGPUs'] = 'GPU-999'
        old_gpu_data = pd.concat([old_gpu_data, pd.DataFrame([old_gpu_row])], ignore_index=True)
        
        # Test with filtering (default)
        stats_filtered = calculate_allocation_usage_by_device(old_gpu_data, include_all_devices=False)
        
        # Should not include GTX 1080 Ti
        for class_stats in stats_filtered.values():
            assert 'GTX 1080 Ti' not in class_stats
        
        # Test without filtering
        stats_all = calculate_allocation_usage_by_device(old_gpu_data, include_all_devices=True)
        
        # Should include GTX 1080 Ti in Priority stats
        assert 'GTX 1080 Ti' in stats_all['Priority']


class TestDatabaseFunctions:
    """Test database-related functions."""
    
    def test_get_time_filtered_data(self, temp_db_with_data):
        """Test time-filtered data retrieval."""
        # Get data from the last 1 hour (should get all data)
        df = get_time_filtered_data(temp_db_with_data, hours_back=1)
        
        assert len(df) == 8  # All rows from sample data
        assert 'timestamp' in df.columns
        assert df['timestamp'].dtype == 'datetime64[ns]'
    
    def test_get_time_filtered_data_with_end_time(self, temp_db_with_data):
        """Test time-filtered data with specific end time."""
        end_time = datetime.datetime(2025, 1, 1, 10, 10, 0)
        df = get_time_filtered_data(temp_db_with_data, hours_back=1, end_time=end_time)
        
        # Should only get data from 10:00, not 10:15
        assert len(df) == 6
        assert all(df['timestamp'] <= pd.Timestamp(end_time))
    
    def test_empty_database(self):
        """Test handling of empty database."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            conn = sqlite3.connect(db_path)
            # Create empty table
            conn.execute('''CREATE TABLE gpu_state (
                Name TEXT, AssignedGPUs TEXT, State TEXT, 
                GPUs_DeviceName TEXT, PrioritizedProjects TEXT,
                GPUsAverageUsage REAL, timestamp TEXT
            )''')
            conn.close()
            
            df = get_time_filtered_data(db_path, hours_back=1)
            assert len(df) == 0
            
        finally:
            os.unlink(db_path)


class TestIntegrationFunctions:
    """Test the higher-level integration functions."""
    
    def test_run_analysis_allocation(self, temp_db_with_data):
        """Test the run_analysis function with allocation analysis."""
        results = run_analysis(
            db_path=temp_db_with_data,
            hours_back=1,
            analysis_type="allocation"
        )
        
        assert "error" not in results
        assert "metadata" in results
        assert "allocation_stats" in results
        
        metadata = results["metadata"]
        assert metadata["num_intervals"] == 2
        assert metadata["total_records"] == 8
        
        allocation_stats = results["allocation_stats"]
        assert "Priority" in allocation_stats
        assert "Shared" in allocation_stats
        assert "Backfill" in allocation_stats
    
    def test_run_analysis_device_grouping(self, temp_db_with_data):
        """Test the run_analysis function with device grouping."""
        results = run_analysis(
            db_path=temp_db_with_data,
            hours_back=1,
            analysis_type="allocation",
            group_by_device=True,
            all_devices=True
        )
        
        assert "error" not in results
        assert "device_stats" in results
        
        device_stats = results["device_stats"]
        assert "Priority" in device_stats
        
        # Should have device breakdown for V100 cards
        priority_devices = device_stats["Priority"]
        assert "Tesla V100-SXM2-32GB" in priority_devices
    
    def test_run_analysis_timeseries(self, temp_db_with_data):
        """Test the run_analysis function with timeseries analysis."""
        results = run_analysis(
            db_path=temp_db_with_data,
            hours_back=1,
            analysis_type="timeseries",
            bucket_minutes=15
        )
        
        assert "error" not in results
        assert "timeseries_data" in results
        
        ts_data = results["timeseries_data"]
        assert len(ts_data) == 2  # Two 15-minute buckets
        assert "priority_usage_percent" in ts_data.columns
    
    def test_run_analysis_no_data(self):
        """Test run_analysis with empty database."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            conn = sqlite3.connect(db_path)
            conn.execute('''CREATE TABLE gpu_state (
                Name TEXT, AssignedGPUs TEXT, State TEXT, 
                GPUs_DeviceName TEXT, PrioritizedProjects TEXT,
                GPUsAverageUsage REAL, timestamp TEXT
            )''')
            conn.close()
            
            results = run_analysis(db_path, hours_back=1)
            assert "error" in results
            assert "No data found" in results["error"]
            
        finally:
            os.unlink(db_path)


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_dataframe(self):
        """Test functions with empty DataFrame input."""
        empty_df = pd.DataFrame(columns=['Name', 'AssignedGPUs', 'State', 'GPUs_DeviceName', 
                                        'PrioritizedProjects', 'GPUsAverageUsage', 'timestamp'])
        
        # Should not crash and return empty/zero results
        stats = calculate_allocation_usage(empty_df)
        assert all(s['avg_claimed'] == 0 for s in stats.values())
        
        ts_df = calculate_time_series_usage(empty_df)
        assert len(ts_df) == 0
    
    def test_missing_columns(self):
        """Test handling of missing required columns."""
        incomplete_df = pd.DataFrame([{
            'Name': 'slot1@host1.domain.com',
            'State': 'Claimed'
            # Missing other required columns
        }])
        
        with pytest.raises((KeyError, AttributeError)):
            filter_df(incomplete_df, "Priority", "Claimed", "")
    
    def test_invalid_utilization_values(self, sample_gpu_data):
        """Test handling of invalid GPU utilization values."""
        invalid_data = sample_gpu_data.copy()
        invalid_data.loc[0, 'GPUsAverageUsage'] = -0.1  # Invalid negative
        invalid_data.loc[1, 'GPUsAverageUsage'] = 1.5   # Invalid > 1
        
        # Should handle gracefully and not include invalid values
        stats = calculate_allocation_usage(invalid_data)
        assert isinstance(stats, dict)
    
    def test_malformed_timestamps(self):
        """Test handling of malformed timestamp data."""
        bad_data = pd.DataFrame([{
            'Name': 'slot1@host1.domain.com',
            'AssignedGPUs': 'GPU-001',
            'State': 'Claimed',
            'GPUs_DeviceName': 'Tesla V100-SXM2-32GB',
            'PrioritizedProjects': 'project1',
            'GPUsAverageUsage': 0.5,
            'timestamp': 'invalid-timestamp'
        }])
        
        with pytest.raises((ValueError, pd.errors.ParserError)):
            calculate_time_series_usage(bad_data)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])