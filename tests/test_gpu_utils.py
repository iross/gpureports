#!/usr/bin/env python3
"""
Unit tests for GPU Utils Module

Tests the machine classification functionality and related utilities.
"""

import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import patch, mock_open

# Import the functions we want to test
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gpu_utils import (
    load_hosted_capacity_hosts,
    classify_machine_category,
    filter_df_by_machine_category,
    get_machines_by_category
)


class TestLoadHostedCapacityHosts:
    """Test the hosted capacity hosts loading functionality."""
    
    def test_load_hosted_capacity_hosts_valid_file(self):
        """Test loading hosted capacity hosts from a valid file."""
        test_content = "host1.example.com\nhost2.example.com\nhost3.example.com\n"
        
        with patch('builtins.open', mock_open(read_data=test_content)):
            with patch('pathlib.Path.exists', return_value=True):
                hosts = load_hosted_capacity_hosts("test_file")
                
        expected = {"host1.example.com", "host2.example.com", "host3.example.com"}
        assert hosts == expected
    
    def test_load_hosted_capacity_hosts_file_not_found(self):
        """Test handling when hosted capacity file doesn't exist."""
        with patch('pathlib.Path.exists', return_value=False):
            hosts = load_hosted_capacity_hosts("nonexistent_file")
        
        assert hosts == set()
    
    def test_load_hosted_capacity_hosts_empty_lines(self):
        """Test that empty lines are skipped."""
        test_content = "host1.example.com\n\nhost2.example.com\n\n"
        
        with patch('builtins.open', mock_open(read_data=test_content)):
            with patch('pathlib.Path.exists', return_value=True):
                hosts = load_hosted_capacity_hosts("test_file")
                
        expected = {"host1.example.com", "host2.example.com"}
        assert hosts == expected


class TestClassifyMachineCategory:
    """Test the machine classification functionality."""
    
    def setUp(self):
        """Reset the global cache before each test."""
        from gpu_utils import _HOSTED_CAPACITY_HOSTS
        _HOSTED_CAPACITY_HOSTS = None
    
    def test_classify_hosted_capacity(self):
        """Test classification of hosted capacity machines."""
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com", "hosted2.com"}):
            category = classify_machine_category("hosted1.com", "some_project")
            assert category == "Hosted Capacity"
    
    def test_classify_researcher_owned(self):
        """Test classification of researcher owned machines."""
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com"}):
            category = classify_machine_category("research1.com", "project_alpha")
            assert category == "Researcher Owned"
    
    def test_classify_researcher_owned_whitespace(self):
        """Test classification with whitespace in prioritized projects."""
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com"}):
            category = classify_machine_category("research1.com", "  project_beta  ")
            assert category == "Researcher Owned"
    
    def test_classify_open_capacity_empty_projects(self):
        """Test classification of open capacity machines with empty projects."""
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com"}):
            category = classify_machine_category("open1.com", "")
            assert category == "Open Capacity"
    
    def test_classify_open_capacity_none_projects(self):
        """Test classification of open capacity machines with None projects."""
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com"}):
            category = classify_machine_category("open1.com", None)
            assert category == "Open Capacity"


class TestFilterDfByMachineCategory:
    """Test the DataFrame filtering by machine category."""
    
    def setUp(self):
        """Set up test data."""
        self.test_df = pd.DataFrame({
            'Machine': ['hosted1.com', 'research1.com', 'open1.com', 'research2.com'],
            'PrioritizedProjects': ['', 'project_alpha', '', 'project_beta'],
            'State': ['Claimed', 'Claimed', 'Unclaimed', 'Claimed'],
            'Name': ['slot1', 'slot2', 'slot3', 'slot4']
        })
    
    def test_filter_hosted_capacity(self):
        """Test filtering for hosted capacity machines."""
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com"}):
            result = filter_df_by_machine_category(self.test_df, "Hosted Capacity")
            
        assert len(result) == 1
        assert result.iloc[0]['Machine'] == 'hosted1.com'
    
    def test_filter_researcher_owned(self):
        """Test filtering for researcher owned machines."""
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com"}):
            result = filter_df_by_machine_category(self.test_df, "Researcher Owned")
            
        assert len(result) == 2
        expected_machines = {'research1.com', 'research2.com'}
        result_machines = set(result['Machine'].tolist())
        assert result_machines == expected_machines
    
    def test_filter_open_capacity(self):
        """Test filtering for open capacity machines."""
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com"}):
            result = filter_df_by_machine_category(self.test_df, "Open Capacity")
            
        assert len(result) == 1
        assert result.iloc[0]['Machine'] == 'open1.com'


class TestGetMachinesByCategory:
    """Test the get machines by category functionality."""
    
    def test_get_machines_by_category(self):
        """Test getting machines organized by category."""
        test_df = pd.DataFrame({
            'Machine': ['hosted1.com', 'research1.com', 'open1.com', 'research2.com', 'hosted1.com'],
            'PrioritizedProjects': ['', 'project_alpha', '', 'project_beta', ''],
        })
        
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value={"hosted1.com"}):
            result = get_machines_by_category(test_df)
        
        expected = {
            "Hosted Capacity": ["hosted1.com"],
            "Researcher Owned": ["research1.com", "research2.com"],
            "Open Capacity": ["open1.com"]
        }
        
        assert result == expected
    
    def test_get_machines_by_category_sorted(self):
        """Test that machine lists are sorted."""
        test_df = pd.DataFrame({
            'Machine': ['z-research.com', 'a-research.com', 'm-research.com'],
            'PrioritizedProjects': ['project1', 'project2', 'project3'],
        })
        
        with patch('gpu_utils.load_hosted_capacity_hosts', return_value=set()):
            result = get_machines_by_category(test_df)
        
        expected_researcher_owned = ["a-research.com", "m-research.com", "z-research.com"]
        assert result["Researcher Owned"] == expected_researcher_owned


if __name__ == '__main__':
    pytest.main([__file__])