#!/usr/bin/env python3
"""
Unit tests for read_data.py's host/CHTC-owned config loading.
"""

import os

# Import the functions we want to test
import sys
from unittest.mock import mock_open, patch

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from read_data import load_chtc_owned_hosts  # noqa: E402


class TestLoadCHTCOwnedHosts:
    """Test the CHTC owned hosts loading functionality."""

    def setup_method(self):
        """Clear the global cache before each test."""
        import read_data

        read_data._CHTC_OWNED_HOSTS = None

    def test_load_chtc_owned_hosts_valid_file(self):
        """Test loading CHTC owned hosts from a valid file."""
        test_content = "host1.example.com\nhost2.example.com\nhost3.example.com\n"

        with patch("builtins.open", mock_open(read_data=test_content)):
            with patch("pathlib.Path.exists", return_value=True):
                hosts = load_chtc_owned_hosts("test_file")

        expected = {"host1.example.com", "host2.example.com", "host3.example.com"}
        assert hosts == expected

    def test_load_chtc_owned_hosts_file_not_found(self):
        """Test handling when CHTC owned file doesn't exist."""
        with patch("pathlib.Path.exists", return_value=False):
            hosts = load_chtc_owned_hosts("nonexistent_file")

        assert hosts == set()

    def test_load_chtc_owned_hosts_empty_lines(self):
        """Test that empty lines are skipped."""
        test_content = "host1.example.com\n\nhost2.example.com\n\n"

        with patch("builtins.open", mock_open(read_data=test_content)):
            with patch("pathlib.Path.exists", return_value=True):
                hosts = load_chtc_owned_hosts("test_file")

        expected = {"host1.example.com", "host2.example.com"}
        assert hosts == expected


if __name__ == "__main__":
    pytest.main([__file__])
