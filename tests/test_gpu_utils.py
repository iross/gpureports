#!/usr/bin/env python3
"""
Unit tests for GPU Utils Module

Tests CHTC-owned-hosts loading and host-exclusion regex handling in
filter_df/filter_df_enhanced -- the pandas functions that remain gpu_utils.py's
job after TASK-49.1/49.4 moved everything else onto the canonical polars
pipeline (stats_calculations.prepare_frames()) or removed it as dead code.
"""

import os

# Import the functions we want to test
import sys
from unittest.mock import mock_open, patch

import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gpu_utils  # noqa: E402
from gpu_utils import filter_df, filter_df_enhanced, load_chtc_owned_hosts  # noqa: E402


class TestLoadCHTCOwnedHosts:
    """Test the CHTC owned hosts loading functionality."""

    def setup_method(self):
        """Clear the global cache before each test."""
        import gpu_utils

        gpu_utils._CHTC_OWNED_HOSTS = None

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


def _exclusion_df():
    return pd.DataFrame(
        {
            "Name": ["slot1@gpu(bad.chtc.wisc.edu", "slot1@gpu2.chtc.wisc.edu"],
            "AssignedGPUs": ["GPU-1A", "GPU-2A"],
            "State": ["Claimed", "Claimed"],
            "Machine": ["gpu(bad.chtc.wisc.edu", "gpu2.chtc.wisc.edu"],
            "PrioritizedProjects": ["", ""],
        }
    )


@pytest.fixture(autouse=True)
def _clear_host_exclusions(monkeypatch):
    monkeypatch.setattr(gpu_utils, "HOST_EXCLUSIONS", {})
    monkeypatch.setattr(gpu_utils, "FILTERED_HOSTS_INFO", [])


class TestFilterDfHostExclusionRegex:
    """filter_df/filter_df_enhanced used to loop over HOST_EXCLUSIONS running one
    str.contains() pass per host, with no re.escape() -- an excluded hostname
    containing a regex metacharacter (e.g. an unbalanced parenthesis) would crash
    the whole filter with a pandas error instead of just being excluded (see
    TASK-46; ported from tests/test_gpu_utils_polars.py in TASK-49.4 once the
    polars filter_df/filter_df_enhanced were removed as dead code)."""

    def test_metacharacter_hostname_does_not_raise(self, monkeypatch):
        monkeypatch.setattr(gpu_utils, "HOST_EXCLUSIONS", {"gpu(bad.chtc.wisc.edu": "test exclusion"})
        result = filter_df(_exclusion_df())
        assert result["Machine"].tolist() == ["gpu2.chtc.wisc.edu"]

    def test_multiple_hosts_excluded_in_one_pass(self, monkeypatch):
        monkeypatch.setattr(
            gpu_utils,
            "HOST_EXCLUSIONS",
            {"gpu(bad.chtc.wisc.edu": "test exclusion", "gpu2.chtc.wisc.edu": "test exclusion"},
        )
        result = filter_df(_exclusion_df())
        assert len(result) == 0


class TestFilterDfEnhancedHostExclusionRegex:
    def test_metacharacter_hostname_does_not_raise(self, monkeypatch):
        monkeypatch.setattr(gpu_utils, "HOST_EXCLUSIONS", {"gpu(bad.chtc.wisc.edu": "test exclusion"})
        result = filter_df_enhanced(_exclusion_df())
        assert "gpu(bad.chtc.wisc.edu" not in result["Machine"].tolist()


if __name__ == "__main__":
    pytest.main([__file__])
