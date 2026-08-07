#!/usr/bin/env python3
"""
Unit tests for classify_slots.py's pandas classification helpers.

Tests host-exclusion regex handling in filter_df/filter_df_enhanced -- the
pandas functions folded in from the former gpu_utils.py (see TASK-49.1/49.4,
and the read_data/classify_slots split that replaced gpu_utils.py entirely).
"""

import os

# Import the functions we want to test
import sys

import pandas as pd
import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import classify_slots  # noqa: E402
from classify_slots import filter_df, filter_df_enhanced  # noqa: E402


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
    monkeypatch.setattr(classify_slots, "HOST_EXCLUSIONS", {})
    monkeypatch.setattr(classify_slots, "FILTERED_HOSTS_INFO", [])


class TestFilterDfHostExclusionRegex:
    """filter_df/filter_df_enhanced used to loop over HOST_EXCLUSIONS running one
    str.contains() pass per host, with no re.escape() -- an excluded hostname
    containing a regex metacharacter (e.g. an unbalanced parenthesis) would crash
    the whole filter with a pandas error instead of just being excluded (see
    TASK-46; ported from tests/test_gpu_utils_polars.py in TASK-49.4 once the
    polars filter_df/filter_df_enhanced were removed as dead code)."""

    def test_metacharacter_hostname_does_not_raise(self, monkeypatch):
        monkeypatch.setattr(classify_slots, "HOST_EXCLUSIONS", {"gpu(bad.chtc.wisc.edu": "test exclusion"})
        result = filter_df(_exclusion_df())
        assert result["Machine"].tolist() == ["gpu2.chtc.wisc.edu"]

    def test_multiple_hosts_excluded_in_one_pass(self, monkeypatch):
        monkeypatch.setattr(
            classify_slots,
            "HOST_EXCLUSIONS",
            {"gpu(bad.chtc.wisc.edu": "test exclusion", "gpu2.chtc.wisc.edu": "test exclusion"},
        )
        result = filter_df(_exclusion_df())
        assert len(result) == 0


class TestFilterDfEnhancedHostExclusionRegex:
    def test_metacharacter_hostname_does_not_raise(self, monkeypatch):
        monkeypatch.setattr(classify_slots, "HOST_EXCLUSIONS", {"gpu(bad.chtc.wisc.edu": "test exclusion"})
        result = filter_df_enhanced(_exclusion_df())
        assert "gpu(bad.chtc.wisc.edu" not in result["Machine"].tolist()


if __name__ == "__main__":
    pytest.main([__file__])
