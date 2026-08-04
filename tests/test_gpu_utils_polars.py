"""Tests for gpu_utils_polars.py's host-exclusion filtering.

filter_df/filter_df_enhanced used to loop over HOST_EXCLUSIONS running one
str.contains() pass per host, with no re.escape() -- an excluded hostname
containing a regex metacharacter (e.g. an unbalanced parenthesis) would crash
the whole filter with a polars ComputeError instead of just being excluded.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import gpu_utils_polars  # noqa: E402
from gpu_utils_polars import filter_df, filter_df_enhanced  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_host_exclusions(monkeypatch):
    monkeypatch.setattr(gpu_utils_polars, "HOST_EXCLUSIONS", {})
    monkeypatch.setattr(gpu_utils_polars, "FILTERED_HOSTS_INFO", [])


def _df():
    return pl.DataFrame(
        {
            "Name": ["slot1@gpu(bad.chtc.wisc.edu", "slot1@gpu2.chtc.wisc.edu"],
            "AssignedGPUs": ["GPU-1A", "GPU-2A"],
            "State": ["Claimed", "Claimed"],
            "Machine": ["gpu(bad.chtc.wisc.edu", "gpu2.chtc.wisc.edu"],
            "PrioritizedProjects": ["", ""],
        }
    )


class TestFilterDfHostExclusionRegex:
    def test_metacharacter_hostname_does_not_raise(self, monkeypatch):
        monkeypatch.setattr(gpu_utils_polars, "HOST_EXCLUSIONS", {"gpu(bad.chtc.wisc.edu": "test exclusion"})
        result = filter_df(_df())
        assert result["Machine"].to_list() == ["gpu2.chtc.wisc.edu"]

    def test_multiple_hosts_excluded_in_one_pass(self, monkeypatch):
        monkeypatch.setattr(
            gpu_utils_polars,
            "HOST_EXCLUSIONS",
            {"gpu(bad.chtc.wisc.edu": "test exclusion", "gpu2.chtc.wisc.edu": "test exclusion"},
        )
        result = filter_df(_df())
        assert result.height == 0


class TestFilterDfEnhancedHostExclusionRegex:
    def test_metacharacter_hostname_does_not_raise(self, monkeypatch):
        monkeypatch.setattr(gpu_utils_polars, "HOST_EXCLUSIONS", {"gpu(bad.chtc.wisc.edu": "test exclusion"})
        result = filter_df_enhanced(_df())
        assert "gpu(bad.chtc.wisc.edu" not in result["Machine"].to_list()
