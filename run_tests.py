#!/usr/bin/env python3
"""
Test runner for GPU usage statistics tests.

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py -v                 # Verbose output
    python run_tests.py -k test_filter     # Run only filter tests
"""

import subprocess
import sys
import os

def main():
    """Run the test suite."""
    # Make sure we're in the right directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Build the pytest command - run all tests in tests/ directory
    cmd = [sys.executable, "-m", "pytest", "tests/"]

    # Add any command line arguments passed to this script
    cmd.extend(sys.argv[1:])

    # Run the tests
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except FileNotFoundError:
        print("Error: pytest not found. Install with: pip install pytest")
        return 1
    except Exception as e:
        print(f"Error running tests: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
