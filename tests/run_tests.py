#!/usr/bin/env python3
"""
Simple test runner script for the warehouse backend application.
"""

import subprocess
import sys
import os

def run_tests():
    """Run all tests with pytest"""
    print("Running tests for jsm-warehousenow-backend...")
    print("=" * 50)
    
    # Run pytest with coverage
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "-v",
        "--cov=warehouse",
        "--cov=services",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov"
    ]
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 50)
        print("✅ All tests passed!")
        print("📊 Coverage report generated in htmlcov/")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Tests failed with exit code {e.returncode}")
        return e.returncode

def run_specific_test(test_file):
    """Run a specific test file"""
    print(f"Running tests in {test_file}...")
    print("=" * 50)
    
    cmd = [sys.executable, "-m", "pytest", test_file, "-v"]
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n✅ Test completed successfully!")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Test failed with exit code {e.returncode}")
        return e.returncode

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test file
        test_file = sys.argv[1]
        if not os.path.exists(test_file):
            print(f"❌ Test file {test_file} not found!")
            sys.exit(1)
        sys.exit(run_specific_test(test_file))
    else:
        # Run all tests
        sys.exit(run_tests())
