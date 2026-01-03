import os
import sys
import subprocess

def test_run_tests_script_exists():
    assert os.path.exists("run_tests.py")

def test_run_tests_script_help():
    result = subprocess.run(
        [sys.executable, "run_tests.py", "--help"],
        capture_output=True,
        text=True,
        timeout=2
    )
    assert result.returncode == 0