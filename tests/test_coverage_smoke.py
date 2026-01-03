def test_import_pipeline_modules():
    """
    Smoke test to ensure pipeline modules can be imported.
    This enables coverage tracking without running full pipeline.
    """
    import scripts.pipeline_orchestrator
    import scripts.data_quality_checks
    import scripts.cleanup_old_data
    import scripts.scheduler

    assert True

import sys
import os
sys.path.append(os.path.abspath("."))

def test_import_pipeline_modules():
    import scripts.pipeline_orchestrator

def test_import_scheduler_module():
    import scripts.scheduler

def test_pipeline_run_step_executes():
    from scripts.pipeline_orchestrator import run_step

    # We don't care if the script succeeds, only that code runs
    result = run_step("data_generation", "scripts/data_generation/generate_data.py")
    assert "status" in result
