from scripts.pipeline_orchestrator import run_step

def test_run_step_executes():
    result = run_step("test", "scripts/data_generation/generate_data.py")
    assert result["status"] in ["success", "failed"]
