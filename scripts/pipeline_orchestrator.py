
import subprocess
import json
import time
from datetime import datetime, timezone
import os
import sys

if os.environ.get("PYTEST_RUNNING") == "1":
    sys.exit(0)


LOG_DIR = "logs"
REPORT_DIR = "data/processed"
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

PIPELINE_STEPS = [
    ("data_generation", "scripts/data_generation/generate_data.py"),
    ("data_ingestion", "scripts/ingestion/ingest_to_staging.py"),
    ("data_quality", "scripts/quality_checks/validate_data.py"),
    ("staging_to_production", "scripts/transformation/staging_to_production.py"),
    ("warehouse_load", "scripts/transformation/load_warehouse.py"),
    ("analytics_generation", "scripts/transformation/generate_analytics.py"),
]


def run_step(step_name, script_path):
    start = time.time()
    try:
        result = subprocess.run(
            ["python", script_path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(result.stdout)
        status = "success"
        error = None
    except subprocess.CalledProcessError as e:
        status = "failed"
        error = e.stderr
        print("ERROR OUTPUT:")
        print(e.stderr)

    duration = round(time.time() - start, 2)

    return {
        "step": step_name,
        "status": status,
        "duration_seconds": duration,
        "error_message": error
    }


def main():
    pipeline_start = datetime.now(timezone.utc).isoformat()
    execution_id = f"PIPE_{pipeline_start.replace(':','').replace('-','')}"
    results = []
    overall_status = "success"

    for step_name, script in PIPELINE_STEPS:
        print(f" Running step: {step_name}")
        result = run_step(step_name, script)
        results.append(result)

        if result["status"] == "failed":
            overall_status = "failed"
            print(f" Step failed: {step_name}")
            break
        else:
            print(f"Step completed: {step_name}")

    pipeline_end = datetime.now(timezone.utc).isoformat()

    report = {
        "pipeline_execution_id": execution_id,
        "start_time": pipeline_start,
        "end_time": pipeline_end,
        "total_steps": len(PIPELINE_STEPS),
        "status": overall_status,
        "steps": results
    }

    report_path = f"{REPORT_DIR}/pipeline_execution_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Pipeline report saved to {report_path}")

if __name__ == "__main__":
    main()
