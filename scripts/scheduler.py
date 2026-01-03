# scripts/scheduler.py

import schedule
import subprocess
import time
import os
import yaml
from datetime import datetime,timezone

LOCK_FILE = "pipeline.lock"
SCHEDULER_LOG = "logs/scheduler_activity.log"

def log(message):
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(SCHEDULER_LOG, "a") as f:
        f.write(f"{timestamp} | {message}\n")
    print(message)

def is_pipeline_running():
    return os.path.exists(LOCK_FILE)

def run_pipeline():
    if is_pipeline_running():
        log("Pipeline already running. Skipping this execution.")
        return

    try:
        open(LOCK_FILE, "w").close()
        log("Starting scheduled pipeline execution")

        result = subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            log("Pipeline executed successfully")
            subprocess.run(["python", "scripts/cleanup_old_data.py"])
        else:
            log("Pipeline execution failed")
            log(result.stderr)

    except Exception as e:
        log(f"Scheduler error: {str(e)}")

    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        log("Pipeline execution finished")

def main():
    os.makedirs("logs", exist_ok=True)

    with open("config/config.yaml") as f:
        config = yaml.safe_load(f)

    run_time = config["scheduler"]["run_time"]

    schedule.every().day.at(run_time).do(run_pipeline)
    log(f"Scheduler started. Pipeline scheduled daily at {run_time}")

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
