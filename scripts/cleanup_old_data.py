# scripts/cleanup_old_data.py

import os
import time
from datetime import datetime, timedelta

RETENTION_DAYS = 7

TARGET_DIRS = [
    "data/raw",
    "data/staging",
    "logs"
]

PRESERVE_KEYWORDS = ["metadata", "report", "summary"]

def should_preserve(file_name):
    return any(keyword in file_name.lower() for keyword in PRESERVE_KEYWORDS)

def cleanup():
    cutoff_time = time.time() - (RETENTION_DAYS * 86400)
    today = datetime.now().date()

    print("Starting cleanup process...")

    for directory in TARGET_DIRS:
        if not os.path.exists(directory):
            continue

        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)

            if not os.path.isfile(file_path):
                continue

            file_mtime = os.path.getmtime(file_path)
            file_date = datetime.fromtimestamp(file_mtime).date()

            if file_date == today:
                continue

            if should_preserve(file):
                continue

            if file_mtime < cutoff_time:
                os.remove(file_path)
                print(f"Deleted old file: {file_path}")

    print("Cleanup completed successfully")

if __name__ == "__main__":
    cleanup()

