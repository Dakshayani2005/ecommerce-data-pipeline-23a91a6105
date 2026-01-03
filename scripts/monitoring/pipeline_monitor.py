import psycopg2
import json
import time
import statistics
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

# ---------------- LOAD ENV ----------------
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

REPORT_PATH = "data/processed/monitoring_report.json"
LOG_PATH = "logs/monitoring.log"

os.makedirs("data/processed", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# ---------------- HELPERS ----------------
def log(message):
    timestamp = datetime.now(timezone.utc).isoformat()
    with open(LOG_PATH, "a") as f:
        f.write(f"{timestamp} | {message}\n")
    print(message)

def db_connect():
    start = time.time()
    conn = psycopg2.connect(**DB_CONFIG)
    response_time = round((time.time() - start) * 1000, 2)
    return conn, response_time

# ---------------- CHECKS ----------------
def check_data_freshness(cur):
    cur.execute("SELECT MAX(created_at) FROM warehouse.fact_sales;")
    warehouse_time = cur.fetchone()[0]

    if warehouse_time is None:
        return {
            "status": "critical",
            "warehouse_latest_record": None,
            "hours_since_update": None
        }

    # Make DB timestamp timezone-aware (assume UTC)
    warehouse_time = warehouse_time.replace(tzinfo=timezone.utc)

    hours_lag = (
        datetime.now(timezone.utc) - warehouse_time
    ).total_seconds() / 3600

    status = "ok" if hours_lag <= 24 else "critical"

    return {
        "status": status,
        "warehouse_latest_record": warehouse_time.isoformat(),
        "hours_since_update": round(hours_lag, 2)
    }

def check_volume_anomaly(cur):
    cur.execute("""
        SELECT date_key, COUNT(*) 
        FROM warehouse.fact_sales
        WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY date_key
        ORDER BY date_key;
    """)
    rows = cur.fetchall()

    if len(rows) < 2:
        return {
            "status": "insufficient_data",
            "expected_range": None,
            "actual_count": None,
            "anomaly_detected": False
        }

    counts = [r[1] for r in rows]
    today_count = counts[-1]

    mean = statistics.mean(counts)
    std = statistics.stdev(counts)

    anomaly = (
        today_count > mean + 3 * std
        or today_count < mean - 3 * std
    )

    return {
        "status": "anomaly_detected" if anomaly else "ok",
        "expected_range": f"{int(mean - 3 * std)} - {int(mean + 3 * std)}",
        "actual_count": today_count,
        "anomaly_detected": anomaly
    }

def check_data_quality(cur):
    cur.execute("""
        SELECT COUNT(*)
        FROM warehouse.fact_sales
        WHERE customer_key IS NULL
           OR product_key IS NULL
           OR date_key IS NULL;
    """)
    nulls = cur.fetchone()[0]

    score = max(0, 100 - nulls)

    return {
        "status": "ok" if score >= 95 else "degraded",
        "quality_score": score,
        "null_violations": nulls
    }

# ---------------- MAIN ----------------
def main():
    monitoring_time = datetime.now(timezone.utc).isoformat()
    alerts = []

    try:
        conn, response_time = db_connect()
        cur = conn.cursor()

        freshness = check_data_freshness(cur)
        if freshness["status"] == "critical":
            alerts.append({
                "severity": "critical",
                "check": "data_freshness",
                "message": "Warehouse data older than 24 hours",
                "timestamp": monitoring_time
            })

        volume = check_volume_anomaly(cur)
        if volume.get("anomaly_detected"):
            alerts.append({
                "severity": "warning",
                "check": "data_volume",
                "message": "Transaction volume anomaly detected",
                "timestamp": monitoring_time
            })

        quality = check_data_quality(cur)
        if quality["status"] == "degraded":
            alerts.append({
                "severity": "warning",
                "check": "data_quality",
                "message": "Data quality score below threshold",
                "timestamp": monitoring_time
            })

        report = {
            "monitoring_timestamp": monitoring_time,
            "pipeline_health": "healthy" if not alerts else "degraded",
            "checks": {
                "data_freshness": freshness,
                "data_volume_anomalies": volume,
                "data_quality": quality,
                "database_connectivity": {
                    "status": "ok",
                    "response_time_ms": response_time
                }
            },
            "alerts": alerts,
            "overall_health_score": max(0, 100 - (len(alerts) * 10))
        }

        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2)

        log("Monitoring completed successfully")

        cur.close()
        conn.close()

    except Exception as e:
        log(f"CRITICAL ERROR: {str(e)}")

if __name__ == "__main__":
    main()
