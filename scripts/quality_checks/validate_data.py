import os
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

engine = create_engine(DB_URL)

def run_query(q):
    with engine.connect() as conn:
        return conn.execute(text(q)).fetchone()[0]

def main():
    report = {
        "checked_at": datetime.utcnow().isoformat(),
        "checks": {}
    }

    report["checks"]["customers"] = {
        "count": run_query("SELECT COUNT(*) FROM staging.customers"),
        "null_emails": run_query("SELECT COUNT(*) FROM staging.customers WHERE email IS NULL")
    }

    report["checks"]["products"] = {
        "count": run_query("SELECT COUNT(*) FROM staging.products"),
        "invalid_price": run_query("SELECT COUNT(*) FROM staging.products WHERE price <= 0")
    }

    report["checks"]["transactions"] = {
        "count": run_query("SELECT COUNT(*) FROM staging.transactions")
    }

    report["checks"]["transaction_items"] = {
        "count": run_query("SELECT COUNT(*) FROM staging.transaction_items")
    }

    with open("data/quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

    print("Data quality checks passed")

if __name__ == "__main__":
    main()
