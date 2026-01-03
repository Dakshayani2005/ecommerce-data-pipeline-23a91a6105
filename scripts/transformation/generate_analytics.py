import psycopg2
import pandas as pd
import json
import time
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

OUTPUT_DIR = "data/processed/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def execute_query(name, sql):
    start = time.time()
    df = pd.read_sql(sql, conn)
    exec_time = round((time.time() - start) * 1000, 2)
    return df, exec_time

def export_to_csv(df, filename):
    df.to_csv(f"{OUTPUT_DIR}/{filename}", index=False)

def main():
    with open("sql/queries/analytical_queries.sql") as f:
        queries = f.read().split(";")

    summary = {}
    total_time = 0
    count = 1

    for q in queries:
        if q.strip():
            df, t = execute_query(f"query{count}", q)
            export_to_csv(df, f"query{count}.csv")
            summary[f"query{count}"] = {
                "rows": len(df),
                "columns": len(df.columns),
                "execution_time_ms": t
            }
            total_time += t
            count += 1

    analytics_summary = {
        "generation_timestamp": pd.Timestamp.now().isoformat(),
        "queries_executed": count - 1,
        "query_results": summary,
        "total_execution_time_seconds": round(total_time / 1000, 2)
    }

    with open(f"{OUTPUT_DIR}/analytics_summary.json", "w") as f:
        json.dump(analytics_summary, f, indent=4)

    print("Analytics Generated Successfully")

if __name__ == "__main__":
    main()
