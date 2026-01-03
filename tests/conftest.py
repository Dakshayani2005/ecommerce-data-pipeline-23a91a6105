import pytest
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture(scope="session")
def db_conn():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            dbname=os.getenv("DB_NAME", "ecommerce_db"),
            user=os.getenv("DB_USER", "admin"),
            password=os.getenv("DB_PASSWORD", "password"),
            connect_timeout=5  
        )
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

    yield conn
    conn.close()

