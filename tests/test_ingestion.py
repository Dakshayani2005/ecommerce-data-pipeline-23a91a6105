def test_staging_tables_exist(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging'
    """)
    tables = [t[0] for t in cur.fetchall()]
    assert "customers" in tables
    assert "products" in tables

def test_data_loaded(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM staging.transactions")
    count = cur.fetchone()[0]
    assert count > 0
