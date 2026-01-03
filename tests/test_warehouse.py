


def test_production_tables(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM production.customers")
    assert cur.fetchone()[0] > 0

def test_email_cleaning(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM production.customers
        WHERE email != LOWER(email)
    """)
    assert cur.fetchone()[0] == 0

def test_constraints(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM production.products
        WHERE price <= 0
    """)
    assert cur.fetchone()[0] == 0
