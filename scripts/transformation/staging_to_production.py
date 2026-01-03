import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    print("Starting Staging to Production ETL")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()

    try:
        # ---------- Ensure schema ----------
        cur.execute("CREATE SCHEMA IF NOT EXISTS production;")

        # ---------- TRUNCATE ----------
        cur.execute("""
        TRUNCATE TABLE
            production.transaction_items,
            production.transactions,
            production.customers,
            production.products
        CASCADE;
        """)

        # ---------- LOAD CUSTOMERS ----------
        cur.execute("""
        INSERT INTO production.customers (
            customer_id, first_name, last_name, email, phone,
            registration_date, city, state, country, age_group
        )
        SELECT
            s.customer_id,
            INITCAP(TRIM(s.first_name)),
            INITCAP(TRIM(s.last_name)),
            LOWER(TRIM(s.email)),
            REGEXP_REPLACE(s.phone, '[^0-9]', '', 'g'),
            s.registration_date,
            TRIM(s.city),
            TRIM(s.state),
            TRIM(s.country),
            s.age_group
        FROM staging.customers s;
        """)

        # ---------- LOAD PRODUCTS ----------
        cur.execute("""
        INSERT INTO production.products (
            product_id,
            product_name,
            category,
            sub_category,
            price,
            cost,
            brand,
            profit_margin,
            price_category
        )
        SELECT
            s.product_id,
            TRIM(s.product_name),
            TRIM(s.category),
            TRIM(s.sub_category),
            s.price,
            s.cost,
            TRIM(s.brand),
            ROUND(((s.price - s.cost) / s.price) * 100, 2),
            CASE
                WHEN s.price < 50 THEN 'Budget'
                WHEN s.price < 200 THEN 'Mid-range'
                ELSE 'Premium'
            END
        FROM staging.products s
        WHERE s.price > 0;
        """)

        # ---------- LOAD TRANSACTIONS ----------
        cur.execute("""
        INSERT INTO production.transactions (
            transaction_id,
            customer_id,
            transaction_date,
            payment_method,
            total_amount
        )
        SELECT
            s.transaction_id,
            s.customer_id,
            s.transaction_date,
            s.payment_method,
            s.total_amount
        FROM staging.transactions s
        WHERE s.total_amount > 0;
        """)

        # ---------- LOAD TRANSACTION ITEMS ----------
        cur.execute("""
        INSERT INTO production.transaction_items (
            item_id,
            transaction_id,
            product_id,
            quantity,
            unit_price,
            discount_percentage,
            line_total
        )
        SELECT
            s.transaction_item_id,
            s.transaction_id,
            s.product_id,
            s.quantity,
            s.unit_price,
            s.discount,
            s.profit
        FROM staging.transaction_items s
        WHERE s.quantity > 0;
        """)

        conn.commit()
        print("ETL Completed Successfully")

    except Exception as e:
        conn.rollback()
        print("Staging to Production ETL failed:", e)
        raise

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
