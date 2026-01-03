import psycopg2, os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cur = conn.cursor()

# ======================
# DIM DATE
# ======================
from datetime import date, timedelta

def load_dim_date():
    cur.execute("TRUNCATE warehouse.dim_date CASCADE;")

    start = date(2024, 1, 1)
    end = date(2024, 12, 31)

    while start <= end:
        cur.execute("""
            INSERT INTO warehouse.dim_date
            (
                date_key,
                full_date,
                year,
                quarter,
                month,
                day,
                month_name,
                day_name,
                week_of_year,
                is_weekend
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            start,                    
            start,
            start.year,
            (start.month - 1) // 3 + 1,
            start.month,
            start.day,
            start.strftime("%B"),
            start.strftime("%A"),
            start.isocalendar()[1],
            start.weekday() >= 5
        ))

        start += timedelta(days=1)



# ======================
# DIM CUSTOMERS
# ======================
def load_dim_customers():
    cur.execute("TRUNCATE warehouse.dim_customers CASCADE;")
    cur.execute("""
        INSERT INTO warehouse.dim_customers
        (customer_id, full_name, email, city, state, country,
         age_group, customer_segment, registration_date,
         effective_date, is_current)
        SELECT
            customer_id,
            LEFT(first_name || ' ' || last_name, 100),
            LEFT(email,100),
            city, state, country,
            age_group,
            'New',
            registration_date,
            CURRENT_DATE,
            TRUE
        FROM production.customers;
    """)

# ======================
# DIM PRODUCTS
# ======================
def load_dim_products():
    cur.execute("TRUNCATE warehouse.dim_products CASCADE;")
    cur.execute("""
        INSERT INTO warehouse.dim_products
        (product_id, product_name, category, sub_category,
         brand, price_range, effective_date, is_current)
        SELECT
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            CASE
                WHEN price < 50 THEN 'Budget'
                WHEN price < 200 THEN 'Mid-range'
                ELSE 'Premium'
            END,
            CURRENT_DATE,
            TRUE
        FROM production.products;
    """)


# ======================
# FACT SALES
# ======================
def load_fact_sales():
    cur.execute("TRUNCATE warehouse.fact_sales;")

    cur.execute("""
        INSERT INTO warehouse.fact_sales (
            date_key,
            customer_key,
            product_key,
            payment_method_key,
            transaction_id,
            quantity,
            unit_price,
            discount_amount,
            line_total,
            profit
        )
        SELECT
            d.date_key,
            c.customer_key,
            p.product_key,
            pm.payment_method_key,
            t.transaction_id,
            ti.quantity,
            ti.unit_price,
            0 AS discount_amount,
            ti.line_total,
            0 AS profit
        FROM production.transaction_items ti
        JOIN production.transactions t
            ON ti.transaction_id = t.transaction_id
        JOIN warehouse.dim_customers c
            ON t.customer_id = c.customer_id
            AND c.is_current = TRUE
        JOIN warehouse.dim_products p
            ON ti.product_id = p.product_id
            AND p.is_current = TRUE
        JOIN warehouse.dim_payment_method pm
            ON t.payment_method = pm.payment_method_name
        JOIN warehouse.dim_date d
            ON d.full_date = t.transaction_date;
    """)






# ======================
# AGG DAILY SALES
# ======================
def load_agg_daily_sales():
    cur.execute("TRUNCATE warehouse.agg_daily_sales;")

    cur.execute("""
        INSERT INTO warehouse.agg_daily_sales (
            date_key,
            total_transactions,
            total_revenue,
            total_profit,
            unique_customers
        )
        SELECT
            d.date_key,   -- ✅ INTEGER matches warehouse schema
            COUNT(DISTINCT f.transaction_id),
            SUM(f.line_total),
            SUM(f.profit),
            COUNT(DISTINCT f.customer_key)
        FROM warehouse.fact_sales f
        JOIN warehouse.dim_date d
            ON f.date_key = d.date_key
        GROUP BY d.date_key;
    """)



def main():
    print("Loading Warehouse...")
    load_dim_date()
    load_dim_customers()
    load_dim_products()
    load_fact_sales()
    load_agg_daily_sales()
    conn.commit()
    print("Warehouse Load Completed Successfully")

if __name__ == "__main__":
    main()
