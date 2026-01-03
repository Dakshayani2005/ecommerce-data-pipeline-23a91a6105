import os
import json
import random
import pandas as pd
from faker import Faker
from datetime import date, datetime , timezone


# -----------------------------
# CONFIG
# -----------------------------
CUSTOMERS_COUNT = 1000
PRODUCTS_COUNT = 500
TRANSACTIONS_COUNT = 10000

RAW_DATA_DIR = "data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

fake = Faker()

START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 1, 1)

# -----------------------------
# CUSTOMERS
# -----------------------------
def generate_customers(n):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "customer_id": f"CUST{i:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"user{i}@example.com",
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(
                start_date=START_DATE,
                end_date=END_DATE
            ),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(["18-25", "26-35", "36-45", "46-60", "60+"])
        })
    return pd.DataFrame(rows)

# -----------------------------
# PRODUCTS
# -----------------------------
def generate_products(n):
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty"]
    rows = []

    for i in range(1, n + 1):
        price = round(random.uniform(200, 5000), 2)
        cost = round(price * random.uniform(0.6, 0.9), 2)

        rows.append({
            "product_id": f"PROD{i:04d}",
            "product_name": fake.word().capitalize(),
            "category": random.choice(categories),
            "sub_category": fake.word(),
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1, 50):03d}"
        })
    return pd.DataFrame(rows)

# -----------------------------
# TRANSACTIONS + ITEMS
# -----------------------------
def generate_transactions(customers, products, txn_count):
    transactions = []
    items = []

    item_id = 1

    for i in range(1, txn_count + 1):
        txn_id = f"TXN{i:05d}"
        customer_id = random.choice(customers["customer_id"])

        txn_date = fake.date_between(
            start_date=START_DATE,
            end_date=END_DATE
        )

        txn_items = random.randint(1, 5)
        total_amount = 0

        for _ in range(txn_items):
            product = products.sample(1).iloc[0]
            qty = random.randint(1, 3)
            discount = random.choice([0, 5, 10, 15])

            line_total = round(
                qty * product["price"] * (1 - discount / 100), 2
            )

            items.append({
                "item_id": f"ITEM{item_id:05d}",
                "transaction_id": txn_id,
                "product_id": product["product_id"],
                "quantity": qty,
                "unit_price": product["price"],
                "discount_percentage": discount,
                "line_total": line_total
            })

            total_amount += line_total
            item_id += 1

        transactions.append({
            "transaction_id": txn_id,
            "customer_id": customer_id,
            "transaction_date": txn_date,
            "transaction_time": fake.time(),
            "payment_method": random.choice(
                ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
            ),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": round(total_amount, 2)
        })

    return pd.DataFrame(transactions), pd.DataFrame(items)

# -----------------------------
# MAIN
# -----------------------------
def main():
    customers = generate_customers(CUSTOMERS_COUNT)
    products = generate_products(PRODUCTS_COUNT)
    transactions, transaction_items = generate_transactions(
        customers, products, TRANSACTIONS_COUNT
    )

    customers.to_csv(f"{RAW_DATA_DIR}/customers.csv", index=False)
    products.to_csv(f"{RAW_DATA_DIR}/products.csv", index=False)
    transactions.to_csv(f"{RAW_DATA_DIR}/transactions.csv", index=False)
    transaction_items.to_csv(f"{RAW_DATA_DIR}/transaction_items.csv", index=False)

    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "customers": len(customers),
            "products": len(products),
            "transactions": len(transactions),
            "transaction_items": len(transaction_items)
        }
    }

    with open(f"{RAW_DATA_DIR}/generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

    print(" Data generation completed successfully")

if __name__ == "__main__":
    main()
