import pandas as pd
import os

RAW_DIR = "data/raw"

def test_csv_files_exist():
    files = [
        "customers.csv",
        "products.csv",
        "transactions.csv",
        "transaction_items.csv"
    ]
    for f in files:
        assert os.path.exists(os.path.join(RAW_DIR, f))

def test_row_counts():
    customers = pd.read_csv(f"{RAW_DIR}/customers.csv")
    products = pd.read_csv(f"{RAW_DIR}/products.csv")
    transactions = pd.read_csv(f"{RAW_DIR}/transactions.csv")

    assert len(customers) >= 900
    assert len(products) >= 400
    assert len(transactions) >= 9000

def test_required_columns():
    customers = pd.read_csv(f"{RAW_DIR}/customers.csv")
    assert "customer_id" in customers.columns
    assert "email" in customers.columns

def test_id_format():
    customers = pd.read_csv(f"{RAW_DIR}/customers.csv")
    assert customers["customer_id"].str.match(r"CUST\d{4}").all()

def test_referential_integrity():
    customers = pd.read_csv(f"{RAW_DIR}/customers.csv")
    transactions = pd.read_csv(f"{RAW_DIR}/transactions.csv")
    assert transactions["customer_id"].isin(customers["customer_id"]).all()
