CREATE SCHEMA IF NOT EXISTS warehouse;

-- DIM DATE
CREATE TABLE warehouse.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    week_of_year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- DIM CUSTOMERS (SCD TYPE 2)
CREATE TABLE warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    full_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(150),
    age_group VARCHAR(20),
    customer_segment VARCHAR(20),
    registration_date DATE,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- DIM PRODUCTS (SCD TYPE 2)
CREATE TABLE warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    brand VARCHAR(50),
    price_range VARCHAR(20),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- DIM PAYMENT METHOD
CREATE TABLE warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(30),
    payment_type VARCHAR(20)
);

-- FACT SALES
CREATE TABLE warehouse.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INT REFERENCES warehouse.dim_date(date_key),
    customer_key INT REFERENCES warehouse.dim_customers(customer_key),
    product_key INT REFERENCES warehouse.dim_products(product_key),
    payment_method_key INT REFERENCES warehouse.dim_payment_method(payment_method_key),
    transaction_id VARCHAR(20),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    line_total DECIMAL(10,2),
    profit DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.agg_daily_sales (
    date_key DATE PRIMARY KEY,
    total_transactions INT,
    total_revenue NUMERIC(12,2),
    total_profit NUMERIC(12,2),
    unique_customers INT
);
