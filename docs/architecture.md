# E-Commerce Data Pipeline Architecture

## Overview
This document describes the architecture of the E-Commerce Data Analytics Platform.  
The system is designed using a layered approach to ensure scalability, data integrity, and analytical performance.

---

## System Components

### 1. Data Generation Layer
Synthetic e-commerce data is generated using Python and Faker.
Output includes CSV files for customers, products, transactions, and transaction items.

---

### 2. Data Ingestion Layer
Raw CSV data is loaded into the PostgreSQL staging schema.
Batch ingestion is performed using Python and psycopg2.

---

### 3. Data Storage Layer

- **Staging Schema:** Stores raw data exactly as received
- **Production Schema:** Cleaned, validated, and normalized data (3NF)
- **Warehouse Schema:** Star schema optimized for analytics

---

### 4. Data Processing Layer
This layer performs:
- Data quality checks
- Data cleansing and enrichment
- Dimensional modeling
- Aggregation table generation

---

### 5. Data Serving Layer
Optimized SQL queries and pre-aggregated tables support BI tools.

---

### 6. Visualization Layer
Power BI / Tableau dashboards provide interactive analytics and insights.

---

### 7. Orchestration Layer
Pipeline orchestrator manages execution order, monitoring, and error handling.

---

## Data Models

### Staging Model
Exact replica of CSV structure with minimal validation.

### Production Model
Normalized relational model with foreign key constraints.

### Warehouse Model
Star schema with fact and dimension tables optimized for reporting.

---

## Deployment Architecture
Docker and Docker Compose are used for containerized PostgreSQL deployment.
