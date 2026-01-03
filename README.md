## Project Architecture

This project implements an end-to-end E-Commerce Data Analytics Pipeline.

Data Flow:
Raw Data → Staging → Production → Warehouse → Analytics → BI Dashboard

CSV Files
   ↓
Staging Schema
   ↓
Production Schema
   ↓
Warehouse (Star Schema)
   ↓
Analytics Tables
   ↓
Power BI / Tableau Dashboard

## Technology Stack

- Data Generation: Python (Faker)
- Database: PostgreSQL
- ETL & Transformations: Python (Pandas, SQLAlchemy)
- Orchestration: Python Pipeline Orchestrator
- Containerization: Docker & Docker Compose
- Testing: Pytest
- BI Tool: Power BI Desktop / Tableau Public

## Project Structure

ecommerce-data-pipeline/
├── data/
│   ├── raw/
│   ├── processed/
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── transformation/
│   ├── monitoring/
│   └── pipeline_orchestrator.py
├── tests/
├── dashboards/
│   ├── powerbi/
│   └── screenshots/
├── docs/
│   ├── architecture.md
│   └── dashboard_guide.md
├── docker-compose.yml
├── pytest.ini
├── requirements.txt
└── README.md

## Setup Instructions

1. Clone the repository
2. Create virtual environment
3. Install dependencies
   pip install -r requirements.txt
4. Create .env file with database credentials
5. Start PostgreSQL using Docker
   docker-compose up -d

## Running the Pipeline

### Full Pipeline
python scripts/pipeline_orchestrator.py

### Individual Steps
python scripts/data_generation/generate_data.py
python scripts/ingestion/ingest_to_staging.py
python scripts/transformation/staging_to_production.py
python scripts/transformation/load_warehouse.py
python scripts/transformation/generate_analytics.py

Data generation completed successfully
Staging ingestion completed successfully
Data quality checks passed
ETL Completed Successfully
Warehouse Load Completed Successfully
Analytics Generated Successfully

## Running Tests

pytest tests/ -v

collected XX items
all tests passed

## Dashboard Access

- Power BI File: dashboards/powerbi/ecommerce_analytics.pbix
- Screenshots: dashboards/screenshots/

## Database Schemas

### Staging Schema
- staging.customers
- staging.products
- staging.transactions
- staging.transaction_items

### Production Schema
- production.customers
- production.products
- production.transactions
- production.transaction_items

### Warehouse Schema
- warehouse.dim_customers
- warehouse.dim_products
- warehouse.dim_date
- warehouse.fact_sales
- warehouse.agg_daily_sales

## Key Insights from Analytics

- Electronics category generates the highest revenue
- Weekend sales are higher than weekdays
- Repeat customers contribute majority of revenue

## Challenges & Solutions

- Database connection issues → Fixed using .env configuration
- Data duplication → Solved using quality checks
- Schema mismatch → Handled using transformation logic

## Future Enhancements

- Real-time streaming using Kafka
- Cloud deployment on AWS
- ML-based sales prediction

## Architecture Design Decisions

PostgreSQL was chosen as the database because it is open-source, reliable, and supports advanced analytical queries. 
The pipeline uses a layered schema approach (staging, production, warehouse) to ensure data quality and traceability.

A star schema is used in the warehouse layer because it improves query performance for analytical workloads and is ideal for BI tools.
Dimension tables store descriptive attributes, while fact tables store transactional metrics.

## Data Lineage

Each data element can be traced end-to-end through the pipeline.
For example, customer email originates in the raw CSV file, moves to the staging schema without modification, 
is cleaned and standardized in the production schema, and is finally used in the warehouse dimension table for analytics.

## Configuration Management

All sensitive configurations such as database credentials are managed using environment variables.
A `.env` file is used to store configuration values, ensuring security and flexibility across environments.

## Troubleshooting

- PostgreSQL connection error: Ensure Docker containers are running
- Authentication issues: Verify `.env` credentials
- Empty dashboards: Run the full pipeline before refreshing BI tools

## Data Model Design

The production schema follows Third Normal Form (3NF) to reduce redundancy and maintain data integrity.
The warehouse schema follows a star schema design to optimize analytical query performance.

Slowly Changing Dimensions (SCD Type 2) are used for customer and product dimensions to preserve historical changes.
Indexes are created on foreign keys and frequently queried columns to improve performance.

