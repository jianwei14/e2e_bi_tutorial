# E2E BI Tutorial

Complete data pipeline: CSV to MinIO to Iceberg to dbt to Trino, orchestrated with Dagster.

## Components

- MinIO: S3-compatible object storage
- Apache Iceberg: Table format for analytics  
- PostgreSQL: Iceberg catalog backend
- DLT: Python data pipeline framework
- dbt: Data transformation framework
- Trino: SQL query engine
- Dagster: Pipeline orchestration
- Apache Superset: BI and data visualization platform

## Prerequisites

- Docker & Docker Compose
- Python 3.12+
- Poetry

## Setup

1. Clone repository:
```bash
git clone https://github.com/jianwei14/e2e_bi_tutorial.git
cd e2e_bi_tutorial
```

2. Create .env file:
```bash
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=admin_secret
AWS_REGION=us-east-1
LAKEHOUSE_NAME=lakehouse
S3_ENDPOINT=http://localhost:9000
ICEBERG_PG_CATALOG_DB=iceberg_catalog
ICEBERG_PG_CATALOG_USER=postgres
ICEBERG_PG_CATALOG_PASSWORD=postgres
```

3. Start infrastructure:
```bash
docker compose up -d
```

4. Start Superset (in separate terminal):
```bash
cd superset
docker compose -f docker-compose-image-tag.yml up
```

5. Install dependencies:
```bash
poetry install
```

## Usage

### Dagster Orchestration (Recommended)

Start Dagster UI:
```bash
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 poetry run dagster dev -h 0.0.0.0 -p 3000 --module-name dagster_pipeline.definitions
```

Access at: http://localhost:3000

Run complete pipeline: Click "Materialize All" in Assets tab

Command line:
```bash
poetry run dagster asset materialize --select "*"
```

### Alternative Methods

Quick script:
```bash
poetry run python run_complete_pipeline.py
```

Manual steps:
```bash
poetry run python run_retail_pipeline.py
poetry run python create_iceberg_tables.py
cd e2e_bi_tutorial/dbt/e2e_bi_tutorial && poetry run dbt build
```

## Querying Data

Connect to Trino:
```bash
trino --server localhost:8080
```

Query raw data:
```sql
use iceberg.raw;
show tables;
select * from customers limit 10;
```

Query transformed data:
```sql
use iceberg.project_marts;
show tables;
select * from marts_iceberg__general limit 10;
```

## Access Points

- Dagster UI: http://localhost:3000
- MinIO Console: http://localhost:9001 (admin/admin_secret)
- Trino: http://localhost:8080
- Iceberg REST: http://localhost:8181
- Apache Superset: http://localhost:8088

## Pipeline Flow

1. DLT Ingestion: CSV files to MinIO
2. Iceberg Tables: JSONL to queryable tables
3. dbt Transformations: Raw to staging to marts
4. Data Quality: Automated validation
5. BI Visualization: Superset dashboards and charts

## Apache Superset Setup & Usage

### Initial Setup

1. Start Superset in separate terminal:
   ```bash
   cd superset
   docker compose -f docker-compose-image-tag.yml up
   ```

2. Access Superset at http://localhost:8088
3. Login with your configured credentials
4. Install Trino connector if needed

### Connect to Trino Database

1. Go to **Settings** → **Database Connections**
2. Click **+ DATABASE**
3. Select **Apache Trino** from the supported databases
4. Enter connection details:
   ```
   HOST: trino
   PORT: 8080
   DATABASE: iceberg
   USERNAME: (leave empty)
   ```
5. Click **CONNECT**

Alternative connection string:
```
trino://trino:8080/iceberg
```

### Create Your First Dataset

1. Go to **Data** → **Datasets**
2. Click **+ DATASET**
3. Select:
   - **Database**: Your Trino connection
   - **Schema**: `raw` (for source data) or `project_marts` (for transformed data)
   - **Table**: Choose from available tables (customers, orders, products, stores)
4. Click **CREATE DATASET AND CREATE CHART**

### Available Schemas and Tables

**Raw Data (`iceberg.raw`):**
- `customers` - Customer information
- `orders` - Order transactions
- `products` - Product catalog
- `stores` - Store locations

**Transformed Data (`iceberg.project_marts`):**
- `marts_iceberg__general` - General business metrics
- `marts_iceberg__marketing` - Marketing analytics
- `marts_iceberg__payment` - Payment analysis

### Create Charts and Dashboards

1. **Create a Chart:**
   - Select your dataset
   - Choose visualization type (Bar Chart, Line Chart, Table, etc.)
   - Configure metrics and dimensions
   - Apply filters as needed
   - Save the chart

2. **Create a Dashboard:**
   - Go to **Dashboards** → **+ DASHBOARD**
   - Add your saved charts
   - Arrange and resize as needed
   - Set up filters and interactions

### Example Queries

Test data connectivity with these SQL queries in **SQL Lab**:

```sql
-- View raw customers
SELECT * FROM iceberg.raw.customers LIMIT 10;

-- View transformed data
SELECT * FROM iceberg.project_marts.marts_iceberg__general LIMIT 10;

-- Custom analytics
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent
FROM iceberg.raw.orders 
GROUP BY customer_id 
ORDER BY total_spent DESC 
LIMIT 20;
```

### Troubleshooting Superset

1. **Connection Issues:**
   - Ensure all containers are running: `docker compose ps`
   - Check Trino is accessible: http://localhost:8080
   - Verify network connectivity between containers

2. **Data Not Loading:**
   - Ensure pipeline has run successfully
   - Check tables exist in Trino: `SHOW TABLES IN iceberg.raw;`
   - Refresh dataset metadata in Superset

3. **Performance Issues:**
   - Use LIMIT clauses for large datasets
   - Create appropriate filters
   - Consider materializing frequently used queries as tables

## Troubleshooting

1. Check containers: `docker compose ps`
2. Check MinIO: http://localhost:9001
3. Check Dagster: http://localhost:3000
4. Check Superset: http://localhost:8088
5. Check Trino: http://localhost:8080
6. Verify CSV files exist in `e2e_bi_tutorial/data/`
7. Check logs: `docker compose logs [service-name]`

For detailed Dagster usage: see DAGSTER_GUIDE.md
