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

4. Install dependencies:
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

## Pipeline Flow

1. DLT Ingestion: CSV files to MinIO
2. Iceberg Tables: JSONL to queryable tables
3. dbt Transformations: Raw to staging to marts
4. Data Quality: Automated validation

## Troubleshooting

1. Check containers: docker compose ps
2. Check MinIO: http://localhost:9001
3. Check Dagster: http://localhost:3000
4. Verify CSV files exist in e2e_bi_tutorial/data/

For detailed Dagster usage: see DAGSTER_GUIDE.md
