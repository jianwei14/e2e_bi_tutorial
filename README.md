# E2E BI Tutorial

CSV to MinIO data pipeline using DLT (Data Load Tool).

## Components

- **MinIO**: S3-compatible object storage
- **Apache Iceberg**: Table format for analytics
- **PostgreSQL**: Iceberg catalog backend
- **DLT**: Python data pipeline framework

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

2. Create `.env` file:
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

### Option 1: Complete Pipeline (Recommended)
Run the complete pipeline to create both MinIO files and Iceberg tables:
```bash
poetry run python run_complete_pipeline.py
```

### Option 2: Step by Step
1. Load CSV data to MinIO:
```bash
poetry run python run_retail_pipeline.py
```

2. Create Iceberg tables for Trino:
```bash
poetry run python create_iceberg_tables.py
```

## Querying with Trino

After running the pipeline, you can query the data using Trino:

1. Connect to Trino (assumes Trino is running):
```bash
trino --server localhost:8080
```

2. Query your Iceberg tables:
```sql
-- Switch to the Iceberg catalog and raw schema
use iceberg.raw;

-- List all tables
show tables;

-- Query the data
select * from customers limit 10;
select * from orders limit 10;
select * from products limit 10;
select * from stores limit 10;

-- Example analytical query
select 
    c.first_name, 
    c.last_name, 
    count(o.id) as order_count,
    sum(o.total_amount) as total_spent
from customers c
join orders o on c.id = o.customer_id
group by c.first_name, c.last_name
order by total_spent desc
limit 20;
```

## Access

- **MinIO Console**: http://localhost:9001 (admin/admin_secret)
- **MinIO API**: http://localhost:9000
- **Iceberg REST**: http://localhost:8181

## Data Files

Place your CSV files in `e2e_bi_tutorial/data/`:
- customers.csv
- orders.csv  
- products.csv
- stores.csv

## Pipeline Features

- **Idempotent**: Safe to run multiple times
- **Two-stage process**: DLT loads data to MinIO, then creates Iceberg tables
- **Trino-compatible**: Creates Iceberg tables queryable by Trino
- **Chunked processing**: Handles large files efficiently
- **Data validation**: Converts data types (dates, etc.)
- **Error handling**: Robust error reporting
- **Automatic decompression**: Handles gzip-compressed DLT output

## Development

The pipeline is configured with:
- `write_disposition="replace"` - Replaces data on each run
- Primary keys defined for each table
- Automatic date column conversion
- MinIO filesystem destination

## Troubleshooting

1. **Container issues**: Check `docker compose ps`
2. **MinIO access**: Verify http://localhost:9001 is accessible
3. **Missing files**: Ensure CSV files exist in `e2e_bi_tutorial/data/`
4. **Python environment**: Verify Poetry environment with `poetry shell`
