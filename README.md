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

Run the data pipeline:
```bash
poetry run python run_retail_pipeline.py
```

This will:
- Load CSV files from `e2e_bi_tutorial/data/`
- Process: customers.csv, orders.csv, products.csv, stores.csv
- Store data in MinIO bucket `lakehouse/raw/`

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
- **CSV format**: Outputs data as CSV files
- **Chunked processing**: Handles large files efficiently
- **Data validation**: Converts data types (dates, etc.)
- **Error handling**: Robust error reporting

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
