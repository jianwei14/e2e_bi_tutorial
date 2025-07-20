# Dagster Pipeline Guide

## Start Dagster

```bash
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 poetry run dagster dev -h 0.0.0.0 -p 3000 --module-name dagster_pipeline.definitions
```

Access: http://localhost:3000

## Run Complete Pipeline

UI: Assets tab → "Materialize All"
CLI: poetry run dagster asset materialize --select "*"

Job: complete_pipeline (runs entire end-to-end pipeline)

## Pipeline Flow

1. dlt_csv_ingestion - CSV files to MinIO
2. iceberg_tables - JSONL to Iceberg tables  
3. dbt_transformations - All dbt models (staging to marts)
4. data_quality_checks - Validate data

## Individual Assets (if needed)

Run specific steps:
```bash
poetry run dagster asset materialize dlt_csv_ingestion
poetry run dagster asset materialize iceberg_tables
poetry run dagster asset materialize dbt_transformations
poetry run dagster asset materialize data_quality_checks
```

## Monitoring

View in Dagster UI:
- Real-time execution progress
- Row counts and metadata
- Error logs and details
- Asset dependencies

## Schedule

daily_pipeline_schedule - Complete pipeline at 6 AM (disabled by default)
Enable in: Schedules tab → "Start Schedule"

## Troubleshooting

Pipeline not loading:
```bash
cd e2e_bi_tutorial/dbt/e2e_bi_tutorial && poetry run dbt parse
```

Reset if needed:
```bash
rm -rf .tmp_dagster_home_*
docker compose restart
```

The complete pipeline handles everything - just click "Materialize All" or run the complete_pipeline job.
