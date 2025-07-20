"""
Dagster Definitions for E2E BI Tutorial
Simplified to show only the complete end-to-end pipeline
"""

from dagster import (
    Definitions,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)
from dagster_dbt import DbtCliResource

from .assets import dlt_csv_ingestion, iceberg_tables, data_quality_checks, dbt_transformations
from .dbt_assets import dbt_cli_resource, retail_dbt_models, staging_dbt_models, marts_dbt_models
from pathlib import Path

# Get dbt project directory
dbt_project_dir = Path(__file__).parent.parent / "e2e_bi_tutorial" / "dbt" / "e2e_bi_tutorial"

# Load all assets 
pipeline_assets = [dlt_csv_ingestion, iceberg_tables, data_quality_checks, dbt_transformations]
dbt_model_assets = [retail_dbt_models, staging_dbt_models, marts_dbt_models]
all_assets = pipeline_assets + dbt_model_assets

# Define ONLY the complete end-to-end pipeline job
complete_pipeline_job = define_asset_job(
    name="complete_pipeline",
    description="Complete E2E pipeline: CSV → DLT → Iceberg → dbt → validation",
    selection=AssetSelection.assets(*pipeline_assets)
)

# Schedule for the complete pipeline (disabled by default)
daily_pipeline_schedule = ScheduleDefinition(
    job=complete_pipeline_job,
    cron_schedule="0 6 * * *",  # Daily at 6 AM
    default_status=DefaultScheduleStatus.STOPPED
)

# Simplified definitions - only complete pipeline
defs = Definitions(
    assets=all_assets,
    jobs=[complete_pipeline_job],
    schedules=[daily_pipeline_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(dbt_project_dir),
            profiles_dir=str(dbt_project_dir),
        )
    }
)
