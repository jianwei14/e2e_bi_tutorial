"""
Dagster Assets for E2E BI Tutorial Pipeline
Orchestrates DLT ingestion and Iceberg table creation
"""

import sys
import subprocess
from pathlib import Path
from dagster import (
    asset, 
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Config,
    get_dagster_logger
)
from typing import Dict, Any
import pandas as pd


class PipelineConfig(Config):
    """Configuration for the pipeline"""
    csv_data_path: str = "e2e_bi_tutorial/data/"
    force_refresh: bool = False


@asset(
    description="Ingest CSV files to MinIO using DLT pipeline",
    group_name="ingestion"
)
def dlt_csv_ingestion(context: AssetExecutionContext, config: PipelineConfig) -> MaterializeResult:
    """
    Run DLT pipeline to ingest CSV files to MinIO
    """
    logger = get_dagster_logger()
    
    try:
        # Get project root directory
        project_root = Path(__file__).parent.parent
        
        # Run the DLT pipeline
        logger.info("Starting DLT CSV ingestion pipeline...")
        
        cmd = ["poetry", "run", "python", "run_retail_pipeline.py"]
        result = subprocess.run(
            cmd, 
            cwd=project_root,
            capture_output=True, 
            text=True, 
            check=True
        )
        
        logger.info(f"DLT pipeline output: {result.stdout}")
        if result.stderr:
            logger.warning(f"DLT pipeline warnings: {result.stderr}")
        
        # Parse output for metadata (basic example)
        metadata = {
            "execution_time": MetadataValue.text("Successfully completed"),
            "output_location": MetadataValue.text("s3://lakehouse/raw/"),
            "pipeline_output": MetadataValue.text(result.stdout[-500:])  # Last 500 chars
        }
        
        return MaterializeResult(metadata=metadata)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"DLT pipeline failed: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise Exception(f"DLT pipeline execution failed: {e}")


@asset(
    description="Create Iceberg tables from DLT output for Trino querying",
    deps=[dlt_csv_ingestion],
    group_name="transformation"
)
def iceberg_tables(context: AssetExecutionContext) -> MaterializeResult:
    """
    Convert DLT JSONL files to Iceberg tables
    """
    logger = get_dagster_logger()
    
    try:
        # Get project root directory
        project_root = Path(__file__).parent.parent
        
        # Run the Iceberg table creation script
        logger.info("Creating Iceberg tables from DLT output...")
        
        cmd = ["poetry", "run", "python", "create_iceberg_tables.py"]
        result = subprocess.run(
            cmd,
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True
        )
        
        logger.info(f"Iceberg creation output: {result.stdout}")
        if result.stderr:
            logger.warning(f"Iceberg creation warnings: {result.stderr}")
        
        # Extract table information from output
        tables_created = []
        lines = result.stdout.split('\n')
        for line in lines:
            if "Successfully created Iceberg table:" in line:
                table_name = line.split(":")[-1].strip()
                tables_created.append(table_name)
        
        metadata = {
            "tables_created": MetadataValue.json(tables_created),
            "total_tables": MetadataValue.int(len(tables_created)),
            "catalog_location": MetadataValue.text("http://localhost:8181"),
            "creation_output": MetadataValue.text(result.stdout[-500:])
        }
        
        return MaterializeResult(metadata=metadata)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Iceberg table creation failed: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise Exception(f"Iceberg table creation failed: {e}")


@asset(
    description="Validate that Iceberg tables are queryable and contain expected data",
    deps=[iceberg_tables],
    group_name="validation"
)
def data_quality_checks(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run basic data quality checks on the created Iceberg tables
    """
    logger = get_dagster_logger()
    
    try:
        # Import here to avoid dependency issues
        from pyiceberg.catalog import load_catalog
        
        # Configure Iceberg catalog
        catalog = load_catalog(
            "iceberg",
            **{
                "type": "rest",
                "uri": "http://localhost:8181",
                "s3.endpoint": "http://localhost:9000", 
                "s3.access-key-id": "admin",
                "s3.secret-access-key": "admin_secret",
                "s3.region": "us-east-1",
                "s3.path-style-access": "true"
            }
        )
        
        # Expected tables and their minimum row counts
        expected_tables = {
            "raw.customers": 1000,
            "raw.orders": 1000, 
            "raw.products": 100,
            "raw.stores": 10
        }
        
        validation_results = {}
        
        for table_name, min_rows in expected_tables.items():
            try:
                # Load table and get row count
                table = catalog.load_table(table_name)
                df = table.scan().to_pandas()
                row_count = len(df)
                
                # Basic validation
                is_valid = row_count >= min_rows
                validation_results[table_name] = {
                    "row_count": row_count,
                    "min_expected": min_rows,
                    "is_valid": is_valid
                }
                
                logger.info(f"Table {table_name}: {row_count} rows (min: {min_rows}) - {'✅ PASS' if is_valid else '❌ FAIL'}")
                
            except Exception as e:
                logger.error(f"Failed to validate table {table_name}: {e}")
                validation_results[table_name] = {
                    "error": str(e),
                    "is_valid": False
                }
        
        # Check if all validations passed
        all_valid = all(result.get("is_valid", False) for result in validation_results.values())
        
        metadata = {
            "validation_results": MetadataValue.json(validation_results),
            "all_checks_passed": MetadataValue.bool(all_valid),
            "total_tables_checked": MetadataValue.int(len(expected_tables))
        }
        
        if not all_valid:
            logger.warning("Some data quality checks failed!")
        else:
            logger.info("All data quality checks passed!")
        
        return MaterializeResult(metadata=metadata)
        
    except Exception as e:
        logger.error(f"Data quality check failed: {e}")
        # Don't fail the entire pipeline for validation issues
        metadata = {
            "validation_error": MetadataValue.text(str(e)),
            "all_checks_passed": MetadataValue.bool(False)
        }
        return MaterializeResult(metadata=metadata) 

@asset(
    description="Run dbt transformations on Iceberg tables",
    deps=[iceberg_tables],
    group_name="transformation"
)
def dbt_transformations(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run dbt staging and marts transformations
    """
    logger = get_dagster_logger()
    
    try:
        # Get project root directory
        project_root = Path(__file__).parent.parent
        dbt_project_dir = project_root / "e2e_bi_tutorial" / "dbt" / "e2e_bi_tutorial"
        
        # Run dbt build command
        logger.info("Running dbt transformations...")
        
        cmd = ["poetry", "run", "dbt", "build", "--profiles-dir", str(dbt_project_dir), "--project-dir", str(dbt_project_dir)]
        result = subprocess.run(
            cmd,
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True
        )
        
        logger.info(f"dbt output: {result.stdout}")
        if result.stderr:
            logger.warning(f"dbt warnings: {result.stderr}")
        
        # Parse dbt output for model information
        models_built = []
        lines = result.stdout.split('\n')
        for line in lines:
            if "OK created" in line or "OK incremental" in line:
                # Extract model name from dbt output
                parts = line.split()
                if len(parts) >= 4:
                    model_name = parts[-1]
                    models_built.append(model_name)
        
        metadata = {
            "models_built": MetadataValue.json(models_built),
            "total_models": MetadataValue.int(len(models_built)),
            "dbt_project_dir": MetadataValue.text(str(dbt_project_dir)),
            "dbt_output": MetadataValue.text(result.stdout[-500:])
        }
        
        return MaterializeResult(metadata=metadata)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt transformations failed: {e}")
        logger.error(f"stdout: {e.stdout}")
        logger.error(f"stderr: {e.stderr}")
        raise Exception(f"dbt transformations failed: {e}")
