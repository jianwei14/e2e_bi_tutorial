"""
dbt Assets for Dagster Integration
Runs dbt transformations as part of the orchestrated pipeline
"""

import os
from pathlib import Path
from dagster import (
    AssetExecutionContext,
    Config,
    get_dagster_logger
)
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
    DbtProject
)

# Define the dbt project
dbt_project_dir = Path(__file__).parent.parent / "e2e_bi_tutorial" / "dbt" / "e2e_bi_tutorial"

# Create dbt project instance
dbt_project = DbtProject(
    project_dir=dbt_project_dir,
    profiles_dir=dbt_project_dir,
)

# Create dbt CLI resource
dbt_cli_resource = DbtCliResource(project_dir=dbt_project_dir, profiles_dir=dbt_project_dir)


class DbtConfig(Config):
    """Configuration for dbt runs"""
    full_refresh: bool = False
    target: str = "trino"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    name="dbt_retail_models"
)
def retail_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig):
    """
    Run all dbt models for retail data transformations
    """
    logger = get_dagster_logger()
    
    # Build dbt command arguments
    dbt_args = ["build"]
    
    if config.full_refresh:
        dbt_args.append("--full-refresh")
        
    if config.target:
        dbt_args.extend(["--target", config.target])
    
    logger.info(f"Running dbt with args: {dbt_args}")
    logger.info(f"dbt project directory: {dbt_project_dir}")
    
    # Run dbt
    yield from dbt.cli(dbt_args, context=context, manifest=dbt_project.manifest_path).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    name="dbt_staging_models",
    select="models/staging"
)
def staging_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig):
    """
    Run only dbt staging models
    """
    logger = get_dagster_logger()
    
    dbt_args = ["build", "--select", "models/staging"]
    
    if config.full_refresh:
        dbt_args.append("--full-refresh")
        
    if config.target:
        dbt_args.extend(["--target", config.target])
    
    logger.info(f"Running dbt staging models with args: {dbt_args}")
    
    yield from dbt.cli(dbt_args, context=context, manifest=dbt_project.manifest_path).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    name="dbt_marts_models", 
    select="models/marts"
)
def marts_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource, config: DbtConfig):
    """
    Run only dbt marts models
    """
    logger = get_dagster_logger()
    
    dbt_args = ["build", "--select", "models/marts"]
    
    if config.full_refresh:
        dbt_args.append("--full-refresh")
        
    if config.target:
        dbt_args.extend(["--target", config.target])
    
    logger.info(f"Running dbt marts models with args: {dbt_args}")
    
    yield from dbt.cli(dbt_args, context=context, manifest=dbt_project.manifest_path).stream()
