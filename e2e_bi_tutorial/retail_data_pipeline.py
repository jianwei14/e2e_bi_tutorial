"""
Retail Data Pipeline using DLT
Optimized for customers, orders, products, and stores CSV files
"""

import dlt
import pandas as pd
from pathlib import Path
from typing import Iterator, Dict, Any, Generator
import logging
from datetime import datetime
import requests
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dlt.source
def retail_data_source(csv_files_path: str = None):
    """
    DLT source optimized for retail data with proper data types and relationships
    """
    if csv_files_path is None:
        script_dir = Path(__file__).parent
        csv_files_path = script_dir / "data"
    else:
        csv_files_path = Path(csv_files_path)
    
    logger.info(f"Loading retail data from: {csv_files_path.absolute()}")
    
    if not csv_files_path.exists():
        raise FileNotFoundError(f"Data directory not found: {csv_files_path}")
    
    # Define expected files and their configurations
    expected_files = {
        'customers': {
            'file': 'customers.csv',
            'primary_key': 'id',
            'date_columns': ['created_at'],
            'description': 'Customer demographic and contact information'
        },
        'orders': {
            'file': 'orders.csv', 
            'primary_key': 'id',
            'date_columns': ['created_at'],
            'description': 'Order transactions linking customers, products, and stores'
        },
        'products': {
            'file': 'products.csv',
            'primary_key': 'id', 
            'date_columns': [],
            'description': 'Product catalog with pricing and descriptions'
        },
        'stores': {
            'file': 'stores.csv',
            'primary_key': 'id',
            'date_columns': [],
            'description': 'Store locations with tax information'
        }
    }
    
    resources = []
    
    for table_name, config in expected_files.items():
        csv_file_path = csv_files_path / config['file']
        
        if not csv_file_path.exists():
            logger.warning(f"File not found: {csv_file_path}")
            continue
            
        logger.info(f"Processing {table_name}: {config['description']}")
        
        # Create resource function for this specific file
        def create_resource(file_path_str: str, table_config: dict, table_name: str):
            @dlt.resource(
                name=table_name,
                primary_key=table_config['primary_key'],
                write_disposition="replace",
                table_name=table_name
            )
            def load_retail_table():
                try:
                    file_path = Path(file_path_str)
                    logger.info(f"Reading {file_path.name}...")
                    
                    # Read CSV with optimized settings for large files
                    df = pd.read_csv(
                        file_path,
                        engine='c'  # Use C engine for speed
                    )
                    
                    total_rows = len(df)
                    logger.info(f"Loaded {total_rows:,} rows from {file_path.name}")
                    
                    # Convert date columns to proper datetime format
                    for date_col in table_config['date_columns']:
                        if date_col in df.columns:
                            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                            logger.info(f"Converted {date_col} to datetime")
                    
                    # Process data in chunks for large files
                    chunk_size = 5000 if total_rows > 20000 else 1000
                    
                    for i in range(0, total_rows, chunk_size):
                        chunk = df.iloc[i:i+chunk_size]
                        
                        # Convert chunk to records
                        records = chunk.to_dict('records')
                        
                        # Log progress for large files
                        if total_rows > 10000:
                            progress = min(i + chunk_size, total_rows)
                            logger.info(f"Processing {table_name}: {progress:,}/{total_rows:,} rows ({progress/total_rows*100:.1f}%)")
                        
                        # Yield records
                        for record in records:
                            yield record
                            
                    logger.info(f"✅ Completed processing {table_name}: {total_rows:,} rows")
                            
                except Exception as e:
                    logger.error(f"❌ Error processing {file_path_str}: {str(e)}")
                    raise
            
            return load_retail_table
        
        # Create and collect the resource
        resource_func = create_resource(str(csv_file_path), config, table_name)
        resources.append(resource_func())
    
    return resources


def run_retail_pipeline():
    """
    Main pipeline function for retail data ingestion to MinIO
    """
    logger.info("🚀 Starting Retail Data Pipeline...")
    
    try:
        # Configure the pipeline
        pipeline = dlt.pipeline(
            pipeline_name="retail_data_ingestion",
            destination="filesystem",
            dataset_name="raw"
        )
        
        logger.info("Pipeline configured. Starting data extraction...")
        
        # Run the pipeline
        start_time = datetime.now()
        info = pipeline.run(
            retail_data_source(),
            write_disposition="replace"
        )
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        logger.info(f"✅ Pipeline completed in {duration:.2f} seconds!")
        
        # Print detailed summary
        print("\n" + "="*60)
        print("📊 PIPELINE SUMMARY")
        print("="*60)
        
        if hasattr(info, 'loads_ids'):
            print(f"🔄 Number of loads: {len(info.loads_ids)}")
        
        # Print table information
        if hasattr(info, 'schema') and info.schema:
            print("📋 Tables loaded:")
            for table_name in info.schema.tables:
                print(f"   ✓ {table_name}")
        
        print(f"⏱️  Total execution time: {duration:.2f} seconds")
        print(f"🗂️  Data location: MinIO bucket 'lakehouse' (http://localhost:9001)")
        print("="*60)
        
        return info
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise


def test_minio_connection():
    """
    Test connection to MinIO by checking if the MinIO console is accessible
    """
    try:
        # Simple HTTP check to see if MinIO is running
        minio_console_url = "http://localhost:9001"
        minio_api_url = "http://localhost:9000"
        
        # Test MinIO API endpoint (this should return XML for bucket list)
        response = requests.get(f"{minio_api_url}/", timeout=5)
        
        if response.status_code in [200, 403]:  # 403 is expected without auth
            logger.info("✅ MinIO API is accessible!")
            
            # Test console access
            try:
                console_response = requests.get(minio_console_url, timeout=5)
                if console_response.status_code == 200:
                    logger.info("✅ MinIO Console is accessible!")
                else:
                    logger.warning("⚠️  MinIO Console may not be fully ready")
            except:
                logger.warning("⚠️  MinIO Console check failed, but API is working")
            
            return True
        else:
            logger.error(f"❌ MinIO API returned status code: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        logger.error("❌ Cannot connect to MinIO - connection refused")
        logger.error("Make sure your MinIO container is running: docker compose up -d")
        return False
    except Exception as e:
        logger.error(f"❌ MinIO connection test failed: {str(e)}")
        return False


def verify_data_files():
    """
    Verify that all expected CSV files exist and are readable
    """
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"
    
    expected_files = ['customers.csv', 'orders.csv', 'products.csv', 'stores.csv']
    
    logger.info("🔍 Verifying data files...")
    
    if not data_dir.exists():
        logger.error(f"❌ Data directory not found: {data_dir}")
        return False
    
    missing_files = []
    found_files = []
    
    for filename in expected_files:
        file_path = data_dir / filename
        if file_path.exists():
            size_mb = file_path.stat().st_size / (1024 * 1024)
            found_files.append(f"{filename} ({size_mb:.1f}MB)")
        else:
            missing_files.append(filename)
    
    if found_files:
        logger.info(f"✅ Found files: {', '.join(found_files)}")
    
    if missing_files:
        logger.warning(f"⚠️  Missing files: {', '.join(missing_files)}")
        return False
    
    return True


if __name__ == "__main__":
    print("🏪 Retail Data Pipeline - CSV to MinIO")
    print("="*50)
    
    # Pre-flight checks
    if not verify_data_files():
        print("❌ Data file verification failed")
        exit(1)
    
    if not test_minio_connection():
        print("❌ MinIO connection failed")
        print("💡 Make sure Docker services are running: docker compose up -d")
        exit(1)
    
    # Run the pipeline
    try:
        run_retail_pipeline()
        print("\n🎉 SUCCESS! Check MinIO Console at http://localhost:9001")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        exit(1) 