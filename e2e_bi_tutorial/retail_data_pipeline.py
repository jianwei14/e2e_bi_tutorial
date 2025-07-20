"""
Retail Data Pipeline using DLT
CSV to MinIO ingestion pipeline
"""

import dlt
import pandas as pd
from pathlib import Path
from typing import Iterator
import logging
from datetime import datetime
import requests
import os

# Set up logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


@dlt.source
def retail_data_source(csv_files_path: str = None):
    """DLT source for retail data"""
    if csv_files_path is None:
        script_dir = Path(__file__).parent
        csv_files_path = script_dir / "data"
    else:
        csv_files_path = Path(csv_files_path)
    
    if not csv_files_path.exists():
        raise FileNotFoundError(f"Data directory not found: {csv_files_path}")
    
    # Define expected files and their configurations
    expected_files = {
        'customers': {
            'file': 'customers.csv',
            'primary_key': 'id',
            'date_columns': ['created_at']
        },
        'orders': {
            'file': 'orders.csv', 
            'primary_key': 'id',
            'date_columns': ['created_at']
        },
        'products': {
            'file': 'products.csv',
            'primary_key': 'id', 
            'date_columns': []
        },
        'stores': {
            'file': 'stores.csv',
            'primary_key': 'id',
            'date_columns': []
        }
    }
    
    resources = []
    
    for table_name, config in expected_files.items():
        csv_file_path = csv_files_path / config['file']
        
        if not csv_file_path.exists():
            continue
        
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
                    
                    # Read CSV
                    df = pd.read_csv(file_path, engine='c')
                    total_rows = len(df)
                    
                    # Convert date columns
                    for date_col in table_config['date_columns']:
                        if date_col in df.columns:
                            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    
                    # Process data in chunks
                    chunk_size = 5000 if total_rows > 20000 else 1000
                    
                    for i in range(0, total_rows, chunk_size):
                        chunk = df.iloc[i:i+chunk_size]
                        records = chunk.to_dict('records')
                        
                        for record in records:
                            yield record
                            
                except Exception as e:
                    logger.error(f"Error processing {file_path_str}: {str(e)}")
                    raise
            
            return load_retail_table
        
        # Create and collect the resource
        resource_func = create_resource(str(csv_file_path), config, table_name)
        resources.append(resource_func())
    
    return resources


def run_retail_pipeline():
    """Main pipeline function"""
    try:
        # Configure the pipeline
        pipeline = dlt.pipeline(
            pipeline_name="retail_data_ingestion",
            destination="filesystem",
            dataset_name="raw"
        )
        
        # Run the pipeline
        start_time = datetime.now()
        info = pipeline.run(
            retail_data_source(),
            write_disposition="replace"
        )
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        print(f"Pipeline completed in {duration:.2f} seconds")
        
        return info
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise


def test_minio_connection():
    """Test connection to MinIO"""
    try:
        minio_api_url = "http://localhost:9000"
        response = requests.get(f"{minio_api_url}/", timeout=5)
        
        if response.status_code in [200, 403]:
            return True
        else:
            return False
            
    except requests.exceptions.ConnectionError:
        return False
    except Exception:
        return False


def verify_data_files():
    """Verify that CSV files exist"""
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"
    
    expected_files = ['customers.csv', 'orders.csv', 'products.csv', 'stores.csv']
    
    if not data_dir.exists():
        return False
    
    missing_files = []
    for filename in expected_files:
        file_path = data_dir / filename
        if not file_path.exists():
            missing_files.append(filename)
    
    return len(missing_files) == 0


if __name__ == "__main__":
    try:
        run_retail_pipeline()
        print("SUCCESS: Check MinIO Console at http://localhost:9001")
    except Exception as e:
        print(f"ERROR: {str(e)}")
        exit(1) 