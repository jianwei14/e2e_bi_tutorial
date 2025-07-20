#!/usr/bin/env python3
"""
Retail Data Pipeline Runner
Simple script to ingest CSV files to MinIO
"""

import sys
from pathlib import Path

# Add the project directory to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from e2e_bi_tutorial.retail_data_pipeline import (
    run_retail_pipeline, 
    test_minio_connection, 
    verify_data_files
)


def main():
    """Run the retail data pipeline"""
    print("Retail Data Pipeline")
    print("===================")
    
    try:
        # Verify data files
        print("Verifying data files...")
        if not verify_data_files():
            print("ERROR: Data file verification failed")
            print("Ensure customers.csv, orders.csv, products.csv, and stores.csv exist in e2e_bi_tutorial/data/")
            return False
        print("Data files found")
        
        # Test MinIO connection
        print("Testing MinIO connection...")
        if not test_minio_connection():
            print("ERROR: MinIO connection failed")
            print("Make sure Docker services are running: docker compose up -d")
            return False
        print("MinIO connection successful")
        
        # Run the pipeline
        print("Running data pipeline...")
        info = run_retail_pipeline()
        
        print("SUCCESS: Data ingested to MinIO")
        print("View results: http://localhost:9001")
        
        return True
        
    except KeyboardInterrupt:
        print("Pipeline interrupted by user")
        return False
    except Exception as e:
        print(f"ERROR: {str(e)}")
        print("Check Docker containers and CSV files")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 