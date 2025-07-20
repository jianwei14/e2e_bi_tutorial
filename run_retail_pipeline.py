#!/usr/bin/env python3
"""
Retail Data Pipeline Runner
Simple script to ingest CSV files (customers, orders, products, stores) to MinIO
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


def print_banner():
    """Print a nice banner for the pipeline"""
    print("🏪" + "="*58 + "🏪")
    print("    RETAIL DATA PIPELINE - CSV TO MINIO INGESTION")
    print("🏪" + "="*58 + "🏪")
    print()


def print_instructions():
    """Print helpful instructions"""
    print("📋 This pipeline will ingest the following files:")
    print("   • customers.csv  → Customer demographics & contact info")
    print("   • orders.csv     → Order transactions & relationships") 
    print("   • products.csv   → Product catalog & pricing")
    print("   • stores.csv     → Store locations & tax rates")
    print()
    print("🎯 Target: MinIO bucket at http://localhost:9000")
    print("👀 View results: MinIO Console at http://localhost:9001")
    print()


def main():
    """
    Main function to run the retail data pipeline
    """
    print_banner()
    print_instructions()
    
    try:
        # Step 1: Verify data files
        print("Step 1/3: Verifying data files...")
        if not verify_data_files():
            print("❌ Data file verification failed!")
            print("   Make sure you have customers.csv, orders.csv, products.csv, and stores.csv")
            print("   in the e2e_bi_tutorial/data/ directory")
            return False
        print("✅ All data files found!")
        print()
        
        # Step 2: Test MinIO connection
        print("Step 2/3: Testing MinIO connection...")
        if not test_minio_connection():
            print("❌ MinIO connection failed!")
            print("   Make sure your Docker services are running:")
            print("   → docker compose up -d")
            print("   → Check http://localhost:9001 is accessible")
            return False
        print("✅ MinIO connection successful!")
        print()
        
        # Step 3: Run the pipeline
        print("Step 3/3: Running data pipeline...")
        print("🚀 Starting ingestion process...")
        print()
        
        info = run_retail_pipeline()
        
        print()
        print("🎉 SUCCESS! Your retail data has been ingested to MinIO!")
        print()
        print("🔗 Next steps:")
        print("   1. View data in MinIO Console: http://localhost:9001")
        print("   2. Data is stored in the 'lakehouse' bucket")
        print("   3. Use your favorite analytics tools to query the data")
        print()
        
        return True
        
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Pipeline failed with error:")
        print(f"   {str(e)}")
        print("\n🔧 Troubleshooting tips:")
        print("   • Make sure Docker containers are running: docker compose up -d")
        print("   • Check that CSV files exist in e2e_bi_tutorial/data/")
        print("   • Verify MinIO is accessible at http://localhost:9001")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 