#!/usr/bin/env python3
"""
Complete Pipeline Runner
Runs DLT pipeline to load CSV data to MinIO, then creates Iceberg tables for Trino querying
"""

import sys
import subprocess
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"\n{description}")
    print("=" * len(description))
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("Warnings:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: {e}")
        if e.stdout:
            print("Output:", e.stdout)
        if e.stderr:
            print("Error:", e.stderr)
        return False

def main():
    """Run the complete pipeline"""
    print("Complete Data Pipeline")
    print("======================")
    print("Step 1: DLT CSV to MinIO")
    print("Step 2: Create Iceberg tables for Trino")
    print()
    
    # Step 1: Run DLT pipeline
    if not run_command("poetry run python run_retail_pipeline.py", "Running DLT Pipeline"):
        print("DLT pipeline failed. Stopping.")
        return False
    
    # Step 2: Create Iceberg tables
    if not run_command("poetry run python create_iceberg_tables.py", "Creating Iceberg Tables"):
        print("Iceberg table creation failed.")
        return False
    
    print("\n" + "="*60)
    print("🎉 COMPLETE PIPELINE SUCCESS!")
    print("="*60)
    print("Your data is now ready for querying with Trino:")
    print()
    print("1. Connect to Trino (if not already connected)")
    print("2. Run these commands:")
    print("   trino> use iceberg.raw;")
    print("   trino> show tables;")
    print("   trino> select * from customers limit 10;")
    print("   trino> select * from orders limit 10;")
    print("   trino> select * from products limit 10;")
    print("   trino> select * from stores limit 10;")
    print()
    print("MinIO Console: http://localhost:9001 (admin/admin_secret)")
    print("="*60)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 