#!/usr/bin/env python3
"""
Convert DLT JSONL files to Iceberg tables
This script reads the JSONL files created by DLT from MinIO and creates Iceberg tables
that can be queried by Trino through the Iceberg REST catalog
"""

import json
import sys
import boto3
import pandas as pd
from pyiceberg.catalog import load_catalog
import pyarrow as pa
from datetime import datetime
from botocore.client import Config

def create_iceberg_tables():
    """Create Iceberg tables from DLT JSONL output in MinIO"""
    
    print("Creating Iceberg tables from DLT JSONL output...")
    
    # Configure S3 client for MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='admin_secret',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
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
    
    # Define the tables we expect from DLT
    table_names = ['customers', 'orders', 'products', 'stores']
    
    # Create namespace in Iceberg catalog
    try:
        catalog.create_namespace("raw")
        print("Created namespace: raw")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("Namespace 'raw' already exists")
        else:
            print(f"Error creating namespace: {e}")
    
    # Get list of objects in the bucket
    try:
        bucket_name = 'lakehouse'
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='raw/'
        )
        
        if 'Contents' not in response:
            print("No files found in MinIO bucket")
            return False
            
    except Exception as e:
        print(f"Error accessing MinIO bucket: {e}")
        return False
    
    # Process each table
    for table_name in table_names:
        try:
            print(f"Processing table: {table_name}")
            
            # Find the latest JSONL file for this table
            table_files = []
            for obj in response['Contents']:
                key = obj['Key']
                if key.startswith(f'raw/{table_name}/') and key.endswith('.jsonl'):
                    table_files.append(key)
            
            if not table_files:
                print(f"No JSONL files found for table: {table_name}")
                continue
            
            # Use the most recent file (sorted by name which includes timestamp)
            latest_file = sorted(table_files)[-1]
            print(f"Reading from: s3://{bucket_name}/{latest_file}")
            
            # Download and read the JSONL file
            try:
                import gzip
                response_obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file)
                raw_content = response_obj['Body'].read()
                
                # Check if content is gzip compressed
                if raw_content.startswith(b'\x1f\x8b'):
                    jsonl_content = gzip.decompress(raw_content).decode('utf-8')
                else:
                    jsonl_content = raw_content.decode('utf-8')
                
                # Parse JSONL to list of dictionaries
                records = []
                for line in jsonl_content.strip().split('\n'):
                    if line.strip():
                        records.append(json.loads(line))
                
                if not records:
                    print(f"Warning: No data found in {table_name}")
                    continue
                    
                print(f"Found {len(records)} records")
                
                # Convert to pandas DataFrame for easier schema handling
                df = pd.DataFrame(records)
                
                # Convert to PyArrow table
                table_data = pa.Table.from_pandas(df)
                
                # Create Iceberg table name
                iceberg_table_name = f"raw.{table_name}"
                
                # Drop table if it exists
                try:
                    catalog.drop_table(iceberg_table_name)
                    print(f"Dropped existing table: {iceberg_table_name}")
                except Exception:
                    pass  # Table doesn't exist, which is fine
                
                # Create the Iceberg table
                iceberg_table = catalog.create_table(
                    iceberg_table_name,
                    schema=table_data.schema
                )
                
                # Insert the data
                iceberg_table.append(table_data)
                
                print(f"Successfully created Iceberg table: {iceberg_table_name}")
                print(f"Inserted {len(table_data)} rows")
                
            except Exception as e:
                print(f"Error reading file {latest_file}: {e}")
                continue
                
        except Exception as e:
            print(f"Error processing table {table_name}: {e}")
            continue
    
    print("\nIceberg table creation completed!")
    print("You can now query these tables using Trino:")
    print("  trino> use iceberg.raw;")
    print("  trino> show tables;")
    print("  trino> select * from customers limit 10;")
    
    return True


def check_minio_connection():
    """Check if MinIO is accessible and has data"""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='admin_secret',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # Try to list buckets
        response = s3_client.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        
        if 'lakehouse' not in bucket_names:
            print("ERROR: 'lakehouse' bucket not found in MinIO")
            return False
            
        print("MinIO connection successful")
        return True
        
    except Exception as e:
        print(f"ERROR: Cannot connect to MinIO: {e}")
        return False


def main():
    """Main function"""
    print("Iceberg Table Creator")
    print("====================")
    
    # Check MinIO connection
    if not check_minio_connection():
        print("Please ensure MinIO is running and DLT pipeline has been executed")
        return False
        
    try:
        return create_iceberg_tables()
    except Exception as e:
        print(f"ERROR: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 