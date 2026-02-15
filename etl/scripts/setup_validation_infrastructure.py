#!/usr/bin/env python3
"""
Set up AWS infrastructure for DSS validation testing
Creates S3 structure, uploads reference data, and verifies Batch configuration
"""
import boto3
import json
import argparse
import sys
import os
from pathlib import Path

def create_s3_validation_structure(bucket_name, scenarios):
    """Create S3 directory structure for validation testing"""
    
    s3_client = boto3.client('s3')
    
    print(f"🗂️  Setting up S3 validation structure in bucket: {bucket_name}")
    
    for scenario_id in scenarios:
        # Create directory markers
        directories = [
            f"scenario/{scenario_id}/",
            f"scenario/{scenario_id}/csv/",
            f"scenario/{scenario_id}/validation/",
            f"scenario/{scenario_id}/verify/"
        ]
        
        for directory in directories:
            try:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=directory,
                    Body=''
                )
                print(f"   ✅ Created: s3://{bucket_name}/{directory}")
            except Exception as e:
                print(f"   ❌ Failed to create {directory}: {e}")

def upload_reference_csv(bucket_name, scenario_id, local_csv_path):
    """Upload reference CSV for validation"""
    
    s3_client = boto3.client('s3')
    
    if not os.path.exists(local_csv_path):
        print(f"❌ Reference CSV not found: {local_csv_path}")
        return False
    
    s3_key = f"scenario/{scenario_id}/verify/java_reference.csv"
    
    try:
        print(f"📤 Uploading reference CSV...")
        print(f"   📄 Local: {local_csv_path}")
        print(f"   🎯 S3: s3://{bucket_name}/{s3_key}")
        
        s3_client.upload_file(local_csv_path, bucket_name, s3_key)
        print(f"   ✅ Upload successful!")
        return True
        
    except Exception as e:
        print(f"   ❌ Upload failed: {e}")
        return False

def verify_batch_configuration(job_queue, job_definition):
    """Verify AWS Batch configuration"""
    
    batch_client = boto3.client('batch')
    
    print(f"🔍 Verifying AWS Batch configuration...")
    
    # Check job queue
    try:
        response = batch_client.describe_job_queues(jobQueues=[job_queue])
        if response['jobQueues']:
            queue = response['jobQueues'][0]
            print(f"   ✅ Job Queue: {job_queue} (State: {queue['state']})")
        else:
            print(f"   ❌ Job Queue not found: {job_queue}")
            return False
    except Exception as e:
        print(f"   ❌ Error checking job queue: {e}")
        return False
    
    # Check job definition
    try:
        response = batch_client.describe_job_definitions(
            jobDefinitionName=job_definition,
            status='ACTIVE'
        )
        if response['jobDefinitions']:
            job_def = response['jobDefinitions'][0]
            print(f"   ✅ Job Definition: {job_definition} (Revision: {job_def['revision']})")
            
            # Show container details
            container = job_def['containerProperties']
            print(f"      🐳 Image: {container['image']}")
            print(f"      💾 Memory: {container['memory']} MB")
            print(f"      🖥️  vCPUs: {container['vcpus']}")
            
        else:
            print(f"   ❌ Job Definition not found or inactive: {job_definition}")
            return False
    except Exception as e:
        print(f"   ❌ Error checking job definition: {e}")
        return False
    
    return True

def create_validation_manifest(bucket_name, scenarios):
    """Create validation test manifest"""
    
    manifest = {
        "validation_test_setup": {
            "created_at": "2024-01-01T00:00:00Z",
            "bucket": bucket_name,
            "scenarios": {}
        }
    }
    
    for scenario_id in scenarios:
        manifest["validation_test_setup"]["scenarios"][scenario_id] = {
            "dss_zip_key": f"scenario/{scenario_id}/input.zip",
            "reference_csv_key": f"scenario/{scenario_id}/verify/java_reference.csv",
            "validation_output_dir": f"scenario/{scenario_id}/validation/",
            "status": "ready_for_testing"
        }
    
    # Save locally
    manifest_path = "validation_test_manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"📋 Created validation manifest: {manifest_path}")
    return manifest_path

def main():
    parser = argparse.ArgumentParser(
        description="Set up AWS infrastructure for DSS validation testing"
    )
    parser.add_argument("--bucket", required=True,
                       help="S3 bucket for validation testing")
    parser.add_argument("--scenarios", nargs='+', required=True,
                       help="List of scenario IDs to set up")
    parser.add_argument("--reference-csv", 
                       help="Local path to reference CSV file")
    parser.add_argument("--job-queue", default="coeqwal-etl-queue",
                       help="AWS Batch job queue to verify")
    parser.add_argument("--job-definition", default="coeqwal-etl-enhanced",
                       help="AWS Batch job definition to verify")
    parser.add_argument("--skip-s3-setup", action="store_true",
                       help="Skip S3 directory structure creation")
    
    args = parser.parse_args()
    
    success = True
    
    # Set up S3 structure
    if not args.skip_s3_setup:
        create_s3_validation_structure(args.bucket, args.scenarios)
    
    # Upload reference CSV if provided
    if args.reference_csv:
        if len(args.scenarios) == 1:
            success &= upload_reference_csv(
                args.bucket, 
                args.scenarios[0], 
                args.reference_csv
            )
        else:
            print("⚠️  Reference CSV provided but multiple scenarios specified.")
            print("   Please upload reference CSVs individually for each scenario.")
    
    # Verify Batch configuration
    success &= verify_batch_configuration(args.job_queue, args.job_definition)
    
    # Create manifest
    create_validation_manifest(args.bucket, args.scenarios)
    
    if success:
        print("\n🎉 Validation infrastructure setup complete!")
        print("\nNext steps:")
        print("1. Upload DSS zip files to the scenario directories")
        print("2. Upload reference CSV files if not done already")
        print("3. Use submit_validation_test.py to run validation jobs")
        return 0
    else:
        print("\n❌ Setup completed with errors. Please review and fix issues.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
