"""
Download API endpoints for scenario file downloads
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
import asyncpg
from pydantic import BaseModel
import logging
import os

# AWS S3 integration
try:
    import boto3
    from botocore.exceptions import ClientError
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False

logger = logging.getLogger(__name__)

router = APIRouter(tags=["downloads"])

# Global variable to hold db_pool reference, set by main.py
db_pool = None
s3_client = None

def set_db_pool(pool):
    """Set the database pool - called from main.py after pool creation"""
    global db_pool
    db_pool = pool

def initialize_s3_client():
    """Initialize S3 client if available"""
    global s3_client
    if S3_AVAILABLE and not s3_client:
        try:
            aws_region = os.getenv("AWS_REGION", "us-west-2")
            s3_client = boto3.client('s3', region_name=aws_region)
            logger.info("S3 client initialized for downloads")
        except Exception as e:
            logger.warning(f"S3 client initialization failed: {e}")
            s3_client = None

# Pydantic models matching the Lambda API response format exactly
class FileInfo(BaseModel):
    key: str
    filename: str

class ScenarioFiles(BaseModel):
    zip: Optional[FileInfo] = None
    output_csv: Optional[FileInfo] = None
    sv_csv: Optional[FileInfo] = None

class Scenario(BaseModel):
    scenario_id: str
    files: ScenarioFiles

class ScenariosResponse(BaseModel):
    scenarios: List[Scenario]

# Dependency for database connections
async def get_db():
    """Get database connection from pool"""
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    
    async with db_pool.acquire() as connection:
        yield connection

def check_s3_file_exists(bucket: str, key: str) -> bool:
    """Check if file exists in S3"""
    if not s3_client:
        return False
        
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        logger.error(f"Error checking S3 file {key}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking S3 file {key}: {e}")
        return False

def list_scenario_files(bucket: str, scenario_id: str) -> Dict[str, FileInfo]:
    """Dynamically discover all files for a scenario in S3"""
    if not s3_client:
        return {}
    
    files = {}
    
    try:
        # List all files under scenario/{scenario_id}/
        prefix = f"scenario/{scenario_id}/"
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                filename = key.split('/')[-1]
                
                # Skip directories (keys ending with /)
                if key.endswith('/'):
                    continue
                
                # Categorize files by location and type
                if '/run/' in key:
                    # Any file in /run/ directory is the DSS file (should be only one)
                    files['zip'] = FileInfo(key=key, filename=filename)
                elif '/csv/' in key and 'calsim_output' in filename.lower():
                    files['output_csv'] = FileInfo(key=key, filename=filename)
                elif '/csv/' in key and 'sv_input' in filename.lower():
                    files['sv_csv'] = FileInfo(key=key, filename=filename)
                # Could add more file types here as needed
                
    except Exception as e:
        logger.error(f"Error listing S3 files for scenario {scenario_id}: {e}")
        
    return files

@router.get("/scenario", response_model=ScenariosResponse)
async def get_scenarios_for_download(db: asyncpg.Connection = Depends(get_db)):
    """
    Get all scenarios with their available files for download
    Matches the Lambda API response format exactly
    """
    # Initialize S3 client if not already done
    initialize_s3_client()
    
    try:
        # Get scenarios from database
        query = """
        SELECT scenario_id, short_code, title, description
        FROM scenario
        WHERE is_active = TRUE
        ORDER BY scenario_id
        """
        
        rows = await db.fetch(query)
        scenarios = []
        
        # S3 bucket configuration
        s3_bucket = os.getenv("S3_BUCKET", "coeqwal-model-run")
        
        for row in rows:
            scenario_id = row['scenario_id']
            
            # Dynamically discover all files for this scenario in S3
            discovered_files = list_scenario_files(s3_bucket, scenario_id)
            
            # Convert to ScenarioFiles format
            files = ScenarioFiles()
            
            if 'zip' in discovered_files:
                files.zip = discovered_files['zip']
            if 'output_csv' in discovered_files:
                files.output_csv = discovered_files['output_csv']
            if 'sv_csv' in discovered_files:
                files.sv_csv = discovered_files['sv_csv']
            
            # If S3 not available, create placeholder files for development
            if not s3_client:
                files.zip = FileInfo(
                    key=f"scenario/{scenario_id}/run/{scenario_id}_placeholder.zip",
                    filename=f"{scenario_id}_placeholder.zip"
                )
                files.output_csv = FileInfo(
                    key=f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_calsim_output.csv",
                    filename=f"{scenario_id}_coeqwal_calsim_output.csv"
                )
                files.sv_csv = FileInfo(
                    key=f"scenario/{scenario_id}/csv/{scenario_id}_coeqwal_sv_input.csv",
                    filename=f"{scenario_id}_coeqwal_sv_input.csv"
                )
            
            # Include scenario if it has at least one file
            if files.zip or files.output_csv or files.sv_csv:
                scenario = Scenario(
                    scenario_id=scenario_id,
                    files=files
                )
                scenarios.append(scenario)
        
        return ScenariosResponse(scenarios=scenarios)
        
    except Exception as e:
        logger.error(f"Failed to get scenarios: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get scenarios")

@router.get("/download")
async def get_download_url(
    scenario: str = Query(..., description="Scenario ID"),
    type: str = Query(..., description="File type: zip, output, or sv")
):
    """
    Generate presigned URL for file download
    Matches the Lambda API structure exactly
    """
    # Initialize S3 client if not already done
    initialize_s3_client()
    
    try:
        s3_bucket = os.getenv("S3_BUCKET", "coeqwal-model-run")
        
        # If S3 is not available, return placeholder
        if not s3_client:
            logger.warning("S3 client not available, returning placeholder URL")
            return {"download_url": f"https://example.com/download/{scenario}/{type}"}
        
        # Dynamically discover files for this scenario
        discovered_files = list_scenario_files(s3_bucket, scenario)
        
        # Map file type to discovered file
        s3_key = None
        if type == "zip" and 'zip' in discovered_files:
            s3_key = discovered_files['zip'].key
        elif type == "output" and 'output_csv' in discovered_files:
            s3_key = discovered_files['output_csv'].key
        elif type == "sv" and 'sv_csv' in discovered_files:
            s3_key = discovered_files['sv_csv'].key
        else:
            raise HTTPException(status_code=400, detail="Invalid file type")
        
        if not s3_key:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Generate presigned URL (valid for 1 hour)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': s3_bucket, 'Key': s3_key},
            ExpiresIn=3600
        )
        
        return {"download_url": presigned_url}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate download URL: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to generate download URL")
