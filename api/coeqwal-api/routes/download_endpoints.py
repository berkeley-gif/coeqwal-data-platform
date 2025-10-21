"""
Download API endpoints for scenario file downloads
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Optional, Any
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

# Global S3 client
s3_client = None

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

def discover_scenarios_from_s3(bucket: str) -> List[str]:
    """Discover scenario IDs by scanning S3 bucket structure"""
    if not s3_client:
        # Return empty list if S3 not available - no fake scenarios
        return []
    
    scenario_ids = set()
    
    try:
        # List all objects under scenario/ prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix="scenario/", Delimiter="/")
        
        for page in pages:
            # Get scenario directories from CommonPrefixes
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    # Extract scenario_id from "scenario/{scenario_id}/"
                    prefix_path = prefix['Prefix']  # e.g., "scenario/s0011/"
                    if prefix_path.startswith('scenario/') and prefix_path.endswith('/'):
                        scenario_id = prefix_path[9:-1]  # Remove "scenario/" and trailing "/"
                        if scenario_id:  # Make sure it's not empty
                            scenario_ids.add(scenario_id)
        
        return sorted(list(scenario_ids))
        
    except Exception as e:
        logger.error(f"Error discovering scenarios from S3: {e}")
        # Return empty list if S3 scan fails
        return []

@router.get("/scenario", response_model=ScenariosResponse)
async def get_scenarios_for_download():
    """Get all scenarios with their available files from S3"""
    # Initialize S3 client if not already done
    initialize_s3_client()
    
    try:
        # S3 bucket configuration
        s3_bucket = os.getenv("S3_BUCKET", "coeqwal-model-run")
        
        # Debug logging
        logger.info(f"S3 client available: {s3_client is not None}")
        logger.info(f"S3 bucket: {s3_bucket}")
        
        # Discover scenarios from S3 bucket structure
        scenario_ids = discover_scenarios_from_s3(s3_bucket)
        logger.info(f"Discovered scenario IDs: {scenario_ids}")
        scenarios = []
        
        for scenario_id in scenario_ids:
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
    """Generate presigned URL for file download"""
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
