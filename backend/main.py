from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Any, Optional, Union
import uvicorn
from dotenv import load_dotenv
import json
import os

# Import the modules from the original app
from modules.validator import validate_and_fetch_schema
from modules.generator import generate_etl_code
from modules.executor import run_etl_script

# Load environment variables
load_dotenv()

app = FastAPI(title="AnytoAny-ETL API", description="API for ETL operations between various databases")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Define models for API
class DatabaseCredentials(BaseModel):
    db_type: str
    credentials: Dict[str, Any]

class TransformationRequest(BaseModel):
    source: DatabaseCredentials
    target: DatabaseCredentials
    transformations: str

class ValidationResponse(BaseModel):
    success: bool
    message: str
    preview: Optional[Any] = None

class CodeGenerationResponse(BaseModel):
    code: str

class ExecutionResponse(BaseModel):
    success: bool
    stdout: str
    stderr: str

@app.get("/")
def read_root():
    return {"message": "Welcome to AnytoAny-ETL API"}

@app.post("/validate", response_model=ValidationResponse)
def validate_connection(db_creds: DatabaseCredentials):
    """
    Validate database connection and fetch schema preview
    """
    try:
        status, preview = validate_and_fetch_schema(db_creds.db_type, db_creds.credentials)
        
        # Convert preview to serializable format if needed
        if isinstance(preview, list):
            # Handle MongoDB or other non-standard formats
            try:
                preview = json.loads(json.dumps(preview, default=str))
            except:
                preview = str(preview)
        
        return {
            "success": not status.startswith("Error"),
            "message": status,
            "preview": preview
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation error: {str(e)}")

@app.post("/generate-code", response_model=CodeGenerationResponse)
def generate_code(request: TransformationRequest):
    """
    Generate ETL code based on source and target databases and transformation rules
    """
    try:
        # Extract data from request
        source_type = request.source.db_type
        source_creds = request.source.credentials
        target_type = request.target.db_type
        target_creds = request.target.credentials
        transformations = request.transformations
        
        # Get schema previews
        src_status, src_preview = validate_and_fetch_schema(source_type, source_creds)
        tgt_status, tgt_preview = validate_and_fetch_schema(target_type, target_creds)
        
        # Generate ETL code
        etl_code = generate_etl_code(
            source_type, 
            source_creds, 
            target_type, 
            target_creds, 
            transformations, 
            src_preview, 
            tgt_preview
        )
        
        return {"code": etl_code}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Code generation error: {str(e)}")

@app.post("/execute-code", response_model=ExecutionResponse)
def execute_code(code: str = Body(..., embed=True)):
    """
    Execute the generated ETL code
    """
    try:
        stdout, stderr = run_etl_script(code)
        return {
            "success": not stderr or "error" not in stderr.lower(),
            "stdout": stdout,
            "stderr": stderr
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Execution error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)