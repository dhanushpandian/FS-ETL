from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional, Union

class DatabaseCredentials(BaseModel):
    """Model for database credentials"""
    db_type: str
    credentials: Dict[str, Any]

class TransformationRequest(BaseModel):
    """Model for ETL transformation request"""
    source: DatabaseCredentials
    target: DatabaseCredentials
    transformations: str = Field(..., description="Transformation rules in natural language")

class CodeRequest(BaseModel):
    """Model for code execution request"""
    code: str = Field(..., description="Python ETL code to execute")
    
class ValidationResponse(BaseModel):
    """Model for database validation response"""
    success: bool
    message: str
    preview: Optional[Any] = None

class CodeGenerationResponse(BaseModel):
    """Model for code generation response"""
    code: str

class ExecutionResponse(BaseModel):
    """Model for code execution response"""
    success: bool
    stdout: str
    stderr: str