# modules/generator.py
import os
from dotenv import load_dotenv
from typing import Dict, Any, Union, List
import json

# Try to import LangChain components with appropriate fallbacks
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.messages import HumanMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    # Fallback to OpenAI if Google Generative AI is not available
    try:
        from langchain_openai import ChatOpenAI as LLMBackend
        from langchain_core.messages import HumanMessage
        LANGCHAIN_AVAILABLE = True
        USE_OPENAI = True
    except ImportError:
        LANGCHAIN_AVAILABLE = False
        USE_OPENAI = False

load_dotenv()

def get_llm():
    """Initialize the LLM based on available APIs and environment variables"""
    if not LANGCHAIN_AVAILABLE:
        raise ImportError("Neither langchain_google_genai nor langchain_openai is available. Please install one of them.")
    
    if 'USE_OPENAI' in locals() and USE_OPENAI:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required but not set")
        return LLMBackend(model="gpt-4", temperature=0)
    else:
        # Use Google Generative AI
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY environment variable is required but not set")
        return ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

def strip_code_block(text: str) -> str:
    """Remove markdown code block formatting if present"""
    if text.startswith("```python"):
        text = text[len("```python"):].lstrip()
    elif text.startswith("```"):
        text = text[len("```"):].lstrip()
    if text.endswith("```"):
        text = text[:-3].rstrip()
    return text

def generate_etl_code(
    source_type: str, 
    source_creds: Dict[str, Any], 
    target_type: str, 
    target_creds: Dict[str, Any], 
    transformations: str,
    src_preview: Union[List, Dict, Any] = None,
    tgt_preview: Union[List, Dict, Any] = None
) -> str:
    """
    Generate ETL code based on source and target database types, credentials,
    and transformation instructions
    """
    # Initialize LLM
    llm = get_llm()
    
    # Convert previews to string representation for the prompt
    src_preview_str = json.dumps(src_preview, default=str) if src_preview else "No preview available"
    tgt_preview_str = json.dumps(tgt_preview, default=str) if tgt_preview else "No preview available"
    
    prompt = f'''
You are a Python data engineer. Write a full ETL script without explanations to:

1. Connect to source DB ({source_type}) using:
   {json.dumps(source_creds, indent=2)}

2. Extract data.

3. Apply transformations:
   - {transformations}

4. Load into target DB ({target_type}) using:
   {json.dumps(target_creds, indent=2)}

Use appropriate libraries (psycopg2, pymysql, pymongo, pyodbc, sqlite3, pandas) and appropriate parameters for each database type.
You need to use only the connection parameters that are needed for the specific database types.
Include all necessary imports and proper connection handling with try/except blocks.

Use this sample data as reference for the source:
{src_preview_str}

This is the structure of the target database:
{tgt_preview_str}

Important:
- Include error handling for all database operations
- Use pandas for data transformations when appropriate
- Close all connections properly
- For SQL databases, create the target table if it doesn't exist
- For MongoDB, create the collection if it doesn't exist
- Make the script completely self-contained and executable
'''

    # Get response from LLM
    response = llm.invoke([HumanMessage(content=prompt)])
    
    # Extract and clean the code
    return strip_code_block(response.content)