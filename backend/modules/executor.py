# modules/executor.py
import subprocess
import tempfile
from typing import Tuple

def run_etl_code(code: str) -> Tuple[str, str]:
    """
    Run the generated ETL code in a separate process
    Returns stdout and stderr
    """
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.py', delete=False) as f:
        f.write(code)
        f.flush()
        result = subprocess.run(["python", f.name], capture_output=True, text=True)
        return result.stdout, result.stderr

def run_etl_script(code: str) -> Tuple[str, str]:
    """
    Run the ETL script and return the output with filtered warnings
    """
    import subprocess
    import tempfile
    import os

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_path = f.name

        result = subprocess.run(["python", temp_path], capture_output=True, text=True)
        
        # Clean up the temporary file
        try:
            os.unlink(temp_path)
        except:
            pass

        # Filter out known numpy warnings
        filtered_stderr = "\n".join(
            line for line in result.stderr.splitlines()
            if "numpy/_core/getlimits.py" not in line
        )

        return result.stdout.strip(), filtered_stderr.strip()
    except Exception as e:
        return "", f"❌ Error running script: {e}"