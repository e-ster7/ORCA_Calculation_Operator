import json
import tempfile
import shutil
from pathlib import Path
from logging_setup import get_logger

logger = get_logger("safe_file")

def atomic_write(filepath, content, mode='w'):
    """Write file atomically to prevent corruption."""
    filepath = Path(filepath)
    
    try:
        with tempfile.NamedTemporaryFile(
            mode=mode,
            dir=filepath.parent,
            delete=False,
            suffix='.tmp'
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
            if mode == 'w':
                tmp_file.write(content)
            else:
                tmp_file.write(content)
        
        shutil.move(str(tmp_path), str(filepath))
        return True
    except Exception as e:
        logger.error(f"Failed to write {filepath}: {e}")
        if tmp_path.exists():
            tmp_path.unlink()
        return False

def atomic_json_write(filepath, data):
    """Write JSON file atomically."""
    content = json.dumps(data, indent=2)
    return atomic_write(filepath, content, mode='w')

def safe_read(filepath, default=None):
    """Safely read file with fallback."""
    try:
        with open(filepath, 'r') as f:
            return f.read()
    except FileNotFoundError:
        return default
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return default

def safe_json_read(filepath, default=None):
    """Safely read JSON file with fallback."""
    content = safe_read(filepath)
    if content is None:
        return default
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filepath}: {e}")
        return default
