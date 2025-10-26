from pathlib import Path
from datetime import datetime

def get_unique_path(base_path, extension=""):
    """Generate unique file path using timestamp."""
    base = Path(base_path)
    stem = base.stem
    parent = base.parent
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    
    if extension:
        return parent / f"{stem}_{timestamp}{extension}"
    return parent / f"{stem}_{timestamp}{base.suffix}"

def ensure_directory(path):
    """Ensure directory exists."""
    Path(path).mkdir(parents=True, exist_ok=True)
    return Path(path)

def safe_move(src, dst):
    """Safely move file, creating destination directory if needed."""
    src_path = Path(src)
    dst_path = Path(dst)
    
    if not src_path.exists():
        raise FileNotFoundError(f"Source file not found: {src}")
    
    ensure_directory(dst_path.parent)
    
    if dst_path.exists():
        dst_path = get_unique_path(dst_path)
    
    src_path.rename(dst_path)
    return dst_path
