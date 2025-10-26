import logging
import os
from pathlib import Path
from datetime import datetime

def setup_logging(log_dir="logs", log_level="INFO"):
    """Configure logging for the pipeline."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"pipeline_{timestamp}.log"
    
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger("orca_pipeline")

def get_logger(name):
    """Get a logger with the specified name."""
    return logging.getLogger(f"orca_pipeline.{name}")
