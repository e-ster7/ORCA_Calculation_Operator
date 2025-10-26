import re
from pathlib import Path
from logging_setup import get_logger

logger = get_logger("orca_output")

def find_output_file(working_dir, base_name):
    """Find ORCA output file with various naming patterns."""
    working_path = Path(working_dir)
    
    possible_names = [
        f"{base_name}.out",
        f"{base_name}_orca.log",
        f"{base_name}.log"
    ]
    
    for name in possible_names:
        output_file = working_path / name
        if output_file.exists():
            return output_file
    
    logger.warning(f"No output file found for {base_name}")
    return None

def parse_orca_output(output_file):
    """Parse ORCA output to determine success/failure."""
    if not output_file or not Path(output_file).exists():
        return {
            "success": False,
            "error": "Output file not found",
            "error_type": "fatal"
        }
    
    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        return {
            "success": False,
            "error": f"Cannot read output file: {e}",
            "error_type": "fatal"
        }
    
    # Check for successful completion
    if "****ORCA TERMINATED NORMALLY****" in content:
        return {
            "success": True,
            "message": "Calculation completed successfully"
        }
    
    # Check for common errors
    if "ORCA finished by error termination" in content:
        error_msg = extract_error_message(content)
        error_type = classify_error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "error_type": error_type
        }
    
    # Incomplete calculation
    return {
        "success": False,
        "error": "Calculation did not complete",
        "error_type": "incomplete"
    }

def extract_error_message(content):
    """Extract error message from ORCA output."""
    patterns = [
        r"Error\s*:\s*(.+)",
        r"ORCA finished by error termination in (.+)",
        r"The following error occurred:\s*(.+)"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return "Unknown error"

def classify_error(error_msg):
    """Classify error as recoverable, incomplete, or fatal."""
    error_lower = error_msg.lower()
    
    # Recoverable errors that can be retried
    recoverable_patterns = [
        "scf not converged",
        "convergence failure",
        "geometry optimization failed"
    ]
    
    for pattern in recoverable_patterns:
        if pattern in error_lower:
            return "recoverable"
    
    # Fatal errors that should not be retried
    fatal_patterns = [
        "segmentation fault",
        "memory allocation",
        "basis set not found",
        "invalid input"
    ]
    
    for pattern in fatal_patterns:
        if pattern in error_lower:
            return "fatal"
    
    return "incomplete"

def extract_final_geometry(output_file):
    """Extract optimized geometry from ORCA output."""
    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except:
        return None
    
    # Find final geometry section
    pattern = r"CARTESIAN COORDINATES \(ANGSTROEM\)\s*-+\s*(.*?)\s*-+"
    matches = list(re.finditer(pattern, content, re.DOTALL))
    
    if not matches:
        return None
    
    # Get last occurrence (final geometry)
    final_geom = matches[-1].group(1).strip()
    
    coords = []
    for line in final_geom.split('\n'):
        parts = line.split()
        if len(parts) >= 4:
            try:
                element = parts[0]
                x, y, z = float(parts[1]), float(parts[2]), float(parts[3])
                coords.append((element, x, y, z))
            except (ValueError, IndexError):
                continue
    
    return coords if coords else None

def extract_scf_energies(output_file):
    """Extract SCF energies for plotting."""
    try:
        with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except:
        return []
    
    pattern = r"FINAL SINGLE POINT ENERGY\s+(-?\d+\.\d+)"
    energies = re.findall(pattern, content)
    
    return [float(e) for e in energies]
