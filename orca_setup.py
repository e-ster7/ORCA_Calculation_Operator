#!/usr/bin/env python3
"""Setup script for ORCA Automation Pipeline."""

import os
import sys
from pathlib import Path
import configparser

def create_directories():
    """Create required directory structure."""
    directories = [
        'folders/input',
        'folders/waiting',
        'folders/working',
        'folders/products/success',
        'folders/products/failed',
        'logs'
    ]
    
    print("Creating directory structure...")
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"  ✓ {directory}")
    
    print()

def create_sample_xyz():
    """Create sample XYZ file for testing."""
    sample_content = """3
Water molecule
O     0.00000000    0.00000000    0.11779600
H     0.00000000    0.75545400   -0.47118400
H     0.00000000   -0.75545400   -0.47118400
"""
    
    sample_file = Path('sample_water.xyz')
    with open(sample_file, 'w') as f:
        f.write(sample_content)
    
    print(f"Created sample file: {sample_file}")
    print()

def configure_orca_path():
    """Prompt user for ORCA executable path."""
    print("=" * 60)
    print("ORCA Configuration")
    print("=" * 60)
    print()
    print("Please enter the full path to your ORCA executable.")
    print("Examples:")
    print("  Windows: C:\\orca\\orca.exe")
    print("  Linux:   /usr/local/orca/orca")
    print("  Mac:     /Applications/orca/orca")
    print()
    
    while True:
        orca_path = input("ORCA executable path: ").strip()
        
        if not orca_path:
            print("Path cannot be empty. Please try again.")
            continue
        
        orca_path_obj = Path(orca_path)
        
        if orca_path_obj.exists():
            print(f"✓ Found ORCA at: {orca_path}")
            return orca_path
        else:
            print(f"⚠ Warning: Path does not exist: {orca_path}")
            response = input("Use this path anyway? (y/n): ").strip().lower()
            if response == 'y':
                return orca_path
            print("Please enter a different path.")

def create_config():
    """Create configuration file with user input."""
    config = configparser.ConfigParser()
    
    orca_path = configure_orca_path()
    
    print()
    print("Additional configuration options (press Enter for defaults):")
    print()
    
    method = input("DFT Method [B3LYP]: ").strip() or "B3LYP"
    basis = input("Basis Set [def2-SVP]: ").strip() or "def2-SVP"
    nprocs = input("Number of processors [4]: ").strip() or "4"
    maxcore = input("Max memory per core (MB) [2000]: ").strip() or "2000"
    max_parallel = input("Max parallel jobs [5]: ").strip() or "5"
    
    config['paths'] = {
        'input_dir': 'folders/input',
        'waiting_dir': 'folders/waiting',
        'working_dir': 'folders/working',
        'products_dir': 'folders/products',
        'state_file': 'pipeline_state.json'
    }
    
    config['orca'] = {
        'orca_path': orca_path,
        'method': method,
        'basis': basis,
        'solvent': 'none',
        'solvent_model': 'CPCM',
        'nprocs': nprocs,
        'maxcore_mb': maxcore,
        'timeout_seconds': '3600',
        'scf_convergence': 'TightSCF'
    }
    
    config['execution'] = {
        'max_parallel_jobs': max_parallel,
        'max_retries': '2',
        'check_interval_seconds': '5'
    }
    
    config['gmail'] = {
        'enabled': 'false',
        'sender_email': 'your_email@gmail.com',
        'sender_password': 'your_app_specific_password',
        'recipient_email': 'recipient@example.com'
    }
    
    config['notification'] = {
        'notify_on_completion': 'true',
        'notify_on_error': 'true',
        'notification_threshold': '5',
        'notification_interval_minutes': '60'
    }
    
    config['logging'] = {
        'log_dir': 'logs',
        'log_level': 'INFO'
    }
    
    with open('config.txt', 'w') as f:
        config.write(f)
    
    print()
    print("✓ Configuration saved to config.txt")
    print()

def main():
    """Main setup routine."""
    print()
    print("=" * 60)
    print("ORCA Automation Pipeline - Setup")
    print("=" * 60)
    print()
    
    create_directories()
    
    if not Path('config.txt').exists():
        create_config()
    else:
        print("⚠ config.txt already exists")
        response = input("Overwrite? (y/n): ").strip().lower()
        if response == 'y':
            create_config()
        else:
            print("Keeping existing configuration")
            print()
    
    create_sample_xyz()
    
    print("=" * 60)
    print("Setup Complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Install Python dependencies:")
    print("   pip install -r requirements.txt")
    print()
    print("2. Test the pipeline:")
    print("   python main.py")
    print()
    print("3. Drop XYZ files into 'folders/input/' to start calculations")
    print()
    print("To run in background (Linux/Mac):")
    print("   nohup python main.py > pipeline.log 2>&1 &")
    print()
    print("To run in background (Windows):")
    print("   pythonw main.py")
    print()

if __name__ == "__main__":
    main()
