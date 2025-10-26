import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
from pathlib import Path
from orca_output_utils import extract_scf_energies
from logging_setup import get_logger

logger = get_logger("energy_plot")

def create_energy_plot(output_file, plot_file):
    """Create energy convergence plot from ORCA output."""
    energies = extract_scf_energies(output_file)
    
    if not energies:
        logger.warning(f"No energy data found in {output_file}")
        return False
    
    try:
        plt.figure(figsize=(10, 6))
        plt.plot(range(1, len(energies) + 1), energies, 'b-o', linewidth=2, markersize=6)
        plt.xlabel('Optimization Step', fontsize=12)
        plt.ylabel('SCF Energy (Hartree)', fontsize=12)
        plt.title('Energy Convergence', fontsize=14, fontweight='bold')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        plt.savefig(plot_file, dpi=150, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Energy plot saved to {plot_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to create energy plot: {e}")
        return False

def create_comparison_plot(molecules_data, plot_file):
    """Create comparison plot for multiple molecules."""
    try:
        plt.figure(figsize=(12, 8))
        
        for mol_name, energies in molecules_data.items():
            if energies:
                plt.plot(range(1, len(energies) + 1), energies, '-o', label=mol_name, linewidth=2)
        
        plt.xlabel('Optimization Step', fontsize=12)
        plt.ylabel('SCF Energy (Hartree)', fontsize=12)
        plt.title('Energy Convergence Comparison', fontsize=14, fontweight='bold')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        plt.savefig(plot_file, dpi=150, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Comparison plot saved to {plot_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to create comparison plot: {e}")
        return False
