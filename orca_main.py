import time
import configparser
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from job import JobManager
from state_store import StateStore
from notifier import Notifier
from logging_setup import setup_logging, get_logger
from path_utils import ensure_directory
import threading

logger = None

class XYZFileHandler(FileSystemEventHandler):
    """Handles new XYZ files in input directory."""
    
    def __init__(self, config, state_store):
        self.config = config
        self.state = state_store
        self.waiting_dir = Path(config.get('paths', 'waiting_dir'))
        self.processing_lock = threading.Lock()
        ensure_directory(self.waiting_dir)
    
    def on_created(self, event):
        """Handle new file creation."""
        if event.is_directory:
            return
        
        file_path = Path(event.src_path)
        
        if file_path.suffix.lower() == '.xyz':
            logger.info(f"Detected new XYZ file: {file_path.name}")
            time.sleep(0.5)  # Wait for file write completion
            self.process_xyz_file(file_path)
    
    def process_xyz_file(self, xyz_file):
        """Convert XYZ to ORCA input and queue job."""
        with self.processing_lock:
            try:
                xyz_path = Path(xyz_file)
                
                if not xyz_path.exists():
                    logger.error(f"XYZ file disappeared: {xyz_file}")
                    return
                
                molecule_name = xyz_path.stem
                
                coords = self._parse_xyz(xyz_path)
                if not coords:
                    logger.error(f"Failed to parse XYZ file: {xyz_file}")
                    return
                
                calc_type = 'freq' if '_freq' in molecule_name else 'opt'
                
                inp_content = self._generate_orca_input(coords, molecule_name, calc_type)
                
                inp_file = self.waiting_dir / f"{molecule_name}.inp"
                with open(inp_file, 'w') as f:
                    f.write(inp_content)
                
                xyz_dest = self.waiting_dir / xyz_path.name
                xyz_path.rename(xyz_dest)
                
                job_id = f"{molecule_name}_{calc_type}_{int(time.time())}"
                job_info = {
                    'molecule_name': molecule_name,
                    'xyz_file': str(xyz_dest),
                    'inp_file': str(inp_file),
                    'calc_type': calc_type,
                    'retry_count': 0
                }
                
                self.state.add_job(job_id, job_info)
                
                logger.info(f"Created job {job_id} for {molecule_name}")
                
            except Exception as e:
                logger.error(f"Error processing {xyz_file}: {e}")
    
    def _parse_xyz(self, xyz_file):
        """Parse XYZ file and extract coordinates."""
        try:
            with open(xyz_file, 'r') as f:
                lines = f.readlines()
            
            lines = [line.strip() for line in lines if line.strip()]
            
            if len(lines) < 3:
                logger.error(f"XYZ file too short: {xyz_file}")
                return None
            
            try:
                num_atoms = int(lines[0].strip())
            except ValueError:
                logger.error(f"Invalid atom count in {xyz_file}")
                return None
            
            coords = []
            for i in range(2, min(2 + num_atoms, len(lines))):
                line = lines[i].replace('\t', ' ')
                parts = line.split()
                
                if len(parts) < 4:
                    continue
                
                element = parts[0].capitalize()
                
                try:
                    x = float(parts[1])
                    y = float(parts[2])
                    z = float(parts[3])
                    coords.append((element, x, y, z))
                except (ValueError, IndexError):
                    logger.warning(f"Skipping invalid line in {xyz_file}: {line}")
                    continue
            
            if len(coords) != num_atoms:
                logger.warning(f"Expected {num_atoms} atoms, found {len(coords)} in {xyz_file}")
            
            return coords if coords else None
            
        except Exception as e:
            logger.error(f"Error parsing {xyz_file}: {e}")
            return None
    
    def _generate_orca_input(self, coords, molecule_name, calc_type):
        """Generate ORCA input file from coordinates."""
        method = self.config.get('orca', 'method', fallback='B3LYP')
        basis = self.config.get('orca', 'basis', fallback='def2-SVP')
        nprocs = self.config.getint('orca', 'nprocs', fallback=4)
        maxcore = self.config.getint('orca', 'maxcore_mb', fallback=2000)
        solvent = self.config.get('orca', 'solvent', fallback='none')
        solvent_model = self.config.get('orca', 'solvent_model', fallback='CPCM')
        scf_conv = self.config.get('orca', 'scf_convergence', fallback='TightSCF')
        
        inp_lines = []
        
        inp_lines.append(f"# ORCA input file for {molecule_name}")
        inp_lines.append(f"# Generated by automated pipeline")
        inp_lines.append("")
        
        inp_lines.append(f"%pal nprocs {nprocs} end")
        inp_lines.append(f"%maxcore {maxcore}")
        inp_lines.append("")
        
        inp_lines.append("%output")
        inp_lines.append("  Print[P_Basis] 2")
        inp_lines.append("  Print[P_MOs] 1")
        inp_lines.append("end")
        inp_lines.append("")
        
        if calc_type == 'opt':
            job_keywords = f"! {method} {basis} OPT {scf_conv}"
        elif calc_type == 'freq':
            job_keywords = f"! {method} {basis} FREQ {scf_conv}"
        else:
            job_keywords = f"! {method} {basis} {scf_conv}"
        
        if solvent.lower() != 'none':
            if solvent_model.upper() == 'CPCM':
                job_keywords += f" CPCM({solvent})"
            elif solvent_model.upper() == 'SMD':
                job_keywords += f" SMD({solvent})"
            elif solvent_model.upper() == 'COSMO':
                job_keywords += f" COSMO({solvent})"
        
        inp_lines.append(job_keywords)
        inp_lines.append("")
        
        charge = 0
        mult = 1
        inp_lines.append(f"* xyz {charge} {mult}")
        
        for element, x, y, z in coords:
            inp_lines.append(f"  {element:<2s}  {x:12.8f}  {y:12.8f}  {z:12.8f}")
        
        inp_lines.append("*")
        inp_lines.append("")
        
        return '\n'.join(inp_lines)

class ORCAPipeline:
    """Main pipeline controller."""
    
    def __init__(self, config_file='config.txt'):
        global logger
        
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        log_dir = self.config.get('logging', 'log_dir', fallback='logs')
        log_level = self.config.get('logging', 'log_level', fallback='INFO')
        logger = setup_logging(log_dir, log_level)
        
        self._ensure_directories()
        
        state_file = self.config.get('paths', 'state_file', fallback='pipeline_state.json')
        self.state = StateStore(state_file)
        
        self.notifier = Notifier(self.config)
        
        self.job_manager = JobManager(self.config, self.state, self.notifier)
        
        input_dir = self.config.get('paths', 'input_dir')
        self.file_handler = XYZFileHandler(self.config, self.state)
        
        self.observer = Observer()
        self.observer.schedule(self.file_handler, input_dir, recursive=False)
        
        self.running = False
        self.job_thread = None
    
    def _ensure_directories(self):
        """Ensure all required directories exist."""
        dirs = [
            'input_dir',
            'waiting_dir',
            'working_dir',
            'products_dir'
        ]
        
        for dir_key in dirs:
            dir_path = self.config.get('paths', dir_key)
            ensure_directory(dir_path)
        
        ensure_directory(self.config.get('logging', 'log_dir', fallback='logs'))
    
    def process_existing_files(self):
        """Process any existing XYZ files in input directory at startup."""
        input_dir = Path(self.config.get('paths', 'input_dir'))
        
        xyz_files = list(input_dir.glob('*.xyz'))
        
        if xyz_files:
            logger.info(f"Found {len(xyz_files)} existing XYZ files to process")
            
            for xyz_file in xyz_files:
                logger.info(f"Processing existing file: {xyz_file.name}")
                self.file_handler.process_xyz_file(xyz_file)
        else:
            logger.info("No existing XYZ files found")
    
    def start(self):
        """Start the pipeline."""
        logger.info("=" * 60)
        logger.info("ORCA Automation Pipeline Starting")
        logger.info("=" * 60)
        
        self.process_existing_files()
        
        self.observer.start()
        logger.info(f"Watching directory: {self.config.get('paths', 'input_dir')}")
        
        self.job_manager.start()
        
        self.running = True
        
        self.job_thread = threading.Thread(target=self._job_processing_loop, daemon=True)
        self.job_thread.start()
        
        logger.info("Pipeline started successfully")
    
    def _job_processing_loop(self):
        """Continuous job processing loop."""
        check_interval = self.config.getint('execution', 'check_interval_seconds', fallback=5)
        
        while self.running:
            try:
                queue_size = self.state.get_queue_size()
                
                if queue_size > 0:
                    logger.info(f"Processing {queue_size} queued jobs")
                    self.job_manager.process_jobs()
                
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"Error in job processing loop: {e}")
                time.sleep(check_interval)
    
    def stop(self):
        """Stop the pipeline."""
        logger.info("Stopping pipeline...")
        
        self.running = False
        
        self.observer.stop()
        self.observer.join()
        
        self.job_manager.stop()
        
        if self.job_thread:
            self.job_thread.join(timeout=10)
        
        logger.info("Pipeline stopped")
    
    def run(self):
        """Run the pipeline continuously."""
        try:
            self.start()
            
            logger.info("Pipeline is running. Press Ctrl+C to stop.")
            
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            self.notifier.notify_fatal_error(str(e))
        finally:
            self.stop()

if __name__ == "__main__":
    pipeline = ORCAPipeline()
    pipeline.run()
