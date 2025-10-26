import subprocess
import shutil
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from orca_output_utils import find_output_file, parse_orca_output, extract_final_geometry
from orca_energy_plot import create_energy_plot # 修正
from orca_state import StateStore             # 修正
from path_utils import ensure_directory
from logging_setup import get_logger

logger = get_logger("job_manager")

class JobManager:
    """Manages parallel ORCA job execution with error handling."""
    
    def __init__(self, config, state_store, notifier):
        self.config = config
        self.state = state_store
        self.notifier = notifier
        
        self.orca_path = config.get('orca', 'orca_path')
        self.timeout = config.getint('orca', 'timeout_seconds', fallback=3600)
        self.max_retries = config.getint('execution', 'max_retries', fallback=2)
        self.max_parallel = config.getint('execution', 'max_parallel_jobs', fallback=5)
        
        self.working_dir = Path(config.get('paths', 'working_dir'))
        self.products_dir = Path(config.get('paths', 'products_dir'))
        self.waiting_dir = Path(config.get('paths', 'waiting_dir'))
        
        ensure_directory(self.working_dir)
        ensure_directory(self.products_dir / "success")
        ensure_directory(self.products_dir / "failed")
        
        self.running = False
        self.executor = None
    
    def start(self):
        """Start job processing."""
        self.running = True
        self.executor = ThreadPoolExecutor(max_workers=self.max_parallel)
        logger.info(f"Job manager started with {self.max_parallel} parallel workers")
    
    def stop(self):
        """Stop job processing."""
        self.running = False
        if self.executor:
            self.executor.shutdown(wait=True)
        logger.info("Job manager stopped")
    
    def process_jobs(self):
        """Process all queued jobs."""
        futures = []
        
        while self.running:
            if len(futures) < self.max_parallel:
                job_id, job_info = self.state.get_next_job()
                
                if job_id:
                    future = self.executor.submit(self._execute_job, job_id, job_info)
                    futures.append(future)
                elif not futures:
                    break
            
            done_futures = [f for f in futures if f.done()]
            for future in done_futures:
                futures.remove(future)
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Job execution error: {e}")
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Job execution error: {e}")
    
    def _execute_job(self, job_id, job_info):
        """Execute a single ORCA job."""
        inp_file = Path(job_info['inp_file'])
        molecule_name = job_info['molecule_name']
        calc_type = job_info.get('calc_type', 'opt')
        retry_count = job_info.get('retry_count', 0)
        
        logger.info(f"Starting job {job_id}: {molecule_name} ({calc_type})")
        
        job_work_dir = self.working_dir / f"{molecule_name}_{calc_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        ensure_directory(job_work_dir)
        
        work_inp = job_work_dir / inp_file.name
        shutil.copy(inp_file, work_inp)
        
        try:
            result = self._run_orca(work_inp, job_work_dir)
            output_file = find_output_file(job_work_dir, work_inp.stem)
            parse_result = parse_orca_output(output_file)
            
            if parse_result["success"]:
                self._handle_success(job_id, job_info, job_work_dir, output_file)
            else:
                self._handle_failure(job_id, job_info, job_work_dir, parse_result, retry_count)
        
        except Exception as e:
            logger.error(f"Job {job_id} crashed: {e}")
            self._handle_failure(job_id, job_info, job_work_dir, {
                "error": str(e),
                "error_type": "fatal"
            }, retry_count)
    
    def _run_orca(self, inp_file, work_dir):
        """Execute ORCA calculation."""
        cmd = [self.orca_path, str(inp_file.name)]
        logger.info(f"Running: {' '.join(cmd)} in {work_dir}")
        
        result = subprocess.run(
            cmd,
            cwd=work_dir,
            capture_output=True,
            text=True,
            timeout=self.timeout
        )
        return result
    
    def _handle_success(self, job_id, job_info, work_dir, output_file):
        """Handle successful calculation."""
        molecule_name = job_info['molecule_name']
        calc_type = job_info.get('calc_type', 'opt')
        
        logger.info(f"Job {job_id} completed successfully")
        
        product_dir = self.products_dir / "success" / molecule_name
        ensure_directory(product_dir)
        
        self._archive_results(work_dir, product_dir, calc_type)
        
        if output_file:
            plot_file = product_dir / f"{molecule_name}_{calc_type}_energy.png"
            create_energy_plot(output_file, plot_file)
        
        self.state.mark_completed(job_id, {"product_dir": str(product_dir)})
        self.notifier.notify_completion(job_id, molecule_name)
        
        if calc_type == 'opt':
            self._chain_frequency_calculation(job_info, work_dir, output_file)
        
        self._cleanup_waiting_files(job_info)
        
        try:
            shutil.rmtree(work_dir)
        except Exception as e:
            logger.warning(f"Failed to cleanup working directory: {e}")
    
    def _handle_failure(self, job_id, job_info, work_dir, parse_result, retry_count):
        """Handle failed calculation."""
        molecule_name = job_info['molecule_name']
        error_type = parse_result.get('error_type', 'unknown')
        error_msg = parse_result.get('error', 'Unknown error')
        
        logger.error(f"Job {job_id} failed: {error_msg} (type: {error_type})")
        
        if error_type == 'incomplete' and retry_count < self.max_retries:
            logger.info(f"Retrying job {job_id} (attempt {retry_count + 1}/{self.max_retries})")
            job_info['retry_count'] = retry_count + 1
            self.state.add_job(job_id, job_info)
            self.state.clear_running(job_id)
        else:
            product_dir = self.products_dir / "failed" / molecule_name
            ensure_directory(product_dir)
            
            self._archive_results(work_dir, product_dir, job_info.get('calc_type', 'opt'))
            
            error_file = product_dir / "error.txt"
            with open(error_file, 'w') as f:
                f.write(f"Error Type: {error_type}\n")
                f.write(f"Error Message: {error_msg}\n")
                f.write(f"Retry Count: {retry_count}\n")
            
            self.state.mark_failed(job_id, error_msg)
            self.notifier.notify_error(job_id, molecule_name, error_msg)
            
            self._cleanup_waiting_files(job_info)
        
        try:
            shutil.rmtree(work_dir)
        except Exception as e:
            logger.warning(f"Failed to cleanup working directory: {e}")
    
    def _archive_results(self, work_dir, product_dir, calc_type):
        """Archive calculation results."""
        work_path = Path(work_dir)
        
        for file in work_path.glob("*"):
            if file.is_file():
                dest = product_dir / f"{calc_type}_{file.name}"
                shutil.copy(file, dest)
    
    def _chain_frequency_calculation(self, opt_job_info, work_dir, output_file):
        """Create frequency calculation job from successful optimization."""
        final_geom = extract_final_geometry(output_file)
        
        if not final_geom:
            logger.warning("Cannot chain frequency calculation: no final geometry")
            return
        
        molecule_name = opt_job_info['molecule_name']
        
        freq_xyz_content = f"{len(final_geom)}\n"
        freq_xyz_content += f"{molecule_name} - optimized geometry for frequency\n"
        for element, x, y, z in final_geom:
            freq_xyz_content += f"{element}  {x:12.8f}  {y:12.8f}  {z:12.8f}\n"
        
        freq_xyz_file = self.waiting_dir / f"{molecule_name}_freq.xyz"
        with open(freq_xyz_file, 'w') as f:
            f.write(freq_xyz_content)
        
        logger.info(f"Created frequency calculation for {molecule_name}")
    
    def _cleanup_waiting_files(self, job_info):
        """Remove processed files from waiting directory."""
        try:
            inp_file = Path(job_info['inp_file'])
            xyz_file = Path(job_info.get('xyz_file', ''))
            
            if inp_file.exists():
                inp_file.unlink()
            if xyz_file.exists():
                xyz_file.unlink()
            
            logger.info(f"Cleaned up waiting files for {job_info['molecule_name']}")
        except Exception as e:
            logger.warning(f"Failed to cleanup waiting files: {e}")
