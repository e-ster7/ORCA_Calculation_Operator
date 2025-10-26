import threading
from pathlib import Path
from datetime import datetime
from safe_file_utils import atomic_json_write, safe_json_read
from logging_setup import get_logger

logger = get_logger("state_store")

class StateStore:
    """Manages persistent job state across pipeline restarts."""
    
    def __init__(self, state_file):
        self.state_file = Path(state_file)
        self.lock = threading.Lock()
        self._ensure_state_file()
    
    def _ensure_state_file(self):
        """Create state file if it doesn't exist."""
        if not self.state_file.exists():
            initial_state = {
                "queued": [],
                "running": {},
                "completed": [],
                "failed": [],
                "last_updated": datetime.now().isoformat()
            }
            atomic_json_write(self.state_file, initial_state)
    
    def _load_state(self):
        """Load current state from file."""
        default_state = {
            "queued": [],
            "running": {},
            "completed": [],
            "failed": []
        }
        return safe_json_read(self.state_file, default=default_state)
    
    def _save_state(self, state):
        """Save state to file atomically."""
        state["last_updated"] = datetime.now().isoformat()
        atomic_json_write(self.state_file, state)
    
    def add_job(self, job_id, job_info):
        """Add job to queue."""
        with self.lock:
            state = self._load_state()
            if job_id not in [j["job_id"] for j in state["queued"]]:
                state["queued"].append({
                    "job_id": job_id,
                    "info": job_info,
                    "added_at": datetime.now().isoformat()
                })
                self._save_state(state)
                logger.info(f"Added job to queue: {job_id}")
    
    def get_next_job(self):
        """Get next job from queue."""
        with self.lock:
            state = self._load_state()
            if state["queued"]:
                job = state["queued"].pop(0)
                job_id = job["job_id"]
                state["running"][job_id] = {
                    "info": job["info"],
                    "started_at": datetime.now().isoformat()
                }
                self._save_state(state)
                return job_id, job["info"]
            return None, None
    
    def mark_completed(self, job_id, result_info=None):
        """Mark job as completed."""
        with self.lock:
            state = self._load_state()
            if job_id in state["running"]:
                job_data = state["running"].pop(job_id)
                state["completed"].append({
                    "job_id": job_id,
                    "info": job_data["info"],
                    "result": result_info,
                    "completed_at": datetime.now().isoformat()
                })
                self._save_state(state)
                logger.info(f"Job completed: {job_id}")
    
    def mark_failed(self, job_id, error_info):
        """Mark job as failed."""
        with self.lock:
            state = self._load_state()
            if job_id in state["running"]:
                job_data = state["running"].pop(job_id)
                state["failed"].append({
                    "job_id": job_id,
                    "info": job_data["info"],
                    "error": error_info,
                    "failed_at": datetime.now().isoformat()
                })
                self._save_state(state)
                logger.error(f"Job failed: {job_id} - {error_info}")
    
    def get_running_jobs(self):
        """Get all currently running jobs."""
        with self.lock:
            state = self._load_state()
            return state.get("running", {})
    
    def get_queue_size(self):
        """Get number of queued jobs."""
        with self.lock:
            state = self._load_state()
            return len(state.get("queued", []))
    
    def clear_running(self, job_id):
        """Remove job from running state."""
        with self.lock:
            state = self._load_state()
            if job_id in state["running"]:
                state["running"].pop(job_id)
                self._save_state(state)
