from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from opsiq_runtime.domain.common.ids import CorrelationId, TenantId


@dataclass
class JobStatus:
    """Status of a running job."""

    correlation_id: str
    tenant_id: str
    primitive_name: str
    status: str  # "running", "completed", "cancelled", "failed"
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[dict] = None
    error: Optional[str] = None


class JobManager:
    """Manages running jobs and cancellation flags."""

    def __init__(self) -> None:
        self._jobs: Dict[str, JobStatus] = {}
        self._cancellation_flags: Dict[str, threading.Event] = {}
        self._lock = threading.Lock()

    def register_job(self, correlation_id: str, tenant_id: str, primitive_name: str) -> None:
        """Register a new job."""
        with self._lock:
            self._jobs[correlation_id] = JobStatus(
                correlation_id=correlation_id,
                tenant_id=tenant_id,
                primitive_name=primitive_name,
                status="running",
                started_at=datetime.now(timezone.utc),
            )
            self._cancellation_flags[correlation_id] = threading.Event()

    def is_cancelled(self, correlation_id: str) -> bool:
        """Check if a job is cancelled."""
        with self._lock:
            flag = self._cancellation_flags.get(correlation_id)
            return flag.is_set() if flag else False

    def cancel_job(self, correlation_id: str) -> bool:
        """
        Cancel a job.

        Returns:
            True if job was found and cancelled, False otherwise
        """
        with self._lock:
            if correlation_id not in self._jobs:
                return False
            job = self._jobs[correlation_id]
            if job.status not in ("running",):
                return False  # Can only cancel running jobs
            flag = self._cancellation_flags.get(correlation_id)
            if flag:
                flag.set()
            job.status = "cancelled"
            job.completed_at = datetime.now(timezone.utc)
            return True

    def complete_job(self, correlation_id: str, result: dict) -> None:
        """Mark a job as completed."""
        with self._lock:
            if correlation_id in self._jobs:
                job = self._jobs[correlation_id]
                job.status = "completed"
                job.completed_at = datetime.now(timezone.utc)
                job.result = result

    def fail_job(self, correlation_id: str, error: str) -> None:
        """Mark a job as failed."""
        with self._lock:
            if correlation_id in self._jobs:
                job = self._jobs[correlation_id]
                job.status = "failed"
                job.completed_at = datetime.now(timezone.utc)
                job.error = error

    def get_job_status(self, correlation_id: str) -> Optional[JobStatus]:
        """Get the status of a job."""
        with self._lock:
            return self._jobs.get(correlation_id)

    def cleanup_old_jobs(self, max_age_hours: int = 24) -> None:
        """Remove old completed/failed/cancelled jobs."""
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        with self._lock:
            to_remove = [
                cid
                for cid, job in self._jobs.items()
                if job.status in ("completed", "failed", "cancelled")
                and job.completed_at
                and job.completed_at < cutoff
            ]
            for cid in to_remove:
                del self._jobs[cid]
                self._cancellation_flags.pop(cid, None)


# Global job manager instance
job_manager = JobManager()

