"""Scheduler System - Job scheduling with APScheduler."""

from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from typing import Any, Callable

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
except ImportError:
    AsyncIOScheduler = None
    logger.warning("APScheduler not installed")

MAX_CONCURRENT_JOBS = 3
JOB_TIMEOUT = 300


class JobStatus(str, Enum):
    """Job status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobFrequency(str, Enum):
    """Job frequency."""

    ONCE = "once"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CRON = "cron"


class ScheduledJob(BaseModel):
    """Scheduled job model."""

    job_id: str
    name: str
    frequency: str
    function_name: str
    function_params: dict[str, Any] = Field(default_factory=dict)
    cron_expression: str | None = None
    status: str = JobStatus.PENDING.value
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_run: datetime | None = None
    next_run: datetime | None = None
    last_status: str | None = None
    last_error: str | None = None


class SchedulerService:
    """Scheduler service for automated tasks."""

    def __init__(self):
        if AsyncIOScheduler is None:
            raise ImportError(
                "APScheduler not installed. Install with: pip install apscheduler"
            )

        self.scheduler = AsyncIOScheduler(
            max_instances=MAX_CONCURRENT_JOBS,
            job_defaults={"coalesce": True, "max_instances": 1},
        )
        self.jobs: dict[str, ScheduledJob] = {}
        self._setup_event_listeners()

    def _setup_event_listeners(self) -> None:
        """Setup event listeners for job execution."""

        def job_executed(event):
            job = self.jobs.get(event.job_id)
            if job:
                job.last_run = datetime.utcnow()
                job.last_status = JobStatus.COMPLETED.value
                logger.info(f"Job {event.job_id} completed successfully")

        def job_error(event):
            job = self.jobs.get(event.job_id)
            if job:
                job.last_run = datetime.utcnow()
                job.last_status = JobStatus.FAILED.value
                job.last_error = str(event.exception)
                logger.error(f"Job {event.job_id} failed: {event.exception}")

        self.scheduler.add_listener(job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(job_error, EVENT_JOB_ERROR)

    def _parse_cron(self, cron: str) -> dict[str, Any]:
        """Parse cron expression."""
        parts = cron.split()
        if len(parts) != 5:
            raise ValueError(
                "Invalid cron expression. Use: minute hour day month weekday"
            )

        return {
            "minute": parts[0],
            "hour": parts[1],
            "day": parts[2],
            "month": parts[3],
            "day_of_week": parts[4],
        }

    async def schedule_job(
        self,
        job_id: str,
        name: str,
        func: Callable,
        frequency: str = JobFrequency.DAILY.value,
        params: dict[str, Any] | None = None,
        cron: str | None = None,
    ) -> ScheduledJob:
        """Schedule a job."""
        params = params or {}

        job = ScheduledJob(
            job_id=job_id,
            name=name,
            function_name=func.__name__,
            function_params=params,
            frequency=frequency,
            cron_expression=cron,
        )

        trigger = None

        if frequency == JobFrequency.ONCE.value:
            from datetime import timedelta

            trigger = IntervalTrigger(seconds=60)
        elif frequency == JobFrequency.HOURLY.value:
            trigger = IntervalTrigger(hours=1)
        elif frequency == JobFrequency.DAILY.value:
            trigger = IntervalTrigger(days=1)
        elif frequency == JobFrequency.WEEKLY.value:
            trigger = IntervalTrigger(weeks=1)
        elif frequency == JobFrequency.CRON.value and cron:
            cron_parts = self._parse_cron(cron)
            trigger = CronTrigger(**cron_parts)

        if trigger:
            self.scheduler.add_job(
                func,
                trigger=trigger,
                id=job_id,
                name=name,
                replace_existing=True,
                **params,
            )

            job.status = JobStatus.PENDING.value

            if frequency != JobFrequency.ONCE.value:
                next_run = self.scheduler.get_job(job_id)
                if next_run:
                    job.next_run = next_run.next_run_time

        self.jobs[job_id] = job
        logger.info(f"Scheduled job: {job_id} ({frequency})")

        return job

    async def unschedule_job(self, job_id: str) -> bool:
        """Unschedule a job."""
        if job_id in self.jobs:
            self.scheduler.remove_job(job_id)
            del self.jobs[job_id]
            logger.info(f"Unscheduled job: {job_id}")
            return True
        return False

    def get_job(self, job_id: str) -> ScheduledJob | None:
        """Get job details."""
        return self.jobs.get(job_id)

    def get_all_jobs(self) -> list[ScheduledJob]:
        """Get all scheduled jobs."""
        return list(self.jobs.values())

    def start(self) -> None:
        """Start the scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")

    def shutdown(self) -> None:
        """Shutdown the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

    def get_status(self) -> dict[str, Any]:
        """Get scheduler status."""
        return {
            "running": self.scheduler.running,
            "jobs_count": len(self.jobs),
            "jobs": [
                {
                    "job_id": job.job_id,
                    "name": job.name,
                    "frequency": job.frequency,
                    "status": job.status,
                    "last_run": job.last_run.isoformat() if job.last_run else None,
                    "next_run": job.next_run.isoformat() if job.next_run else None,
                    "last_status": job.last_status,
                }
                for job in self.jobs.values()
            ],
        }


scheduler_service = SchedulerService()


async def test_job_function(param: str = "test") -> str:
    """Test job function."""
    logger.info(f"Running test job with param: {param}")
    return f"Job completed with param: {param}"


def create_scheduler_service() -> SchedulerService:
    """Create scheduler service instance."""
    return SchedulerService()
