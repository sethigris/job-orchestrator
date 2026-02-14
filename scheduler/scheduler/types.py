"""
Data models for the job scheduler.

This module defines the core data structures used in scheduling:
- Jobs to be scheduled
- Workers available for execution
- Assignments of jobs to workers
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from enum import Enum


class BackoffStrategy(Enum):
    """Strategy for retry backoff calculations."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    NONE = "none"


@dataclass
class Job:
    """
    A job to be scheduled.
    
    Attributes:
        job_id: Unique identifier for the job
        priority: Priority level (1-10, higher is more important)
        submitted_at: ISO 8601 timestamp when job was submitted
        retry_count: Number of times this job has been retried
    """
    job_id: str
    priority: int
    submitted_at: str
    retry_count: int = 0
    
    def __post_init__(self):
        """Validate job fields."""
        if not 1 <= self.priority <= 10:
            raise ValueError(f"Priority must be between 1 and 10, got {self.priority}")
        if self.retry_count < 0:
            raise ValueError(f"Retry count cannot be negative, got {self.retry_count}")


@dataclass
class Worker:
    """
    A worker node available for job execution.
    
    Attributes:
        worker_id: Unique identifier for the worker
    """
    worker_id: str


@dataclass
class Assignment:
    """
    Assignment of a job to a worker.
    
    Attributes:
        job_id: ID of the job being assigned
        worker_id: ID of the worker receiving the job
        scheduled_at: ISO 8601 timestamp when assignment was made
    """
    job_id: str
    worker_id: str
    scheduled_at: str


@dataclass
class ScheduleRequest:
    """
    Request to schedule jobs across available workers.
    
    Attributes:
        pending_jobs: List of jobs waiting to be scheduled
        available_workers: List of workers that can accept jobs
    """
    pending_jobs: List[Job]
    available_workers: List[Worker]


@dataclass
class ScheduleResponse:
    """
    Response containing job assignments.
    
    Attributes:
        assignments: List of job-to-worker assignments
    """
    assignments: List[Assignment]


@dataclass
class BackoffConfig:
    """
    Configuration for retry backoff strategy.
    
    Attributes:
        strategy: The backoff strategy to use
        initial_delay_ms: Initial delay in milliseconds
        max_delay_ms: Maximum delay in milliseconds
        linear_increment_ms: Increment for linear backoff
    """
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    initial_delay_ms: int = 1000
    max_delay_ms: int = 60000
    linear_increment_ms: int = 5000


@dataclass
class SchedulingPolicy:
    """
    Policy configuration for the scheduler.
    
    Attributes:
        backoff: Backoff configuration for retries
        max_assignments: Maximum number of assignments per scheduling round
    """
    backoff: BackoffConfig = field(default_factory=BackoffConfig)
    max_assignments: int = 100


def calculate_backoff(config: BackoffConfig, retry_count: int) -> int:
    """
    Calculate backoff delay for a given retry count.
    
    Args:
        config: Backoff configuration
        retry_count: Number of retries attempted
        
    Returns:
        Delay in milliseconds
    """
    if config.strategy == BackoffStrategy.NONE:
        return 0
    elif config.strategy == BackoffStrategy.LINEAR:
        return min(config.max_delay_ms, 
                  config.linear_increment_ms * retry_count)
    else:  # EXPONENTIAL
        delay = config.initial_delay_ms * (2 ** retry_count)
        return min(config.max_delay_ms, delay)
