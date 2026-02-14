"""
Job Scheduler Package

A deterministic job scheduling service that assigns jobs to workers
based on priority and submission time.
"""

__version__ = '0.1.0'

from .types import (
    Job,
    Worker,
    Assignment,
    ScheduleRequest,
    ScheduleResponse,
    SchedulingPolicy,
    BackoffConfig,
    BackoffStrategy,
    calculate_backoff
)

from .algorithm import (
    schedule,
    sort_jobs,
    assign_jobs_round_robin,
    calculate_scheduling_metrics
)

from .server import create_app, run_server

__all__ = [
    'Job',
    'Worker',
    'Assignment',
    'ScheduleRequest',
    'ScheduleResponse',
    'SchedulingPolicy',
    'BackoffConfig',
    'BackoffStrategy',
    'calculate_backoff',
    'schedule',
    'sort_jobs',
    'assign_jobs_round_robin',
    'calculate_scheduling_metrics',
    'create_app',
    'run_server',
]
