"""
Core scheduling algorithm.

This module implements the pure scheduling logic that assigns jobs
to workers based on priority, submission time, and available resources.

The algorithm is deterministic: given the same inputs, it will always
produce the same outputs.
"""

from datetime import datetime
from typing import List
from itertools import cycle

from .types import Job, Worker, Assignment, SchedulingPolicy


def schedule(
    policy: SchedulingPolicy,
    current_time: datetime,
    jobs: List[Job],
    workers: List[Worker]
) -> List[Assignment]:
    """
    Schedule jobs across available workers.
    
    This is the main scheduling function. It implements a priority-based
    algorithm with FIFO ordering within the same priority level.
    
    Algorithm:
    1. Sort jobs by priority (descending), then by submission time (ascending)
    2. Assign jobs to workers in round-robin fashion
    3. Respect max_assignments limit
    
    Args:
        policy: Scheduling policy configuration
        current_time: Current timestamp for scheduling
        jobs: List of jobs to schedule
        workers: List of available workers
        
    Returns:
        List of assignments mapping jobs to workers
    """
    if not jobs or not workers:
        return []
    
    # Sort jobs by priority (higher first), then by submission time (earlier first)
    sorted_jobs = sort_jobs(jobs)
    
    # Limit assignments based on policy
    max_assignments = min(policy.max_assignments, len(jobs))
    jobs_to_assign = sorted_jobs[:max_assignments]
    
    # Round-robin assignment
    assignments = assign_jobs_round_robin(
        jobs_to_assign,
        workers,
        current_time
    )
    
    return assignments


def sort_jobs(jobs: List[Job]) -> List[Job]:
    """
    Sort jobs by priority (descending) and submission time (ascending).
    
    This ensures:
    - Higher priority jobs are scheduled first
    - Within the same priority, FIFO ordering is maintained
    
    Args:
        jobs: List of jobs to sort
        
    Returns:
        Sorted list of jobs
    """
    return sorted(
        jobs,
        key=lambda job: (
            -job.priority,  # Negative for descending order
            job.submitted_at  # Ascending order (FIFO)
        )
    )


def assign_jobs_round_robin(
    jobs: List[Job],
    workers: List[Worker],
    current_time: datetime
) -> List[Assignment]:
    """
    Assign jobs to workers in round-robin fashion.
    
    This ensures fair distribution of jobs across all workers.
    
    Args:
        jobs: Sorted list of jobs to assign
        workers: List of available workers
        current_time: Timestamp for assignments
        
    Returns:
        List of assignments
    """
    assignments = []
    timestamp = current_time.isoformat()
    
    # Create infinite cycle of workers for round-robin
    worker_cycle = cycle(workers)
    
    for job in jobs:
        worker = next(worker_cycle)
        assignment = Assignment(
            job_id=job.job_id,
            worker_id=worker.worker_id,
            scheduled_at=timestamp
        )
        assignments.append(assignment)
    
    return assignments


def calculate_scheduling_metrics(
    assignments: List[Assignment],
    jobs: List[Job]
) -> dict:
    """
    Calculate metrics about the scheduling decision.
    
    Args:
        assignments: List of job assignments
        jobs: Original list of jobs
        
    Returns:
        Dictionary containing scheduling metrics
    """
    assigned_job_ids = {a.job_id for a in assignments}
    assigned_jobs = [j for j in jobs if j.job_id in assigned_job_ids]
    
    high_priority_count = sum(1 for j in assigned_jobs if j.priority >= 8)
    retry_count = sum(1 for j in assigned_jobs if j.retry_count > 0)
    
    return {
        "jobs_scheduled": len(assignments),
        "high_priority_count": high_priority_count,
        "retries_scheduled": retry_count,
        "average_priority": (
            sum(j.priority for j in assigned_jobs) / len(assigned_jobs)
            if assigned_jobs else 0
        )
    }


def should_schedule_job(
    job: Job,
    policy: SchedulingPolicy,
    current_time: datetime
) -> bool:
    """
    Determine if a job should be scheduled based on backoff policy.
    
    In a full implementation, this would check the last failure time
    and apply backoff delays. For now, we schedule all jobs immediately.
    
    Args:
        job: Job to evaluate
        policy: Scheduling policy
        current_time: Current timestamp
        
    Returns:
        True if job should be scheduled now
    """
    # In a complete implementation, we would:
    # 1. Get last failure timestamp
    # 2. Calculate backoff delay based on retry_count
    # 3. Check if enough time has passed
    # For educational purposes, we schedule immediately
    return True


def distribute_jobs_by_load(
    jobs: List[Job],
    workers: List[Worker],
    worker_loads: dict
) -> List[Assignment]:
    """
    Advanced: Distribute jobs considering worker load.
    
    This is an alternative to round-robin that considers how busy
    each worker currently is.
    
    Args:
        jobs: Jobs to assign
        workers: Available workers
        worker_loads: Dictionary mapping worker_id to current load (0.0-1.0)
        
    Returns:
        List of assignments
        
    Note:
        This is not currently used but shows how to implement
        load-aware scheduling.
    """
    assignments = []
    current_time = datetime.utcnow().isoformat()
    
    # Sort workers by load (least loaded first)
    sorted_workers = sorted(
        workers,
        key=lambda w: worker_loads.get(w.worker_id, 0.0)
    )
    
    # Create cycle from sorted workers
    worker_cycle = cycle(sorted_workers)
    
    for job in jobs:
        worker = next(worker_cycle)
        assignment = Assignment(
            job_id=job.job_id,
            worker_id=worker.worker_id,
            scheduled_at=current_time
        )
        assignments.append(assignment)
    
    return assignments


def priority_scheduler_with_affinity(
    jobs: List[Job],
    workers: List[Worker],
    job_worker_affinity: dict
) -> List[Assignment]:
    """
    Advanced: Schedule jobs with worker affinity preferences.
    
    Some jobs may prefer certain workers (e.g., GPU workers,
    workers with specific capabilities).
    
    Args:
        jobs: Jobs to assign
        workers: Available workers
        job_worker_affinity: Dictionary mapping job_id to preferred worker_ids
        
    Returns:
        List of assignments
        
    Note:
        This is not currently used but shows how to implement
        affinity-based scheduling.
    """
    assignments = []
    current_time = datetime.utcnow().isoformat()
    
    # Track which workers have been assigned
    worker_availability = {w.worker_id: True for w in workers}
    worker_dict = {w.worker_id: w for w in workers}
    
    # First pass: assign jobs with affinity to preferred workers
    for job in jobs:
        preferred_workers = job_worker_affinity.get(job.job_id, [])
        
        assigned = False
        for worker_id in preferred_workers:
            if worker_availability.get(worker_id, False):
                assignments.append(Assignment(
                    job_id=job.job_id,
                    worker_id=worker_id,
                    scheduled_at=current_time
                ))
                worker_availability[worker_id] = False
                assigned = True
                break
        
        # If no preferred worker available, assign to any available worker
        if not assigned:
            for worker_id, available in worker_availability.items():
                if available:
                    assignments.append(Assignment(
                        job_id=job.job_id,
                        worker_id=worker_id,
                        scheduled_at=current_time
                    ))
                    worker_availability[worker_id] = False
                    break
    
    return assignments
