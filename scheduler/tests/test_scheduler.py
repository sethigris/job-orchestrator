"""
Unit tests for the job scheduler.

Run with: pytest tests/test_scheduler.py
"""

import pytest
from datetime import datetime

from scheduler.types import Job, Worker, Assignment, SchedulingPolicy, BackoffConfig, BackoffStrategy
from scheduler.algorithm import (
    schedule,
    sort_jobs,
    assign_jobs_round_robin,
    calculate_scheduling_metrics
)


class TestJobSorting:
    """Test job sorting logic."""
    
    def test_sort_by_priority(self):
        """Jobs should be sorted by priority (higher first)."""
        jobs = [
            Job("job1", priority=1, submitted_at="2024-02-13T10:00:00Z"),
            Job("job2", priority=10, submitted_at="2024-02-13T10:00:00Z"),
            Job("job3", priority=5, submitted_at="2024-02-13T10:00:00Z"),
        ]
        
        sorted_jobs = sort_jobs(jobs)
        
        assert sorted_jobs[0].job_id == "job2"  # priority 10
        assert sorted_jobs[1].job_id == "job3"  # priority 5
        assert sorted_jobs[2].job_id == "job1"  # priority 1
    
    def test_sort_by_time_within_priority(self):
        """Within same priority, jobs should be FIFO."""
        jobs = [
            Job("job1", priority=5, submitted_at="2024-02-13T10:00:02Z"),
            Job("job2", priority=5, submitted_at="2024-02-13T10:00:00Z"),
            Job("job3", priority=5, submitted_at="2024-02-13T10:00:01Z"),
        ]
        
        sorted_jobs = sort_jobs(jobs)
        
        assert sorted_jobs[0].job_id == "job2"  # earliest
        assert sorted_jobs[1].job_id == "job3"  # middle
        assert sorted_jobs[2].job_id == "job1"  # latest


class TestRoundRobinAssignment:
    """Test round-robin job assignment."""
    
    def test_single_worker(self):
        """All jobs should go to single worker."""
        jobs = [
            Job("job1", priority=5, submitted_at="2024-02-13T10:00:00Z"),
            Job("job2", priority=5, submitted_at="2024-02-13T10:00:01Z"),
        ]
        workers = [Worker("worker-1")]
        
        assignments = assign_jobs_round_robin(jobs, workers, datetime.utcnow())
        
        assert len(assignments) == 2
        assert all(a.worker_id == "worker-1" for a in assignments)
    
    def test_multiple_workers(self):
        """Jobs should distribute across workers."""
        jobs = [
            Job(f"job{i}", priority=5, submitted_at=f"2024-02-13T10:00:0{i}Z")
            for i in range(4)
        ]
        workers = [Worker("worker-1"), Worker("worker-2")]
        
        assignments = assign_jobs_round_robin(jobs, workers, datetime.utcnow())
        
        worker1_jobs = [a for a in assignments if a.worker_id == "worker-1"]
        worker2_jobs = [a for a in assignments if a.worker_id == "worker-2"]
        
        assert len(worker1_jobs) == 2
        assert len(worker2_jobs) == 2
    
    def test_more_jobs_than_workers(self):
        """Should cycle through workers."""
        jobs = [
            Job(f"job{i}", priority=5, submitted_at=f"2024-02-13T10:00:0{i}Z")
            for i in range(5)
        ]
        workers = [Worker("worker-1"), Worker("worker-2")]
        
        assignments = assign_jobs_round_robin(jobs, workers, datetime.utcnow())
        
        assert len(assignments) == 5
        
        # First job to worker-1, second to worker-2, third to worker-1, etc.
        assert assignments[0].worker_id == "worker-1"
        assert assignments[1].worker_id == "worker-2"
        assert assignments[2].worker_id == "worker-1"
        assert assignments[3].worker_id == "worker-2"
        assert assignments[4].worker_id == "worker-1"


class TestScheduling:
    """Test main scheduling function."""
    
    def test_empty_jobs(self):
        """Should return empty assignments for no jobs."""
        policy = SchedulingPolicy()
        jobs = []
        workers = [Worker("worker-1")]
        
        assignments = schedule(policy, datetime.utcnow(), jobs, workers)
        
        assert assignments == []
    
    def test_empty_workers(self):
        """Should return empty assignments for no workers."""
        policy = SchedulingPolicy()
        jobs = [Job("job1", priority=5, submitted_at="2024-02-13T10:00:00Z")]
        workers = []
        
        assignments = schedule(policy, datetime.utcnow(), jobs, workers)
        
        assert assignments == []
    
    def test_priority_ordering(self):
        """High priority jobs should be scheduled first."""
        policy = SchedulingPolicy()
        jobs = [
            Job("low", priority=1, submitted_at="2024-02-13T10:00:00Z"),
            Job("high", priority=10, submitted_at="2024-02-13T10:00:01Z"),
            Job("medium", priority=5, submitted_at="2024-02-13T10:00:02Z"),
        ]
        workers = [Worker("worker-1"), Worker("worker-2")]
        
        assignments = schedule(policy, datetime.utcnow(), jobs, workers)
        
        # First assignment should be the high priority job
        assert assignments[0].job_id == "high"
    
    def test_max_assignments_limit(self):
        """Should respect max_assignments policy."""
        policy = SchedulingPolicy(max_assignments=2)
        jobs = [
            Job(f"job{i}", priority=5, submitted_at=f"2024-02-13T10:00:0{i}Z")
            for i in range(5)
        ]
        workers = [Worker("worker-1")]
        
        assignments = schedule(policy, datetime.utcnow(), jobs, workers)
        
        assert len(assignments) == 2


class TestMetrics:
    """Test scheduling metrics calculation."""
    
    def test_basic_metrics(self):
        """Should calculate correct metrics."""
        jobs = [
            Job("job1", priority=5, submitted_at="2024-02-13T10:00:00Z"),
            Job("job2", priority=10, submitted_at="2024-02-13T10:00:01Z"),
        ]
        assignments = [
            Assignment("job1", "worker-1", "2024-02-13T10:00:02Z"),
            Assignment("job2", "worker-1", "2024-02-13T10:00:02Z"),
        ]
        
        metrics = calculate_scheduling_metrics(assignments, jobs)
        
        assert metrics['jobs_scheduled'] == 2
        assert metrics['average_priority'] == 7.5
    
    def test_high_priority_count(self):
        """Should count high priority jobs."""
        jobs = [
            Job("job1", priority=8, submitted_at="2024-02-13T10:00:00Z"),
            Job("job2", priority=10, submitted_at="2024-02-13T10:00:01Z"),
            Job("job3", priority=5, submitted_at="2024-02-13T10:00:02Z"),
        ]
        assignments = [
            Assignment("job1", "worker-1", "2024-02-13T10:00:03Z"),
            Assignment("job2", "worker-1", "2024-02-13T10:00:03Z"),
            Assignment("job3", "worker-1", "2024-02-13T10:00:03Z"),
        ]
        
        metrics = calculate_scheduling_metrics(assignments, jobs)
        
        assert metrics['high_priority_count'] == 2  # priority >= 8
    
    def test_retry_count(self):
        """Should count retried jobs."""
        jobs = [
            Job("job1", priority=5, submitted_at="2024-02-13T10:00:00Z", retry_count=1),
            Job("job2", priority=5, submitted_at="2024-02-13T10:00:01Z", retry_count=0),
        ]
        assignments = [
            Assignment("job1", "worker-1", "2024-02-13T10:00:02Z"),
            Assignment("job2", "worker-1", "2024-02-13T10:00:02Z"),
        ]
        
        metrics = calculate_scheduling_metrics(assignments, jobs)
        
        assert metrics['retries_scheduled'] == 1


class TestBackoff:
    """Test backoff calculation."""
    
    def test_exponential_backoff(self):
        """Exponential backoff should double each time."""
        from scheduler.types import calculate_backoff
        
        config = BackoffConfig(
            strategy=BackoffStrategy.EXPONENTIAL,
            initial_delay_ms=1000,
            max_delay_ms=60000
        )
        
        assert calculate_backoff(config, 0) == 1000   # 1000 * 2^0
        assert calculate_backoff(config, 1) == 2000   # 1000 * 2^1
        assert calculate_backoff(config, 2) == 4000   # 1000 * 2^2
        assert calculate_backoff(config, 3) == 8000   # 1000 * 2^3
    
    def test_exponential_backoff_max(self):
        """Should cap at max_delay_ms."""
        from scheduler.types import calculate_backoff
        
        config = BackoffConfig(
            strategy=BackoffStrategy.EXPONENTIAL,
            initial_delay_ms=1000,
            max_delay_ms=5000
        )
        
        assert calculate_backoff(config, 10) == 5000  # Would be 1024000, capped at 5000
    
    def test_linear_backoff(self):
        """Linear backoff should increment linearly."""
        from scheduler.types import calculate_backoff
        
        config = BackoffConfig(
            strategy=BackoffStrategy.LINEAR,
            linear_increment_ms=5000
        )
        
        assert calculate_backoff(config, 0) == 0
        assert calculate_backoff(config, 1) == 5000
        assert calculate_backoff(config, 2) == 10000
    
    def test_no_backoff(self):
        """No backoff should always return 0."""
        from scheduler.types import calculate_backoff
        
        config = BackoffConfig(strategy=BackoffStrategy.NONE)
        
        assert calculate_backoff(config, 0) == 0
        assert calculate_backoff(config, 10) == 0
        assert calculate_backoff(config, 100) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
