"""
HTTP API server for the job scheduler.

This module provides a Flask-based REST API that receives scheduling
requests and returns job assignments.
"""

from flask import Flask, request, jsonify
from datetime import datetime
from typing import Dict, Any
import logging

from .types import (
    Job, Worker, Assignment, ScheduleRequest, ScheduleResponse,
    SchedulingPolicy, BackoffConfig, BackoffStrategy
)
from .algorithm import schedule, calculate_scheduling_metrics


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def create_app(config: Dict[str, Any] = None) -> Flask:
    """
    Create and configure the Flask application.
    
    Args:
        config: Optional configuration dictionary
        
    Returns:
        Configured Flask app
    """
    app = Flask(__name__)
    
    # Default configuration
    app.config.update({
        'TESTING': False,
        'MAX_ASSIGNMENTS': 100,
        'BACKOFF_STRATEGY': 'exponential',
        'INITIAL_DELAY_MS': 1000,
        'MAX_DELAY_MS': 60000,
    })
    
    # Apply custom config
    if config:
        app.config.update(config)
    
    # Create default policy
    backoff = BackoffConfig(
        strategy=BackoffStrategy(app.config['BACKOFF_STRATEGY']),
        initial_delay_ms=app.config['INITIAL_DELAY_MS'],
        max_delay_ms=app.config['MAX_DELAY_MS']
    )
    
    policy = SchedulingPolicy(
        backoff=backoff,
        max_assignments=app.config['MAX_ASSIGNMENTS']
    )
    
    @app.route('/health', methods=['GET'])
    def health():
        """Health check endpoint."""
        return jsonify({
            'status': 'healthy',
            'service': 'job-scheduler',
            'version': '0.1.0',
            'timestamp': datetime.utcnow().isoformat()
        })
    
    @app.route('/schedule', methods=['POST'])
    def schedule_jobs():
        """
        Schedule jobs across available workers.
        
        Request body:
        {
            "type": "schedule_request",
            "pending_jobs": [
                {
                    "job_id": "uuid",
                    "priority": 5,
                    "submitted_at": "2024-02-13T10:00:00Z",
                    "retry_count": 0
                }
            ],
            "available_workers": [
                {"worker_id": "worker-1"}
            ]
        }
        
        Response:
        {
            "type": "schedule_response",
            "assignments": [
                {
                    "job_id": "uuid",
                    "worker_id": "worker-1",
                    "scheduled_at": "2024-02-13T10:00:01Z"
                }
            ]
        }
        """
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({'error': 'Empty request body'}), 400
            
            # Parse jobs
            jobs = []
            for job_data in data.get('pending_jobs', []):
                try:
                    job = Job(
                        job_id=job_data['job_id'],
                        priority=job_data['priority'],
                        submitted_at=job_data['submitted_at'],
                        retry_count=job_data.get('retry_count', 0)
                    )
                    jobs.append(job)
                except (KeyError, ValueError) as e:
                    logger.error(f"Invalid job data: {e}")
                    return jsonify({'error': f'Invalid job data: {e}'}), 400
            
            # Parse workers
            workers = []
            for worker_data in data.get('available_workers', []):
                try:
                    worker = Worker(worker_id=worker_data['worker_id'])
                    workers.append(worker)
                except KeyError as e:
                    logger.error(f"Invalid worker data: {e}")
                    return jsonify({'error': f'Invalid worker data: {e}'}), 400
            
            # Run scheduling algorithm
            current_time = datetime.utcnow()
            assignments = schedule(policy, current_time, jobs, workers)
            
            # Calculate metrics
            metrics = calculate_scheduling_metrics(assignments, jobs)
            
            logger.info(f"Scheduled {len(assignments)} jobs across {len(workers)} workers")
            logger.info(f"Metrics: {metrics}")
            
            # Build response
            response = {
                'type': 'schedule_response',
                'assignments': [
                    {
                        'job_id': a.job_id,
                        'worker_id': a.worker_id,
                        'scheduled_at': a.scheduled_at
                    }
                    for a in assignments
                ],
                'metrics': metrics
            }
            
            return jsonify(response), 200
            
        except Exception as e:
            logger.error(f"Error scheduling jobs: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    @app.route('/policy', methods=['GET'])
    def get_policy():
        """Get current scheduling policy."""
        return jsonify({
            'backoff': {
                'strategy': policy.backoff.strategy.value,
                'initial_delay_ms': policy.backoff.initial_delay_ms,
                'max_delay_ms': policy.backoff.max_delay_ms
            },
            'max_assignments': policy.max_assignments
        })
    
    @app.errorhandler(404)
    def not_found(error):
        """Handle 404 errors."""
        return jsonify({'error': 'Not found'}), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        """Handle 500 errors."""
        logger.error(f"Internal server error: {error}")
        return jsonify({'error': 'Internal server error'}), 500
    
    return app


def run_server(host: str = '0.0.0.0', port: int = 8001, debug: bool = False):
    """
    Run the scheduler HTTP server.
    
    Args:
        host: Host to bind to
        port: Port to listen on
        debug: Enable debug mode
    """
    logger.info("=" * 50)
    logger.info("  Job Scheduler Server (Python)")
    logger.info("=" * 50)
    logger.info("")
    logger.info(f"Starting server on {host}:{port}")
    logger.info("")
    logger.info("Endpoints:")
    logger.info(f"  POST {host}:{port}/schedule - Schedule jobs")
    logger.info(f"  GET  {host}:{port}/health   - Health check")
    logger.info(f"  GET  {host}:{port}/policy   - Get policy")
    logger.info("")
    
    app = create_app()
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_server()
