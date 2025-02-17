import os
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from app import create_app
from app.extensions.celery_ext import celery
from celery.signals import (
    after_setup_logger,
    after_setup_task_logger,
    task_prerun,
    task_postrun,
    task_failure,
    task_retry,
    task_success,
    worker_ready,
    worker_shutting_down,
    worker_init,
    celeryd_after_setup
)
from celery.utils.log import get_task_logger
import json
from datetime import datetime

# Create Flask app instance
app = create_app()

# Push application context
app_context = app.app_context()
app_context.push()

# Set up logging directories
if not os.path.exists('logs'):
    os.makedirs('logs')
if not os.path.exists('logs/streams'):
    os.makedirs('logs/streams')

# Configure Celery task execution with explicit logging
app.logger.info("Configuring Celery worker")

# Configure logging formats
MAIN_LOG_FORMAT = '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
TASK_LOG_FORMAT = '%(asctime)s - %(task_name)s[%(task_id)s] - [%(levelname)s] - %(message)s'
STREAM_LOG_FORMAT = '%(asctime)s - Stream[%(stream_uuid)s] - [%(levelname)s] - %(message)s'

class ContextTask(celery.Task):
    """Enhanced Task class with better logging and context management"""
    
    def __call__(self, *args, **kwargs):
        with app.app_context():
            # Log task start with arguments
            task_args = {
                'task_id': self.request.id,
                'task_name': self.name,
                'args': args,
                'kwargs': kwargs
            }
            app.logger.info(f'Starting task execution: {json.dumps(task_args, default=str)}')
            
            try:
                return self.run(*args, **kwargs)
            except Exception as e:
                app.logger.error(f'Task execution failed: {str(e)}', exc_info=True)
                raise
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        with app.app_context():
            error_details = {
                'task_id': task_id,
                'exception': str(exc),
                'args': args,
                'kwargs': kwargs,
                'traceback': str(einfo)
            }
            app.logger.error(f'Task failure details: {json.dumps(error_details, default=str)}')
            super().on_failure(exc, task_id, args, kwargs, einfo)
    
    def on_retry(self, exc, task_id, args, kwargs, einfo):
        with app.app_context():
            retry_details = {
                'task_id': task_id,
                'exception': str(exc),
                'args': args,
                'kwargs': kwargs,
                'traceback': str(einfo)
            }
            app.logger.warning(f'Task retry details: {json.dumps(retry_details, default=str)}')
            super().on_retry(exc, task_id, args, kwargs, einfo)

celery.Task = ContextTask

# Celery signal handlers
@after_setup_logger.connect
def setup_logger(logger, *args, **kwargs):
    """Configure main Celery logger"""
    formatter = logging.Formatter(MAIN_LOG_FORMAT)
    
    # Main log file
    fh = RotatingFileHandler(
        'logs/celery_worker.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=10
    )
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    
    # Error log file
    error_fh = RotatingFileHandler(
        'logs/celery_worker_error.log',
        maxBytes=10 * 1024 * 1024,
        backupCount=10
    )
    error_fh.setFormatter(formatter)
    error_fh.setLevel(logging.ERROR)
    logger.addHandler(error_fh)
    
    # Debug log file
    debug_fh = RotatingFileHandler(
        'logs/celery_worker_debug.log',
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )
    debug_fh.setFormatter(formatter)
    debug_fh.setLevel(logging.DEBUG)
    logger.addHandler(debug_fh)

@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    """Configure task-specific logger"""
    formatter = logging.Formatter(TASK_LOG_FORMAT)
    
    # Task-specific log file
    fh = RotatingFileHandler(
        'logs/celery_tasks.log',
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

def setup_stream_logger(stream_uuid: str):
    """Configure stream-specific logger"""
    logger = logging.getLogger(f'stream.{stream_uuid}')
    formatter = logging.Formatter(STREAM_LOG_FORMAT)
    
    # Stream-specific log file
    fh = TimedRotatingFileHandler(
        f'logs/streams/stream_{stream_uuid}.log',
        when='midnight',
        interval=1,
        backupCount=7
    )
    fh.setFormatter(formatter)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    
    return logger

@task_prerun.connect
def task_prerun_handler(task_id, task, *args, **kwargs):
    """Handle task pre-run events"""
    with app.app_context():
        # Extract stream_uuid if present in task kwargs
        stream_uuid = None
        if hasattr(task, 'request') and task.request.kwargs:
            stream_uuid = task.request.kwargs.get('stream_uuid')
        
        log_data = {
            'task_id': task_id,
            'task_name': task.name,
            'args': args,
            'kwargs': kwargs,
            'stream_uuid': stream_uuid
        }
        app.logger.info(f'Task starting: {json.dumps(log_data, default=str)}')

        # Set up stream-specific logging if applicable
        if stream_uuid and task.name.startswith('streamsv2.'):
            setup_stream_logger(stream_uuid)

@task_postrun.connect
def task_postrun_handler(task_id, task, *args, retval=None, state=None, **kwargs):
    """Handle task post-run events"""
    with app.app_context():
        log_data = {
            'task_id': task_id,
            'task_name': task.name,
            'state': state,
            'return_value': str(retval)[:200] if retval else None  # Truncate long results
        }
        app.logger.info(f'Task completed: {json.dumps(log_data, default=str)}')

@task_failure.connect
def task_failure_handler(task_id, exception, traceback, *args, **kwargs):
    """Handle task failure events"""
    with app.app_context():
        error_data = {
            'task_id': task_id,
            'exception': str(exception),
            'traceback': str(traceback),
            'args': args,
            'timestamp': datetime.utcnow().isoformat()
        }
        app.logger.error(f'Task failed: {json.dumps(error_data, default=str)}')

@task_retry.connect
def task_retry_handler(request, reason, einfo, **kwargs):
    """Handle task retry events"""
    with app.app_context():
        retry_data = {
            'task_id': request.id,
            'task_name': request.task.name,
            'reason': str(reason),
            'error_info': str(einfo),
            'retry_count': request.retries
        }
        app.logger.warning(f'Task retry initiated: {json.dumps(retry_data, default=str)}')

@task_success.connect
def task_success_handler(sender=None, **kwargs):
    """Handle task success events"""
    with app.app_context():
        if sender and hasattr(sender, 'request'):
            success_data = {
                'task_id': sender.request.id,
                'task_name': sender.name,
                'runtime': kwargs.get('runtime')
            }
            app.logger.info(f'Task succeeded: {json.dumps(success_data, default=str)}')

@worker_init.connect
def worker_init_handler(**kwargs):
    """Handle worker initialization"""
    app.logger.info('Initializing Celery worker')

@worker_ready.connect
def worker_ready_handler(**kwargs):
    """Handle worker ready event"""
    app.logger.info('Celery worker is ready and accepting tasks')

@worker_shutting_down.connect
def worker_shutting_down_handler(**kwargs):
    """Handle worker shutdown event"""
    app.logger.info('Celery worker is shutting down')

@celeryd_after_setup.connect
def setup_direct_queue(sender, instance, **kwargs):
    """Setup worker-specific queues"""
    app.logger.info(f'Worker {sender} setup completed')

if __name__ == '__main__':
    celery.start()