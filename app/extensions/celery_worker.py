import os
import logging
from logging.handlers import RotatingFileHandler
from app import create_app
from app.extensions.celery_ext import celery
from celery.signals import (
    after_setup_logger,
    after_setup_task_logger,
    task_prerun,
    task_postrun,
    task_failure,
    worker_ready,
    worker_shutting_down
)

# Create Flask app instance
app = create_app()

# Push application context
app_context = app.app_context()
app_context.push()

# Configure Celery task execution with explicit logging
app.logger.info("Configuring Celery worker")

class ContextTask(celery.Task):
    def __call__(self, *args, **kwargs):
        with app.app_context():
            return self.run(*args, **kwargs)
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        with app.app_context():
            app.logger.error(f'Celery task {task_id} failed: {exc}')
            super().on_failure(exc, task_id, args, kwargs, einfo)

celery.Task = ContextTask

# Celery signal handlers
@after_setup_logger.connect
def setup_logger(logger, *args, **kwargs):
    """Configure Celery logger"""
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # File handler for general logs
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    fh = RotatingFileHandler(
        'logs/celery_worker.log',
        maxBytes=10000000,
        backupCount=5
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # Separate error log
    error_fh = RotatingFileHandler(
        'logs/celery_worker_error.log',
        maxBytes=10000000,
        backupCount=5
    )
    error_fh.setFormatter(formatter)
    error_fh.setLevel(logging.ERROR)
    logger.addHandler(error_fh)

@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    """Configure task-specific logger"""
    setup_logger(logger, *args, **kwargs)

@task_prerun.connect
def task_prerun_handler(task_id, task, *args, **kwargs):
    """Handle task pre-run events"""
    with app.app_context():
        app.logger.info(f'Task {task.name}[{task_id}] starting')

@task_postrun.connect
def task_postrun_handler(task_id, task, *args, retval=None, state=None, **kwargs):
    """Handle task post-run events"""
    with app.app_context():
        app.logger.info(f'Task {task.name}[{task_id}] completed with state: {state}')

@task_failure.connect
def task_failure_handler(task_id, exception, traceback, *args, **kwargs):
    """Handle task failure events"""
    with app.app_context():
        app.logger.error(
            f'Task {task_id} failed: {str(exception)}\nTraceback:\n{traceback}'
        )

@worker_ready.connect
def worker_ready_handler(**kwargs):
    """Handle worker ready event"""
    app.logger.info('Celery worker is ready')

@worker_shutting_down.connect
def worker_shutting_down_handler(**kwargs):
    """Handle worker shutdown event"""
    app.logger.info('Celery worker is shutting down')

if __name__ == '__main__':
    celery.start()