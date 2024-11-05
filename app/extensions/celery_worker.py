# app/extensions/celery_worker.py (move to this location)

from app import create_app
from app.extensions.celery_ext import celery

# Create Flask app instance with specific import
app = create_app()

# Push application context
app.app_context().push()

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