import os
from celery import Celery
from flask import Flask
from config import Config
from app.extensions import mongo
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_celery_app(app=None):
    if app is None:
        app = Flask(__name__)
        app.config.from_object(Config)
        Config.init_app(app)
    
    # Initialize MongoDB
    mongo.init_app(app)

    # Ensure Celery configuration is set
    app.config['broker_url'] = app.config.get('broker_url') or Config.broker_url
    app.config['result_backend'] = app.config.get('result_backend') or Config.result_backend

    # Debug: Print Celery configuration
    logger.info("Celery configuration:")
    logger.info(f"broker_url: {app.config.get('broker_url')}")
    logger.info(f"result_backend: {app.config.get('result_backend')}")
    logger.info(f"All config keys: {app.config.keys()}")
    logger.info(f"CELERY_BROKER_URL from env: {os.environ.get('CELERY_BROKER_URL')}")
    logger.info(f"CELERY_RESULT_BACKEND from env: {os.environ.get('CELERY_RESULT_BACKEND')}")

    if not app.config.get('broker_url') or not app.config.get('result_backend'):
        logger.error("Celery broker_url or result_backend not set in configuration")
        raise ValueError("Celery broker_url or result_backend not set in configuration")

    celery = Celery(
        app.import_name,
        broker=app.config['broker_url'],
        backend=app.config['result_backend']
    )
    
    # Update Celery config
    celery.conf.update(app.config)
    celery.conf.update(
        task_serializer=app.config.get('task_serializer', 'json'),
        accept_content=app.config.get('accept_content', ['json']),
        result_serializer=app.config.get('result_serializer', 'json'),
        timezone=app.config.get('timezone', 'UTC'),
        enable_utc=app.config.get('enable_utc', True),
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        broker_connection_retry_on_startup=app.config.get('broker_connection_retry_on_startup', True),
        task_routes={
            'app.tasks.fetch_fhir_data': {'queue': app.config.get('FHIR_QUEUE', 'fhir_queue')},
            'app.tasks.process_pending_conversions': {'queue': app.config.get('CONVERSION_QUEUE', 'conversion_queue')},
            'app.tasks.parse_files': {'queue': app.config.get('FILE_QUEUE', 'file_queue')},
            'app.tasks.validate_fhir_messages': {'queue': app.config.get('VALIDATION_QUEUE', 'validation_queue')},
            'app.tasks.periodic_cleanup': {'queue': app.config.get('MAINTENANCE_QUEUE', 'maintenance_queue')},
            'app.tasks.log_conversion_metrics': {'queue': app.config.get('METRICS_QUEUE', 'metrics_queue')},
            'app.tasks.hl7v2_to_fhir_conversion': {'queue': app.config.get('HL7V2_CONVERSION_QUEUE', 'hl7v2_conversion')},
            'app.tasks.process_hl7v2_messages': {'queue': app.config.get('HL7V2_CONVERSION_QUEUE', 'hl7v2_conversion')},
            'app.tasks.cleanup_old_messages': {'queue': app.config.get('MAINTENANCE_QUEUE', 'maintenance_queue')},
            'app.tasks.check_stuck_messages': {'queue': app.config.get('MAINTENANCE_QUEUE', 'maintenance_queue')}
        }
    )

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery

def init_celery(app):
    celery = create_celery_app(app)
    return celery

# Create Flask app
flask_app = Flask(__name__)
flask_app.config.from_object(Config)
Config.init_app(flask_app)

# Initialize MongoDB
mongo.init_app(flask_app)

# Create Celery app
try:
    celery = create_celery_app(flask_app)
    logger.info("Celery app created successfully")
except Exception as e:
    logger.error(f"Failed to create Celery app: {str(e)}")
    raise

# Import tasks at the end to avoid circular imports
try:
    from app import tasks
    logger.info("Tasks imported successfully")
except ImportError as e:
    logger.error(f"Failed to import tasks: {str(e)}")
    raise

if __name__ == '__main__':
    # This block allows you to run celery workers directly from this file
    celery.start()