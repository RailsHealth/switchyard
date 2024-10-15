from celery import Celery
from flask import Flask
from config import Config
from app.extensions import mongo

def create_celery_app(app=None):
    if app is None:
        app = Flask(__name__)
        app.config.from_object(Config)
        
    # Initialize MongoDB
    mongo.init_app(app)

    celery = Celery(
        app.import_name,
        broker=app.config.get('broker_url', 'redis://localhost:6379/0'),
        backend=app.config.get('result_backend', 'redis://localhost:6379/0')
    )
    
    # Update Celery config
    celery.conf.update(app.config)
    celery.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        task_routes={
            'app.tasks.hl7v2_to_fhir_conversion': {'queue': 'hl7v2_conversion'},
            'app.tasks.process_hl7v2_messages': {'queue': 'hl7v2_conversion'},
            'app.tasks.cleanup_old_messages': {'queue': 'maintenance'},
            'app.tasks.check_stuck_messages': {'queue': 'maintenance'}
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

flask_app = Flask(__name__)
flask_app.config.from_object(Config)
mongo.init_app(flask_app)
celery = create_celery_app(flask_app)

# Import tasks at the end to avoid circular imports
from app import tasks