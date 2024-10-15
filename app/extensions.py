from flask_pymongo import PyMongo
from flask_apscheduler import APScheduler
from authlib.integrations.flask_client import OAuth
from celery import Celery

mongo = PyMongo()
scheduler = APScheduler()
oauth = OAuth()
celery = Celery(__name__)

def init_extensions(app):
    mongo.init_app(app)
    oauth.init_app(app)
    
    # Configure APScheduler
    app.config['SCHEDULER_API_ENABLED'] = True
    
    if not scheduler.running:
        scheduler.init_app(app)
        scheduler.start()

    # Configure Celery
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask