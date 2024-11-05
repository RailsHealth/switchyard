from flask_pymongo import PyMongo
from flask_apscheduler import APScheduler
from authlib.integrations.flask_client import OAuth
from .celery_ext import celery, init_celery

mongo = PyMongo()
scheduler = APScheduler()
oauth = OAuth()

def init_extensions(app):
    """Initialize all Flask extensions"""
    mongo.init_app(app)
    oauth.init_app(app)
    
    # Configure APScheduler
    app.config['SCHEDULER_API_ENABLED'] = True
    
    if not scheduler.running:
        scheduler.init_app(app)
        scheduler.start()