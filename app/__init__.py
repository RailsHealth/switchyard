import os
import logging
from flask import Flask, render_template, g
from config import Config
from datetime import datetime
import uuid
from celery import Celery
from .extensions import mongo, oauth, init_extensions
from app.middleware import set_user_and_org_context, get_user_organizations, set_current_organization
from app.services.fhir_translator import FHIRTranslator
from app.services.fhir_validator import init_fhir_validator
from app.auth import init_oauth
from flask_wtf.csrf import CSRFProtect
from app.services.health_data_converter import init_health_data_converter
from app.services.fhir_service import initialize_fhir_interfaces
from logging.handlers import RotatingFileHandler

# Initialize logging
logger = logging.getLogger(__name__)

# Initialize Flask-WTF CSRF protection
csrf = CSRFProtect()

# Initialize Celery at module level
celery = Celery('app')

def init_celery(app=None):
    """Initialize Celery with Flask application context"""
    if app is None:
        return celery

    # Update celery config from Flask config
    celery.conf.update(app.config)

    # Configure Celery task execution context
    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)
        
        def on_failure(self, exc, task_id, args, kwargs, einfo):
            with app.app_context():
                app.logger.error(f'Celery task {task_id} failed: {exc}')
                super().on_failure(exc, task_id, args, kwargs, einfo)

    celery.Task = ContextTask

def setup_logging(app):
    """Configure application logging"""
    if not app.debug and not app.testing:
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.mkdir('logs')
        if not os.path.exists('logs/celery'):
            os.mkdir('logs/celery')
        if not os.path.exists('logs/streams'):
            os.mkdir('logs/streams')

        # Main application log
        file_handler = RotatingFileHandler(
            'logs/rails_health.log',
            maxBytes=10240000,
            backupCount=10
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)

        # Dedicated error log
        error_handler = RotatingFileHandler(
            'logs/error.log',
            maxBytes=10240000,
            backupCount=5
        )
        error_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        error_handler.setLevel(logging.ERROR)
        app.logger.addHandler(error_handler)

        app.logger.setLevel(logging.INFO)
        app.logger.info('Rails Health startup')

def init_mongodb(app):
    """Initialize MongoDB collections and indexes"""
    with app.app_context():
        # Core collections
        collections = [
            'users', 
            'organizations', 
            'parsing_logs',
            'validation_logs', 
            'message_cycle_logs',
            'streams_v2',
            'destination_messages',
            'delivery_logs',
            'activity_logs',
            'stream_event_logs',
            'failed_logs'
        ]
        
        for collection in collections:
            if collection not in mongo.db.list_collection_names():
                mongo.db.create_collection(collection)

        try:
            # Create/update indexes
            mongo.db.messages.create_index([("timestamp", -1)])
            mongo.db.messages.create_index([("uuid", 1)], unique=True)
            mongo.db.messages.create_index([("conversion_status", 1), ("type", 1)])
            mongo.db.messages.create_index([("organization_uuid", 1)])
            
            # Stream-related indexes
            mongo.db.destination_messages.create_index([
                ("status", 1),
                ("next_retry_at", 1)
            ])
            mongo.db.destination_messages.create_index([
                ("stream_uuid", 1),
                ("status", 1)
            ])
            mongo.db.destination_messages.create_index("original_message_uuid")
            
            # Logging indexes
            mongo.db.message_cycle_logs.create_index([
                ("message_uuid", 1),
                ("timestamp", 1)
            ])
            mongo.db.stream_event_logs.create_index([
                ("stream_uuid", 1),
                ("timestamp", 1)
            ])

        except Exception as e:
            logger.error(f"Error creating/updating indexes: {str(e)}")

def register_error_handlers(app):
    """Register application error handlers"""
    @app.errorhandler(404)
    def not_found_error(error):
        app.logger.error('404 error occurred: %s', str(error))
        return render_template('404.html'), 404

    @app.errorhandler(500)
    def internal_error(error):
        app.logger.error('500 error occurred: %s', str(error))
        return render_template('500.html'), 500

def create_default_stream():
    """Create default 'Pasted Messages' stream if it doesn't exist"""
    pasted_stream = mongo.db.streams.find_one({"name": "Pasted Messages"})
    if not pasted_stream:
        pasted_stream = {
            "uuid": str(uuid.uuid4()),
            "name": "Pasted Messages",
            "message_type": "Mixed",
            "active": True,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "deleted": False,
            "is_default": True,
            "files_processed": 0,
            "organization_uuid": None
        }
        mongo.db.streams.insert_one(pasted_stream)

def create_default_org_and_admins(app):
    """Create default organization and admin users"""
    from app.models.user import User
    from app.models.organization import Organization
    
    # Create default organization if it doesn't exist
    default_org = next((org for org in Organization.list_all() 
                       if org['name'] == app.config['DEFAULT_ORGANIZATION_NAME']), None)
    if not default_org:
        org_uuid = Organization.create(
            name=app.config['DEFAULT_ORGANIZATION_NAME'],
            org_type="Healthcare Provider"
        )
        logger.info(f"Created default organization: {app.config['DEFAULT_ORGANIZATION_NAME']}")
    else:
        org_uuid = default_org['uuid']

    # Update Pasted Messages stream with organization
    mongo.db.streams.update_one(
        {"name": "Pasted Messages"},
        {"$set": {"organization_uuid": org_uuid}}
    )

    # Set up admin users
    for email in app.config['ADMIN_PANEL_EMAILS']:
        user = User.get_by_email(email)
        if not user:
            user_uuid = User.create(
                google_id=None,
                name=email.split('@')[0],
                email=email,
                profile_picture_url=""
            )
            User.add_to_organization(user_uuid, org_uuid, role='admin')
            logger.info(f"Added admin user: {email}")

def create_app(config_class=Config):
    """Create and configure the Flask application"""
    app = Flask(__name__,
                template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates')),
                static_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'static')))
    
    # Load configuration
    app.config.from_object(config_class)

    # Setup logging first
    setup_logging(app)
    logger.info("Starting application initialization")

    try:
        # Initialize core extensions
        logger.info("Initializing core extensions")
        init_extensions(app)
        init_oauth(app)
        csrf.init_app(app)

        # Initialize MongoDB before Celery
        logger.info("Initializing MongoDB")
        init_mongodb(app)

        # Initialize Celery after MongoDB
        logger.info("Initializing Celery")
        from app.extensions.celery_ext import init_celery
        app.celery = init_celery(app)  # Store celery instance on app

        # Initialize Flask-Admin
        logger.info("Initializing Admin interface")
        with app.app_context():
            from .admin import init_admin
            init_admin(app)

        # Set up FHIR components
        logger.info("Initializing FHIR components")
        yaml_path = os.path.join(app.root_path, 'config', 'fhir_mapping.yaml')
        app.config['FHIR_MAPPING_FILE'] = yaml_path
        app.fhir_translator = FHIRTranslator(app.config['FHIR_MAPPING_FILE'])
        init_fhir_validator(app)

        # Initialize services within app context
        logger.info("Initializing application services")
        with app.app_context():
            init_health_data_converter(app, mongo)
            initialize_fhir_interfaces()

        # Initialize StreamsV2 after all base services
        logger.info("Initializing StreamsV2")
        from app.streamsv2 import init_app as init_streamsv2
        init_streamsv2(app)

        # Register all blueprints
        logger.info("Registering blueprints")
        from app.routes import (
            main, messages, logs, organizations, 
            accounts, templates, settings, endpoints, orgs
        )
        from app.auth import bp as auth_bp
        
        app.register_blueprint(main.bp)
        app.register_blueprint(auth_bp)
        app.register_blueprint(messages.bp, url_prefix='/messages')
        app.register_blueprint(logs.bp, url_prefix='/logs')
        app.register_blueprint(organizations.bp, url_prefix='/organizations')
        app.register_blueprint(accounts.bp, url_prefix='/accounts')
        app.register_blueprint(endpoints.bp, url_prefix='/endpoints')
        app.register_blueprint(templates.bp, url_prefix='/templates')
        app.register_blueprint(settings.bp, url_prefix='/settings')
        app.register_blueprint(orgs.bp, url_prefix='/orgs')

        # Register error handlers
        register_error_handlers(app)

        # Add middleware
        app.before_request(set_user_and_org_context)

        # Add context processor for templates
        @app.context_processor
        def inject_user_and_org():
            return dict(
                user=g.user,
                organization=g.organization,
                user_role=g.user_role,
                get_user_organizations=get_user_organizations,
                set_current_organization=set_current_organization
            )

        # Create default stream and organization
        logger.info("Setting up default stream and organization")
        with app.app_context():
            create_default_stream()
            create_default_org_and_admins(app)

        # Register Celery tasks AFTER all blueprints and services are initialized
        logger.info("Registering Celery tasks")
        with app.app_context():
            # First import all task modules
            from app.streamsv2.core import stream_tasks
            from app import tasks

            # Force Celery to discover tasks
            app.celery.autodiscover_tasks([
                'app.streamsv2.core',
                'app.tasks',
                'app.services'
            ], force=True)
            
            # Verify task registration
            registered_tasks = app.celery.tasks.keys()
            logger.info("Registered tasks: %s", registered_tasks)

            # Verify queue configuration
            queue_names = [q.name for q in app.celery.conf.task_queues]
            logger.info("Configured queues: %s", queue_names)

        logger.info("Application initialization completed successfully")
        return app

    except Exception as e:
        logger.error(f"Error during application initialization: {str(e)}", exc_info=True)
        raise
    
# Import tasks at the end to avoid circular imports
from app import tasks