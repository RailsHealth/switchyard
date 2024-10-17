import os
import logging
from flask import Flask, render_template, g
from config import Config
from datetime import datetime
import uuid
from .extensions import mongo, oauth, init_extensions
from app.middleware import set_user_and_org_context, get_user_organizations, set_current_organization
from app.services.fhir_translator import FHIRTranslator
from app.services.fhir_validator import init_fhir_validator
from app.auth import init_oauth
from flask_wtf.csrf import CSRFProtect
from app.services.health_data_converter import init_health_data_converter
from app.services.fhir_service import initialize_fhir_interfaces
from app.celery_app import celery, init_celery

csrf = CSRFProtect()

def create_app(config_class=Config):
    app = Flask(__name__,
                template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates')),
                static_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'static')))
    app.config.from_object(config_class)

    # Initialize extensions
    init_extensions(app)
    init_oauth(app)
    csrf.init_app(app)

    # Initialize Celery
    init_celery(app)

    # Set Celery beat schedule
    celery.conf.beat_schedule = {
        'process-hl7v2-messages': {
            'task': 'app.tasks.process_hl7v2_messages',
            'schedule': app.config['PROCESS_HL7V2_MESSAGES_INTERVAL'],
            'args': (app.config, app.name)
        },
        'cleanup-old-messages': {
            'task': 'app.tasks.cleanup_old_messages',
            'schedule': app.config['CLEANUP_OLD_MESSAGES_INTERVAL'],
            'args': (30,)  # Delete messages older than 30 days
        },
        'check-stuck-messages': {
            'task': 'app.tasks.check_stuck_messages',
            'schedule': app.config['CHECK_STUCK_MESSAGES_INTERVAL'],
        },
        'fetch-fhir-data': {
            'task': 'app.tasks.fetch_fhir_data',
            'schedule': app.config['FETCH_FHIR_INTERVAL'],
        },
        'process-pending-conversions': {
            'task': 'app.tasks.process_pending_conversions',
            'schedule': app.config['PROCESS_PENDING_CONVERSIONS_INTERVAL'],
        },
        'parse-files': {
            'task': 'app.tasks.parse_files',
            'schedule': app.config['PARSE_FILES_INTERVAL'],
        },
        'validate-fhir-messages': {
            'task': 'app.tasks.validate_fhir_messages',
            'schedule': app.config['VALIDATE_FHIR_MESSAGES_INTERVAL'],
        },
        'log-conversion-metrics': {
            'task': 'app.tasks.log_conversion_metrics',
            'schedule': app.config['LOG_CONVERSION_METRICS_INTERVAL'],
        },
        'scheduled-maintenance': {
            'task': 'app.tasks.scheduled_maintenance',
            'schedule': app.config['SCHEDULED_MAINTENANCE_INTERVAL'],
        },
        'refresh-fhir-interfaces': {
            'task': 'app.tasks.refresh_fhir_interfaces',
            'schedule': app.config['REFRESH_FHIR_INTERFACES_INTERVAL'],
        },
    }

    # Initialize Flask-Admin
    with app.app_context():
        from .admin import init_admin
        init_admin(app)

    # Set FHIR_MAPPING_FILE in app config
    yaml_path = os.path.join(app.root_path, 'config', 'fhir_mapping.yaml')
    app.config['FHIR_MAPPING_FILE'] = yaml_path

    # Initialize FHIRTranslator
    app.fhir_translator = FHIRTranslator(app.config['FHIR_MAPPING_FILE'])

    # Initialize FHIRValidator
    init_fhir_validator(app)

    # Initialize the health data converter
    init_health_data_converter(app, mongo)

    # Initialize FHIR interfaces
    initialize_fhir_interfaces()

    # Register blueprints
    from app.routes import main, streams, messages, logs, organizations, accounts
    from app.auth import bp as auth_bp
    app.register_blueprint(main.bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(streams.bp, url_prefix='/streams')
    app.register_blueprint(messages.bp, url_prefix='/messages')
    app.register_blueprint(logs.bp, url_prefix='/logs')
    app.register_blueprint(organizations.bp, url_prefix='/organizations')
    app.register_blueprint(accounts.bp, url_prefix='/accounts')

    # Initialize MongoDB collections
    with app.app_context():
        if 'users' not in mongo.db.list_collection_names():
            mongo.db.create_collection('users')
        if 'organizations' not in mongo.db.list_collection_names():
            mongo.db.create_collection('organizations')
        if 'parsing_logs' not in mongo.db.list_collection_names():
            mongo.db.create_collection('parsing_logs')
        if 'validation_logs' not in mongo.db.list_collection_names():
            mongo.db.create_collection('validation_logs')
        mongo.db.streams.update_many(
            {"files_processed": {"$exists": False}},
            {"$set": {"files_processed": 0}}
        )

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

    # Create "Pasted Messages" stream if it doesn't exist
    with app.app_context():
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
                "organization_uuid": None  # This will be set when creating default org
            }
            mongo.db.streams.insert_one(pasted_stream)

    # Create default organization and add admin panel users
    create_default_org_and_admins(app)

    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        app.logger.error('404 error occurred: %s', str(error))
        return render_template('404.html'), 404

    @app.errorhandler(500)
    def internal_error(error):
        app.logger.error('500 error occurred: %s', str(error))
        return render_template('500.html'), 500

    # Logging setup
    if not app.debug and not app.testing:
        if not os.path.exists('logs'):
            os.mkdir('logs')
        file_handler = logging.FileHandler('logs/rails_health.log')
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)

    app.logger.setLevel(logging.INFO)
    app.logger.info('Rails Health startup')

    return app

def create_default_org_and_admins(app):
    from app.models.user import User
    from app.models.organization import Organization
    
    with app.app_context():
        # Create default organization if it doesn't exist
        default_org = next((org for org in Organization.list_all() if org['name'] == app.config['DEFAULT_ORGANIZATION_NAME']), None)
        if not default_org:
            org_uuid = Organization.create(
                name=app.config['DEFAULT_ORGANIZATION_NAME'],
                org_type="Healthcare Provider"
            )
            app.logger.info(f"Created default organization: {app.config['DEFAULT_ORGANIZATION_NAME']}")
        else:
            org_uuid = default_org['uuid']

        # Update "Pasted Messages" stream with the default organization UUID
        mongo.db.streams.update_one(
            {"name": "Pasted Messages"},
            {"$set": {"organization_uuid": org_uuid}}
        )

        # Add admin panel users
        for email in app.config['ADMIN_PANEL_EMAILS']:
            user = User.get_by_email(email)
            if not user:
                user_uuid = User.create(
                    google_id=None,  # This will be set when the user first logs in
                    name=email.split('@')[0],  # Use part before @ as name
                    email=email,
                    profile_picture_url=""
                )
                User.add_to_organization(user_uuid, org_uuid, role='admin')
                app.logger.info(f"Added admin panel user: {email}")
            elif org_uuid not in [org['uuid'] for org in user.get('organizations', [])]:
                User.add_to_organization(user['uuid'], org_uuid, role='admin')
                app.logger.info(f"Added existing user {email} to default organization as admin")

# Import tasks here to avoid circular imports
from app import tasks