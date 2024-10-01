from flask import Flask, render_template, g
from config import Config
import os
import logging
from app.services.fhir_translator import FHIRTranslator
import uuid
from datetime import datetime
from .extensions import mongo, scheduler, oauth, init_extensions
from app.middleware import set_user_and_org_context, get_user_organizations, set_current_organization
from app.services.fhir_validator import init_fhir_validator
from app.auth import init_oauth
from flask_wtf.csrf import CSRFProtect
from app.services.health_data_converter import init_conversion_scheduler


csrf = CSRFProtect()

def create_app(config_class=Config):
    app = Flask(__name__,
                template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates')),
                static_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'static')))
    app.config.from_object(config_class)

    init_extensions(app)
    init_oauth(app)
    csrf.init_app(app)

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

    # Register blueprints
    from app.routes import main, streams, messages, logs, organizations, accounts
    from app.auth import bp as auth_bp
    app.register_blueprint(main.bp)
    app.register_blueprint(streams.bp, url_prefix='/streams')
    app.register_blueprint(messages.bp, url_prefix='/messages')
    app.register_blueprint(logs.bp, url_prefix='/logs')
    app.register_blueprint(organizations.bp, url_prefix='/organizations')
    app.register_blueprint(auth_bp, url_prefix='/auth')
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
        file_handler = logging.FileHandler(filename='app.log')
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)
        app.logger.setLevel(logging.INFO)
        app.logger.info('Rails Health startup')

    # Initialize the conversion scheduler
    from app.services.health_data_converter import init_conversion_scheduler
    init_conversion_scheduler(app)

    # Schedule other tasks
    schedule_tasks(app)

    return app

def schedule_tasks(app):
    @scheduler.task('interval', id='fetch_fhir_data', seconds=600, misfire_grace_time=900)
    def fetch_fhir_data():
        with app.app_context():
            from app.routes.streams import fhir_interfaces
            app.logger.info("Starting scheduled FHIR data fetch")
            for stream_uuid, interface in fhir_interfaces.items():
                if interface.listening:
                    app.logger.info(f"Fetching data for FHIR stream: {stream_uuid}")
                    interface.fetch_data()
            app.logger.info("Completed scheduled FHIR data fetch")

    @scheduler.task('interval', id='parse_files', seconds=30, misfire_grace_time=300)
    def parse_files():
        with app.app_context():
            from app.services.file_parser_service import FileParserService
            app.logger.info("Starting file parsing process")
            FileParserService.process_pending_files()
            app.logger.info("Completed file parsing process")

    @scheduler.task('interval', id='validate_fhir_messages', seconds=300, misfire_grace_time=900)
    def validate_fhir_messages():
        with app.app_context():
            from app.services.fhir_validator import FHIRValidator
            app.logger.info("Starting FHIR message validation process")
            FHIRValidator.validate_fhir_messages()
            app.logger.info("Completed FHIR message validation process")

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