import os
import logging
from dotenv import load_dotenv

load_dotenv()

print(f"CELERY_BROKER_URL from env: {os.environ.get('CELERY_BROKER_URL')}")
print(f"CELERY_RESULT_BACKEND from env: {os.environ.get('CELERY_RESULT_BACKEND')}")

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    MONGO_URI = os.environ.get('MONGO_URI') or 'mongodb://localhost:27017/hl7_db'
    DB_NAME = os.environ.get('DB_NAME') or 'hl7_db'
    DEBUG = os.environ.get('FLASK_DEBUG') == 'True'
    PORT = int(os.environ.get('PORT', 5001))

    # Redis configuration
    REDIS_URL = os.environ.get('REDIS_URL') or 'redis://localhost:6379/0'

    # Celery Configuration
    broker_url = os.environ.get('CELERY_BROKER_URL') or REDIS_URL
    result_backend = os.environ.get('CELERY_RESULT_BACKEND') or REDIS_URL
    task_serializer = 'json'
    accept_content = ['json']
    result_serializer = 'json'
    timezone = 'UTC'
    enable_utc = True
    broker_connection_retry_on_startup = True

    # Celery Queue Names
    FHIR_QUEUE = 'fhir_queue'
    CONVERSION_QUEUE = 'conversion_queue'
    FILE_QUEUE = 'file_queue'
    VALIDATION_QUEUE = 'validation_queue'
    MAINTENANCE_QUEUE = 'maintenance_queue'
    METRICS_QUEUE = 'metrics_queue'
    HL7V2_CONVERSION_QUEUE = 'hl7v2_conversion'

    # Task Intervals (in seconds)
    FETCH_FHIR_INTERVAL = 600
    PROCESS_PENDING_CONVERSIONS_INTERVAL = 60
    PARSE_FILES_INTERVAL = 30
    VALIDATE_FHIR_MESSAGES_INTERVAL = 300
    PERIODIC_CLEANUP_INTERVAL = 3600
    LOG_CONVERSION_METRICS_INTERVAL = 900
    PROCESS_HL7V2_MESSAGES_INTERVAL = 60
    CLEANUP_OLD_MESSAGES_INTERVAL = 3600
    CHECK_STUCK_MESSAGES_INTERVAL = 900
    SCHEDULED_MAINTENANCE_INTERVAL = 86400  # 24 hours
    REFRESH_FHIR_INTERFACES_INTERVAL = 3600  # 1 hour

    # OAuth configurations
    GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
    GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')

    # FHIR server configurations
    FHIR_SERVERS = {
        'firely': {
            'name': 'Firely Test Server',
            'url': 'https://server.fire.ly',
            'version': 'R4'
        },
        'hapi': {
            'name': 'HAPI FHIR Test Server',
            'url': 'http://hapi.fhir.org/baseR4',
            'version': 'R4'
        }
    }

    # Conversion API URLs
    HL7_TO_FHIR_API_URL = os.environ.get('HL7_TO_FHIR_API_URL')
    NOTES_TO_FHIR_API_URL = os.environ.get('NOTES_TO_FHIR_API_URL')
    CCDA_TO_FHIR_API_URL = os.environ.get('CCDA_TO_FHIR_API_URL')
    X12_TO_FHIR_API_URL = os.environ.get('X12_TO_FHIR_API_URL')

    # Storage settings
    STORAGE_TYPE = os.environ.get('STORAGE_TYPE', 'local')
    STORAGE_PATH = os.environ.get('STORAGE_PATH', '/path/to/datastore')

    # SFTP settings
    SFTP_RETRY_ATTEMPTS = 3
    SFTP_RETRY_DELAY = 5  # seconds
    DEFAULT_SFTP_FETCH_INTERVAL = 600  # 10 minutes in seconds

    # Message types
    MESSAGE_TYPES = [
        'HL7v2',
        'FHIR',
        'CCDA',
        'X12',
        'Clinical Notes'
    ]

    # Mapping of stream types to message types
    STREAM_TO_MESSAGE_TYPE = {
        'HL7v2': 'HL7v2',
        'FHIR Test Server': 'FHIR',
        'CCDA SFTP': 'CCDA',
        'X12 SFTP': 'X12',
        'Clinical Notes SFTP': 'Clinical Notes'
    }

    # Session configuration
    SESSION_TYPE = 'filesystem'
    SESSION_PERMANENT = False
    SESSION_USE_SIGNER = True
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'

    # CSRF protection
    WTF_CSRF_ENABLED = True
    WTF_CSRF_SECRET_KEY = os.environ.get('WTF_CSRF_SECRET_KEY') or 'csrf-secret-key'

    # Logging configuration
    LOG_TO_STDOUT = os.environ.get('LOG_TO_STDOUT')
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

    # Organization types
    ORGANIZATION_TYPES = [
        'Healthcare Provider',
        'Payer',
        'Health IT Vendor',
        'Research Institution',
        'Government Agency',
        'Other'
    ]

    # User roles within organizations
    USER_ROLES = ['admin', 'viewer']

    # Admin panel access
    ADMIN_PANEL_EMAILS = os.environ.get('ADMIN_PANEL_EMAILS', 'admin1@example.com,admin2@example.com,admin3@example.com').split(',')

    # Default organization name
    DEFAULT_ORGANIZATION_NAME = os.environ.get('DEFAULT_ORGANIZATION_NAME', 'Rails Health')

    # FHIR Validation
    FHIR_VALIDATION_ENABLED = os.environ.get('FHIR_VALIDATION_ENABLED', 'True') == 'True'
    FHIR_VALIDATION_SERVER = os.environ.get('FHIR_VALIDATION_SERVER', 'http://hapi.fhir.org/baseR4')

    # Rate limiting
    RATELIMIT_ENABLED = True
    RATELIMIT_STORAGE_URL = REDIS_URL
    RATELIMIT_DEFAULT = "200 per day;50 per hour;1 per second"

    @classmethod
    def print_celery_config(cls):
        print(f"broker_url in Config: {cls.broker_url}")
        print(f"result_backend in Config: {cls.result_backend}")

    @classmethod
    def init_app(cls, app):
        app.config['broker_url'] = cls.broker_url
        app.config['result_backend'] = cls.result_backend

        # Set up logging
        if not app.debug and not app.testing:
            if cls.LOG_TO_STDOUT:
                stream_handler = logging.StreamHandler()
                stream_handler.setLevel(logging.INFO)
                app.logger.addHandler(stream_handler)
            else:
                if not os.path.exists('logs'):
                    os.mkdir('logs')
                file_handler = RotatingFileHandler('logs/rails_health.log', maxBytes=10240, backupCount=10)
                file_handler.setFormatter(logging.Formatter(
                    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
                ))
                file_handler.setLevel(logging.INFO)
                app.logger.addHandler(file_handler)

        app.logger.setLevel(logging.INFO)
        app.logger.info('Rails Health startup')

Config.print_celery_config()