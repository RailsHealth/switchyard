import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    MONGO_URI = os.environ.get('MONGO_URI') or 'mongodb://localhost:27017/hl7_db'
    DB_NAME = os.environ.get('DB_NAME') or 'hl7_db'
    DEBUG = os.environ.get('FLASK_DEBUG') == 'True'
    PORT = int(os.environ.get('PORT', 5001))

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

    # APScheduler settings
    SCHEDULER_API_ENABLED = True
    SCHEDULER_TIMEZONE = "UTC"

    # Storage settings
    STORAGE_TYPE = os.environ.get('STORAGE_TYPE', 'local')
    STORAGE_PATH = os.environ.get('STORAGE_PATH', '/Users/jacobkpattara/Desktop/Rails/R5/datastore')

    # SFTP settings
    SFTP_RETRY_ATTEMPTS = 3
    SFTP_RETRY_DELAY = 5  # seconds

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

    DEFAULT_SFTP_FETCH_INTERVAL = 600  # 10 minutes in seconds

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

    @staticmethod
    def init_app(app):
        pass