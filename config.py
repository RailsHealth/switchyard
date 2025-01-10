import os
import logging
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

load_dotenv()

print(f"CELERY_BROKER_URL from env: {os.environ.get('CELERY_BROKER_URL')}")
print(f"CELERY_RESULT_BACKEND from env: {os.environ.get('CELERY_RESULT_BACKEND')}")

class Config:
    # Basic Flask configuration
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    DEBUG = os.environ.get('FLASK_DEBUG') == 'True'
    PORT = int(os.environ.get('PORT', 5001))

    # MongoDB configuration
    MONGO_URI = os.environ.get('MONGO_URI') or 'mongodb://localhost:27017/hl7_db'
    DB_NAME = os.environ.get('DB_NAME') or 'hl7_db'

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

    # Additional Celery settings
    CELERY_TASK_ACKS_LATE = True
    CELERY_TASK_REJECT_ON_WORKER_LOST = True

    # Test HL7 Service Configuration
    TEST_HL7_SERVICE_URL = os.getenv('TEST_HL7_SERVICE_URL', 'http://localhost:8001')
    TEST_HL7_SERVICE_HOST = os.getenv('TEST_HL7_SERVICE_MLLP_HOST', 'localhost') # possibly redundant
    TEST_HL7_SERVICE_MLLP_HOST = os.getenv('TEST_HL7_SERVICE_MLLP_HOST', 'localhost')
    TEST_HL7_SERVICE_API_KEY = os.getenv('TEST_HL7_SERVICE_API_KEY', 'your-api-key-here')
    TEST_HL7_ENABLED = os.getenv('TEST_HL7_ENABLED', 'True') == 'True'

    # Celery Queue Names
    FHIR_QUEUE = 'fhir_queue'
    CONVERSION_QUEUE = 'conversion_queue'
    PASTED_MESSAGE_PARSING_QUEUE = 'pasted_message_parsing'
    SFTP_FILE_PARSING_QUEUE = 'sftp_file_parsing'
    FILE_PARSING_QUEUE = 'file_parsing'  # Generic file parsing queue
    VALIDATION_QUEUE = 'validation_queue'
    MAINTENANCE_QUEUE = 'maintenance_queue'
    METRICS_QUEUE = 'metrics_queue'
    HL7V2_CONVERSION_QUEUE = 'hl7v2_conversion'
    CLINICAL_NOTES_CONVERSION_QUEUE = 'clinical_notes_conversion'
    CCDA_CONVERSION_QUEUE = 'ccda_conversion'
    X12_CONVERSION_QUEUE = 'x12_conversion'

    # Celery Task Routes
    CELERY_TASK_ROUTES = {
        'app.tasks.fetch_fhir_data': {'queue': FHIR_QUEUE},
        'app.tasks.process_pending_conversions': {'queue': CONVERSION_QUEUE},
        'app.tasks.parse_pasted_message': {'queue': PASTED_MESSAGE_PARSING_QUEUE},
        'app.tasks.parse_sftp_file': {'queue': SFTP_FILE_PARSING_QUEUE},
        'app.tasks.parse_files': {'queue': FILE_PARSING_QUEUE},
        'app.services.file_parser_service.extract_and_parse_file': {'queue': FILE_PARSING_QUEUE},
        'app.tasks.validate_fhir_messages': {'queue': VALIDATION_QUEUE},
        'app.tasks.periodic_cleanup': {'queue': MAINTENANCE_QUEUE},
        'app.tasks.log_conversion_metrics': {'queue': METRICS_QUEUE},
        'app.tasks.hl7v2_to_fhir_conversion': {'queue': HL7V2_CONVERSION_QUEUE},
        'app.tasks.clinical_notes_to_fhir_conversion': {'queue': CLINICAL_NOTES_CONVERSION_QUEUE},
        'app.tasks.ccda_to_fhir_conversion': {'queue': CCDA_CONVERSION_QUEUE},
        'app.tasks.x12_to_fhir_conversion': {'queue': X12_CONVERSION_QUEUE},
        'app.tasks.process_hl7v2_messages': {'queue': HL7V2_CONVERSION_QUEUE},
        'app.tasks.cleanup_old_messages': {'queue': MAINTENANCE_QUEUE},
        'app.tasks.check_stuck_messages': {'queue': MAINTENANCE_QUEUE}
    }

    # Task Intervals (in seconds)
    FETCH_FHIR_INTERVAL = 600
    PROCESS_PENDING_CONVERSIONS_INTERVAL = 60
    PARSE_FILES_INTERVAL = 30
    PARSE_PASTED_MESSAGE_INTERVAL = 10  # seconds
    PARSE_SFTP_FILE_INTERVAL = 15  # seconds
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
    HL7V2_API_URL = os.environ.get('HL7V2_API_URL')
    CLINICAL_NOTES_API_URL = os.environ.get('CLINICAL_NOTES_API_URL')
    CCDA_API_URL = os.environ.get('CCDA_API_URL')
    X12_API_URL = os.environ.get('X12_API_URL')

    # Conversion settings
    MAX_CONVERSION_RETRIES = 3
    CONVERSION_RETRY_DELAY = 5  # seconds between retries
    PROCESSING_TIMEOUT = 300  # 5 minutes


    # Storage settings
    STORAGE_TYPE = os.environ.get('STORAGE_TYPE', 'local')
    STORAGE_PATH = os.environ.get('STORAGE_PATH', '/path/to/datastore')

    # SFTP settings
    SFTP_RETRY_ATTEMPTS = 3
    SFTP_RETRY_DELAY = 5  # seconds
    DEFAULT_SFTP_FETCH_INTERVAL = 600  # 10 minutes in seconds

    # Message types
    MESSAGE_TYPES = ['HL7v2', 'FHIR', 'CCDA', 'X12', 'Clinical Notes']

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
    USER_ROLES = os.environ.get('USER_ROLES', 'admin,viewer').split(',')

    # Admin panel access
    ADMIN_PANEL_EMAILS = os.environ.get('ADMIN_PANEL_EMAILS', '').split(',')

    # Default organization name
    DEFAULT_ORGANIZATION_NAME = os.environ.get('DEFAULT_ORGANIZATION_NAME', 'Rails Health')

    # FHIR Validation
    FHIR_VALIDATION_ENABLED = os.environ.get('FHIR_VALIDATION_ENABLED', 'True') == 'True'
    FHIR_VALIDATION_SERVER = os.environ.get('FHIR_VALIDATION_SERVER', 'http://hapi.fhir.org/baseR4')

    # Endpoint Configuration
    ENDPOINT_MODES = ['source', 'destination']
    
    # Mapping of message types to available endpoint types for each mode
    ENDPOINT_MAPPINGS = {
        'source': {
            'HL7v2': ['mllp'],  
            'CCDA': ['sftp'],
            'X12': ['sftp'],
            'Clinical Notes': ['sftp']
        },
        'destination': {
            'HL7v2 in JSON': ['aws_s3', 'gcp_storage'],
            'CCDA in JSON': ['aws_s3', 'gcp_storage'],
            'X12 in JSON': ['aws_s3', 'gcp_storage'],
            'Clinical Notes in JSON': ['aws_s3', 'gcp_storage']
        }
    }

    # Test endpoint configurations
    TEST_ENDPOINTS = {
        'HL7v2': {
            'name': 'Test HL7v2 Server',
            'description': 'Simulates an HL7v2 endpoint receiving a random HL7v2 message once every 2 seconds. Data generated using simhospital.',
            'mode': 'source'
        },
        'FHIR': {
            'firely': {
                'name': 'Firely FHIR Server',
                'description': 'Connect to the public FHIR server maintained by Firely. Once switched ON, it starts receiving data from the server.',
                'mode': 'source'
            },
            'hapi': {
                'name': 'HAPI FHIR Server',
                'description': 'Connect to the public FHIR server maintained by HAPI. Once switched ON, it starts receiving data from the server.',
                'mode': 'source'
            }
        }
    }

    # Endpoint type-specific configurations
    ENDPOINT_TYPE_CONFIGS = {
        'mllp': {
            'default_timeout': 60,
            'max_connections': 10,
            'required_fields': ['host', 'port', 'timeout']
        },
        'sftp': {
            'default_fetch_interval': 600,  # 10 minutes
            'max_file_size': 100 * 1024 * 1024,  # 100MB
            'supported_auth_methods': ['password', 'key'],
            'required_fields': ['host', 'port', 'username', 'remote_path']
        },
        'aws_s3': {
            'regions': [
                'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
                'eu-west-1', 'eu-west-2', 'eu-central-1',
                'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1'
            ],
            'required_fields': ['bucket', 'region', 'aws_access_key_id', 'aws_secret_access_key']
        },
        'gcp_storage': {
            'required_fields': ['bucket', 'project_id', 'credentials_json']
        }
    }

    # Coming soon features
    COMING_SOON_FEATURES = {
        'endpoint_types': {
            'mllp_ipsec': 'MLLP with IPSec support',
            'https': 'HTTPS endpoints for FHIR servers'
        }
    }

    # Endpoint validation settings
    ENDPOINT_VALIDATION = {
        'name_max_length': 100,
        'max_endpoints_per_org': 50,
        'allowed_file_extensions': {
            'CCDA': ['.xml', '.ccda', '.pdf'],
            'X12': ['.x12', '.txt', '.pdf'],
            'Clinical Notes': ['.txt', '.pdf']
        }
    }

    # Cloud storage settings
    CLOUD_STORAGE = {
        'providers': {
            'aws_s3': {
                'name': 'AWS S3',
                'encryption_options': {
                    'none': 'No encryption',
                    'AES256': 'Server-side encryption with S3-managed keys'
                },
                'required_fields': ['bucket', 'region', 'aws_access_key_id', 'aws_secret_access_key'],
                'validation': {
                    'bucket_name_pattern': r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$'
                }
            },
            'gcp_storage': {
                'name': 'Google Cloud Storage',
                'encryption_options': {
                    'none': 'No encryption',
                    'google-managed': 'Google-managed encryption'
                },
                'required_fields': ['bucket', 'project_id', 'credentials_json'],
                'validation': {
                    'bucket_name_pattern': r'^[a-z0-9][a-z0-9-_.]*[a-z0-9]$'
                }
            }
        },
        'upload_settings': {
            'max_file_size': 5 * 1024 * 1024 * 1024,  # 5GB
            'multipart_threshold': 100 * 1024 * 1024,  # 100MB
            'max_concurrent_uploads': 10,
            'retry_attempts': 3
        }
    }

    # Default settings for endpoint creation
    DEFAULT_ENDPOINT_SETTINGS = {
        'deidentification_requested': False,
        'purging_requested': False,
        'retry_attempts': 3,
        'retry_delay': 5,  # seconds
        'connection_timeout': 30  # seconds
    }

    # Message type display names
    MESSAGE_TYPE_DISPLAY = {
        # Source formats
        'HL7v2': 'HL7v2',
        'CCDA': 'CCDA (supports PDF only)',
        'X12': 'X12 (supports PDF only)',
        'Clinical Notes': 'Clinical Notes (supports PDF only)',
        
        # Destination formats
        'HL7v2 in JSON': 'HL7v2 (JSON)',
        'CCDA in JSON': 'CCDA (JSON)',
        'X12 in JSON': 'X12 (JSON)',
        'Clinical Notes in JSON': 'Clinical Notes (JSON)'
    }

    # Message type descriptions (optional but helpful for UI)
    MESSAGE_TYPE_DESCRIPTIONS = {
        # Source formats
        'HL7v2': 'Health Level 7 Version 2 messages via MLLP',
        'CCDA': 'Consolidated Clinical Document Architecture documents (PDF)',
        'X12': 'X12 EDI documents (PDF)',
        'Clinical Notes': 'Clinical documentation and notes (PDF)',
        
        # Destination formats
        'HL7v2 in JSON': 'HL7v2 messages wrapped in JSON for cloud storage',
        'CCDA in JSON': 'CCDA content wrapped in JSON for cloud storage',
        'X12 in JSON': 'X12 content wrapped in JSON for cloud storage',
        'Clinical Notes in JSON': 'Clinical Notes content wrapped in JSON for cloud storage'
    }

    # Endpoint mode display names and descriptions
    ENDPOINT_MODE_INFO = {
        'source': {
            'name': 'Source',
            'description': 'This endpoint sends data. Currently supports MLLP for HL7v2 and SFTP for PDF files.'
        },
        'destination': {
            'name': 'Destination',
            'description': 'This endpoint receives data. Currently supports AWS S3-SSE, Google Cloud Storage-SSE.'
        }
    }

    # Endpoint status mappings
    ENDPOINT_STATUS = {
        'active': 'Active',
        'inactive': 'Inactive',
        'starting': 'Starting',
        'stopping': 'Stopping',
        'error': 'Error'
    }

    # Control action mappings
    ENDPOINT_ACTIONS = {
        'start': 'Start',
        'stop': 'Stop',
        'pause': 'Pause',
        'resume': 'Resume'
    }

    # Organization level endpoint limits
    ORGANIZATION_ENDPOINT_LIMITS = {
        'max_active_endpoints': 20,
        'max_test_endpoints': 3,
        'max_storage_per_endpoint': 1024 * 1024 * 1024  # 1GB
    }

    # Rate limiting
    RATELIMIT_ENABLED = True
    RATELIMIT_STORAGE_URL = REDIS_URL
    RATELIMIT_DEFAULT = "200 per day;50 per hour;1 per second"

    # StreamsV2 Configuration
    STREAMSV2_QUEUE_PREFIX = "streamv2"
    STREAMSV2_PROCESSING_INTERVAL = 60  # Process each stream every minute
    STREAMSV2_BATCH_SIZE = 100  # Number of messages to process in each batch
    STREAMSV2_MAX_RETRIES = 3
    STREAMSV2_RETRY_DELAY = 5  # seconds
    
    # StreamsV2 Cleanup Settings
    STREAMSV2_CLEANUP_INTERVAL = 86400  # 24 hours
    STREAMSV2_RECORD_RETENTION_DAYS = 30
    
    # StreamsV2 Message Format Settings
    STREAMSV2_MESSAGE_TYPES = {
        'source': {
            'HL7v2': {
                'content_type': 'text/plain',
                'allowed_endpoints': ['mllp'],
                'destination_format': 'HL7v2 in JSON'
            },
            'CCDA': {
                'content_type': 'text/plain',
                'allowed_endpoints': ['sftp'],
                'destination_format': 'CCDA in JSON'
            },
            'X12': {
                'content_type': 'text/plain',
                'allowed_endpoints': ['sftp'],
                'destination_format': 'X12 in JSON'
            },
            'Clinical Notes': {
                'content_type': 'text/plain',
                'allowed_endpoints': ['sftp'],
                'destination_format': 'Clinical Notes in JSON'
            }
        },
        'destination': {
            'HL7v2 in JSON': {
                'content_type': 'application/json',
                'allowed_endpoints': ['aws_s3', 'gcp_storage'],
                'source_type': 'HL7v2'
            },
            'CCDA in JSON': {
                'content_type': 'application/json',
                'allowed_endpoints': ['aws_s3', 'gcp_storage'],
                'source_type': 'CCDA'
            },
            'X12 in JSON': {
                'content_type': 'application/json',
                'allowed_endpoints': ['aws_s3', 'gcp_storage'],
                'source_type': 'X12'
            },
            'Clinical Notes in JSON': {
                'content_type': 'application/json',
                'allowed_endpoints': ['aws_s3', 'gcp_storage'],
                'source_type': 'Clinical Notes'
            }
        }
    }
    
    # StreamsV2 Storage Settings
    STREAMSV2_STORAGE_CONFIG = {
        'temp_dir': '/tmp/streamsv2',
        'file_prefix': 'stream_',
        'max_file_size': 10 * 1024 * 1024  # 10MB
    }
    
    # StreamsV2 Queue Configuration
    STREAMSV2_TASK_ROUTES = {
        'tasks.process_stream_messages': {'queue': 'stream_processing'},
        'tasks.monitor_streams': {'queue': 'stream_monitoring'},
        'tasks.cleanup_stream_processed_messages': {'queue': 'stream_maintenance'}
    }
    
    STREAMSV2_BEAT_SCHEDULE = {
        'monitor-streams': {
            'task': 'tasks.monitor_streams',
            'schedule': STREAMSV2_PROCESSING_INTERVAL,
            'options': {'queue': 'stream_monitoring'}
        },
        'cleanup-stream-processed-messages': {
            'task': 'tasks.cleanup_stream_processed_messages',
            'schedule': STREAMSV2_CLEANUP_INTERVAL,
            'kwargs': {'days': STREAMSV2_RECORD_RETENTION_DAYS},
            'options': {'queue': 'stream_maintenance'}
        }
    }
    
    # StreamsV2 Monitoring Settings
    STREAMSV2_METRICS = {
        'collection_interval': 300,  # 5 minutes
        'retention_period': 7 * 24 * 60 * 60,  # 7 days
        'alert_thresholds': {
            'error_rate': 0.1,  # Alert if error rate exceeds 10%
            'processing_delay': 300  # Alert if processing delay exceeds 5 minutes
        }
    }
    
    # Update existing CELERY_TASK_ROUTES with StreamsV2 routes
    CELERY_TASK_ROUTES.update(STREAMSV2_TASK_ROUTES)
    
    STREAMSV2_BEAT_SCHEDULE = {
        'monitor-streams': {
            'task': 'tasks.monitor_streams',
            'schedule': STREAMSV2_PROCESSING_INTERVAL,
            'options': {'queue': 'stream_monitoring'}
        },
        'cleanup-stream-processed-messages': {
            'task': 'tasks.cleanup_stream_processed_messages',
            'schedule': STREAMSV2_CLEANUP_INTERVAL,
            'kwargs': {'days': STREAMSV2_RECORD_RETENTION_DAYS},
            'options': {'queue': 'stream_maintenance'}
        }
    }
    
    # StreamsV2 Endpoint Type Mappings
    STREAMSV2_ENDPOINT_MAPPINGS = {
        'source': {
            'mllp': {
                'supported_types': ['HL7v2'],
                'output_format': 'HL7v2 in JSON'
            },
            'sftp': {
                'supported_types': ['CCDA', 'X12', 'Clinical Notes'],
                'output_format': '{type} in JSON'  # Gets formatted with the type
            }
        },
        'destination': {
            'aws_s3': {
                'supported_types': [
                    'HL7v2 in JSON',
                    'CCDA in JSON',
                    'X12 in JSON',
                    'Clinical Notes in JSON'
                ],
                'required_config': ['bucket', 'region', 'access_key', 'secret_key']
            },
            'gcp_storage': {
                'supported_types': [
                    'HL7v2 in JSON',
                    'CCDA in JSON',
                    'X12 in JSON',
                    'Clinical Notes in JSON'
                ],
                'required_config': ['bucket', 'project_id', 'credentials']
            }
        }
    }

    # StreamsV2 Status Constants
    STREAMSV2_STATUS = {
        'ACTIVE': 'active',
        'INACTIVE': 'inactive',
        'ERROR': 'error',
        'DELETED': 'deleted'
    }

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