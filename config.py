# config.py

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
    TEST_HL7_SERVICE_HOST = os.getenv('TEST_HL7_SERVICE_MLLP_HOST', 'localhost')
    TEST_HL7_SERVICE_MLLP_HOST = os.getenv('TEST_HL7_SERVICE_MLLP_HOST', 'localhost')
    TEST_HL7_SERVICE_API_KEY = os.getenv('TEST_HL7_SERVICE_API_KEY', 'your-api-key-here')
    TEST_HL7_ENABLED = os.getenv('TEST_HL7_ENABLED', 'True') == 'True'

    # Celery Queue Names
    FHIR_QUEUE = 'fhir_queue'
    CONVERSION_QUEUE = 'conversion_queue'
    PASTED_MESSAGE_PARSING_QUEUE = 'pasted_message_parsing'
    SFTP_FILE_PARSING_QUEUE = 'sftp_file_parsing'
    FILE_PARSING_QUEUE = 'file_parsing'
    VALIDATION_QUEUE = 'validation_queue'
    MAINTENANCE_QUEUE = 'maintenance_queue'
    METRICS_QUEUE = 'metrics_queue'
    HL7V2_CONVERSION_QUEUE = 'hl7v2_conversion'
    CLINICAL_NOTES_CONVERSION_QUEUE = 'clinical_notes_conversion'
    CCDA_CONVERSION_QUEUE = 'ccda_conversion'
    X12_CONVERSION_QUEUE = 'x12_conversion'

    # Task Intervals (in seconds)
    FETCH_FHIR_INTERVAL = 600
    PROCESS_PENDING_CONVERSIONS_INTERVAL = 60
    PARSE_FILES_INTERVAL = 30
    PARSE_PASTED_MESSAGE_INTERVAL = 10
    PARSE_SFTP_FILE_INTERVAL = 15
    VALIDATE_FHIR_MESSAGES_INTERVAL = 300
    PERIODIC_CLEANUP_INTERVAL = 3600
    LOG_CONVERSION_METRICS_INTERVAL = 900
    PROCESS_HL7V2_MESSAGES_INTERVAL = 60
    CLEANUP_OLD_MESSAGES_INTERVAL = 3600
    CHECK_STUCK_MESSAGES_INTERVAL = 900
    SCHEDULED_MAINTENANCE_INTERVAL = 86400
    REFRESH_FHIR_INTERFACES_INTERVAL = 3600

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
    CONVERSION_RETRY_DELAY = 5
    PROCESSING_TIMEOUT = 300

    # Storage settings
    STORAGE_TYPE = os.environ.get('STORAGE_TYPE', 'local')
    STORAGE_PATH = os.environ.get('STORAGE_PATH', '/path/to/datastore')

    # SFTP settings
    SFTP_RETRY_ATTEMPTS = 3
    SFTP_RETRY_DELAY = 5
    DEFAULT_SFTP_FETCH_INTERVAL = 600

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

    # User roles and admin settings
    USER_ROLES = os.environ.get('USER_ROLES', 'admin,viewer').split(',')
    ADMIN_PANEL_EMAILS = os.environ.get('ADMIN_PANEL_EMAILS', '').split(',')
    DEFAULT_ORGANIZATION_NAME = os.environ.get('DEFAULT_ORGANIZATION_NAME', 'Rails Health')

    # FHIR Validation
    FHIR_VALIDATION_ENABLED = os.environ.get('FHIR_VALIDATION_ENABLED', 'True') == 'True'
    FHIR_VALIDATION_SERVER = os.environ.get('FHIR_VALIDATION_SERVER', 'http://hapi.fhir.org/baseR4')

    # Endpoint Configuration
    ENDPOINT_MODES = ['source', 'destination']

    # Add endpoint mappings configuration
    ENDPOINT_MAPPINGS = {
        'source': {
            'mllp': {
                'name': 'HL7v2 MLLP',
                'message_types': ['HL7v2'],
                'configuration_template': 'configure_mllp_endpoint.html',
                'required_fields': ['host', 'port'],
                'validator': 'app.endpoints.validators.mllp_validator'
            },
            'sftp': {
                'name': 'SFTP',
                'message_types': ['CCDA', 'X12', 'Clinical Notes'],
                'configuration_template': 'configure_sftp_endpoint.html',
                'required_fields': ['host', 'port', 'username', 'remote_path'],
                'validator': 'app.endpoints.validators.sftp_validator'
            }
        },
        'destination': {
            'aws_s3': {
                'name': 'AWS S3',
                'message_types': ['HL7v2 in JSON', 'CCDA in JSON', 'X12 in JSON', 'Clinical Notes in JSON'],
                'configuration_template': 'configure_aws_s3_endpoint.html',
                'required_fields': ['bucket', 'region', 'aws_access_key_id', 'aws_secret_access_key'],
                'validator': 'app.endpoints.validators.aws_s3_validator'
            },
            'gcp_storage': {
                'name': 'Google Cloud Storage',
                'message_types': ['HL7v2 in JSON', 'CCDA in JSON', 'X12 in JSON', 'Clinical Notes in JSON'],
                'configuration_template': 'configure_gcp_storage_endpoint.html',
                'required_fields': ['bucket', 'project_id', 'credentials_json'],
                'validator': 'app.endpoints.validators.gcp_storage_validator'
            }
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

# Message Types and Display Configuration
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

    # Endpoint Type-specific Configurations
    ENDPOINT_TYPE_CONFIGS = {
        'mllp': {
            'default_timeout': 60,
            'max_connections': 10,
            'required_fields': ['host', 'port', 'timeout'],
            'connection_settings': {
                'socket_timeout': 30,
                'keepalive': True,
                'keepalive_interval': 30,
                'max_buffer_size': 1048576  # 1MB
            }
        },
        'sftp': {
            'default_fetch_interval': 600,  # 10 minutes
            'max_file_size': 100 * 1024 * 1024,  # 100MB
            'supported_auth_methods': ['password', 'key'],
            'required_fields': ['host', 'port', 'username', 'remote_path'],
            'connection_settings': {
                'timeout': 30,
                'banner_timeout': 60,
                'auth_timeout': 60
            }
        },
        'aws_s3': {
            'regions': [
                'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
                'eu-west-1', 'eu-west-2', 'eu-central-1',
                'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1'
            ],
            'required_fields': ['bucket', 'region', 'aws_access_key_id', 'aws_secret_access_key'],
            'optional_fields': ['kms_key_id', 'endpoint_url', 'storage_class']
        },
        'gcp_storage': {
            'required_fields': ['bucket', 'project_id', 'credentials_json'],
            'optional_fields': ['location', 'storage_class', 'kms_key_name']
        }
    }

    # Cloud Storage Configuration
    CLOUD_STORAGE = {
        'providers': {
            'aws_s3': {
                'name': 'AWS S3',
                'encryption_options': {
                    'none': 'No encryption',
                    'AES256': 'Server-side encryption with S3-managed keys',
                    'aws:kms': 'AWS KMS managed keys',
                    'customer-provided': 'Customer provided keys'
                },
                'storage_classes': [
                    'STANDARD',
                    'STANDARD_IA',
                    'ONEZONE_IA',
                    'INTELLIGENT_TIERING',
                    'GLACIER',
                    'DEEP_ARCHIVE'
                ],
                'required_fields': ['bucket', 'region', 'aws_access_key_id', 'aws_secret_access_key'],
                'validation': {
                    'bucket_name_pattern': r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$',
                    'region_pattern': r'^[a-z]{2}-[a-z]+-\d{1}$'
                }
            },
            'gcp_storage': {
                'name': 'Google Cloud Storage',
                'encryption_options': {
                    'none': 'No encryption',
                    'google-managed': 'Google-managed encryption',
                    'customer-managed': 'Customer-managed encryption (CMEK)',
                    'customer-supplied': 'Customer-supplied encryption keys (CSEK)'
                },
                'storage_classes': [
                    'STANDARD',
                    'NEARLINE',
                    'COLDLINE',
                    'ARCHIVE'
                ],
                'required_fields': ['bucket', 'project_id', 'credentials_json'],
                'validation': {
                    'bucket_name_pattern': r'^[a-z0-9][a-z0-9-_.]*[a-z0-9]$',
                    'project_id_pattern': r'^[a-z0-9-]+$'
                }
            }
        },
        'upload_settings': {
            'max_file_size': 5 * 1024 * 1024 * 1024,  # 5GB
            'multipart_threshold': 100 * 1024 * 1024,  # 100MB
            'max_concurrent_uploads': 10,
            'retry_attempts': 3,
            'chunk_size': 8 * 1024 * 1024,  # 8MB
            'upload_timeout': 3600  # 1 hour
        },
        'monitoring': {
            'metrics_enabled': True,
            'metrics_interval': 300,  # 5 minutes
            'alert_on_failures': True,
            'max_upload_latency': 300  # 5 minutes
        }
    }

    # StreamsV2 Core Settings
    STREAMSV2_SETTINGS = {
        # Core processing settings
        'PROCESSING_INTERVAL': 60,
        'BATCH_SIZE': 100,
        'MAX_RETRIES': 3,
        'RETRY_DELAY': 300,
        'PROCESSING_TIMEOUT': 300,
        
        # Monitoring settings
        'HEALTH_CHECK_INTERVAL': 300,
        'ERROR_THRESHOLD': 0.1,
        'PROCESSING_DELAY_THRESHOLD': 300,
        
        # Cleanup settings
        'MESSAGE_RETENTION_DAYS': 30,
        'LOG_RETENTION_DAYS': 7,
        'CLEANUP_INTERVAL': 86400,
        
        # Performance settings
        'MAX_CONCURRENT_DELIVERIES': 5,
        'RATE_LIMIT': 100,
        'CONNECTION_TIMEOUT': 30,
        
        # Resource limits
        'MAX_MEMORY_USAGE': 1024 * 1024 * 1024,  # 1GB
        'MAX_DISK_USAGE': 10 * 1024 * 1024 * 1024,  # 10GB
        'MAX_CONCURRENT_TASKS': 20
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

    STREAMSV2_MESSAGE_TYPES = {
        'source': {
            'HL7v2': {
                'allowed_endpoints': ['mllp'],
                'destination_format': 'HL7v2 in JSON'
            },
            'CCDA': {
                'allowed_endpoints': ['sftp'],
                'destination_format': 'CCDA in JSON'
            },
            'X12': {
                'allowed_endpoints': ['sftp'],
                'destination_format': 'X12 in JSON'
            },
            'Clinical Notes': {
                'allowed_endpoints': ['sftp'],
                'destination_format': 'Clinical Notes in JSON'
            }
        },
        'destination': {
            'HL7v2 in JSON': {
                'allowed_endpoints': ['aws_s3', 'gcp_storage']
            },
            'CCDA in JSON': {
                'allowed_endpoints': ['aws_s3', 'gcp_storage']
            },
            'X12 in JSON': {
                'allowed_endpoints': ['aws_s3', 'gcp_storage']
            },
            'Clinical Notes in JSON': {
                'allowed_endpoints': ['aws_s3', 'gcp_storage']
            }
        }
    }

    # StreamsV2 Storage Settings
    STREAMSV2_STORAGE_CONFIG = {
        'temp_dir': '/tmp/streamsv2',
        'file_prefix': 'stream_',
        'max_file_size': 10 * 1024 * 1024,  # 10MB
        'cleanup_interval': 3600,  # 1 hour
        'path_template': '{year}/{month}/{day}/{stream_uuid}/',
        'compression_enabled': True,
        'compression_algorithm': 'gzip'
    }

    # StreamsV2 Queue Configuration
    STREAMSV2_QUEUES = {
        'stream_processing': {
            'exchange': 'streams',
            'routing_key': 'stream.process',
            'queue_arguments': {
                'x-max-priority': 10,
                'x-message-ttl': 3600000,  # 1 hour
                'x-max-length': 100000,
                'x-overflow': 'reject-publish'
            }
        },
        'stream_monitoring': {
            'exchange': 'streams',
            'routing_key': 'stream.monitor',
            'queue_arguments': {
                'x-max-priority': 5,
                'x-message-ttl': 300000,  # 5 minutes
                'x-max-length': 10000,
                'x-overflow': 'reject-publish'
            }
        },
        'delivery_queue': {
            'exchange': 'delivery',
            'routing_key': 'delivery',
            'queue_arguments': {
                'x-max-priority': 10,
                'x-message-ttl': 86400000,  # 24 hours
                'x-max-length': 500000,
                'x-overflow': 'reject-publish',
                'x-dead-letter-exchange': 'delivery.dlx'
            }
        },
        'stream_maintenance': {
            'exchange': 'streams',
            'routing_key': 'stream.maintenance',
            'queue_arguments': {
                'x-max-priority': 3,
                'x-message-ttl': 86400000,  # 24 hours
                'x-max-length': 10000,
                'x-overflow': 'reject-publish'
            }
        }
    }

    # StreamsV2 Task Routes
    STREAMSV2_TASK_ROUTES = {
        # Core processing tasks
        'streamsv2.process_stream_messages': {'queue': 'stream_processing'},
        'streamsv2.transform_stream_messages': {'queue': 'stream_processing'},
        'streamsv2.process_stream_destinations': {'queue': 'delivery_queue'},
        
        # Monitoring tasks
        'streamsv2.monitor_streams': {'queue': 'stream_monitoring'},
        'streamsv2.check_endpoint_health': {'queue': 'stream_monitoring'},
        'streamsv2.update_stream_metrics': {'queue': 'stream_monitoring'},
        
        # Delivery tasks
        'streamsv2.deliver_messages': {'queue': 'delivery_queue'},
        'streamsv2.process_delivery_retries': {'queue': 'delivery_queue'},
        
        # Maintenance tasks
        'streamsv2.cleanup_stream_processed_messages': {'queue': 'stream_maintenance'},
        'streamsv2.recover_stuck_messages': {'queue': 'stream_maintenance'},
        'streamsv2.process_stuck_destinations': {'queue': 'stream_maintenance'},
        'streamsv2.cleanup_completed_deliveries': {'queue': 'stream_maintenance'}
    }

    # Base Celery Task Routes
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
        'app.tasks.check_stuck_messages': {'queue': MAINTENANCE_QUEUE},
        **STREAMSV2_TASK_ROUTES  # Include StreamsV2 routes
    }

    # StreamsV2 Beat Schedule
    STREAMSV2_BEAT_SCHEDULE = {
        'monitor-streams': {
            'task': 'streamsv2.monitor_streams',
            'schedule': STREAMSV2_SETTINGS['PROCESSING_INTERVAL'],
            'options': {'queue': 'stream_monitoring'}
        },
        'check-endpoint-health': {
            'task': 'streamsv2.check_endpoint_health',
            'schedule': STREAMSV2_SETTINGS['HEALTH_CHECK_INTERVAL'],
            'options': {'queue': 'stream_monitoring'}
        },
        'update-stream-metrics': {
            'task': 'streamsv2.update_stream_metrics',
            'schedule': 300,  # Every 5 minutes
            'options': {'queue': 'stream_monitoring'}
        },
        'process-delivery-retries': {
            'task': 'streamsv2.process_delivery_retries',
            'schedule': STREAMSV2_SETTINGS['RETRY_DELAY'],
            'options': {'queue': 'delivery_queue'}
        },
        'cleanup-stream-messages': {
            'task': 'streamsv2.cleanup_stream_processed_messages',
            'schedule': STREAMSV2_SETTINGS['CLEANUP_INTERVAL'],
            'options': {'queue': 'stream_maintenance'}
        },
        'recover-stuck-messages': {
            'task': 'streamsv2.recover_stuck_messages',
            'schedule': 1800,  # Every 30 minutes
            'options': {'queue': 'stream_maintenance'}
        },
        'process-stuck-destinations': {
            'task': 'streamsv2.process_stuck_destinations',
            'schedule': 1800,  # Every 30 minutes
            'options': {'queue': 'stream_maintenance'}
        }
    }

    # Task Monitoring Settings
    TASK_MONITORING = {
        'heartbeat_interval': 30,
        'stale_task_threshold': 300,
        'max_task_runtime': 3600,
        'monitor_queues': True,
        'queue_alert_threshold': 1000,
        'dead_letter_queue': 'dlq.tasks',
        'task_events_enabled': True,
        'task_track_started': True,
        'worker_concurrency': 4,
        'worker_prefetch_multiplier': 1,
        'worker_max_tasks_per_child': 1000,
        'worker_max_memory': 512 * 1024 * 1024  # 512MB
    }

    # StreamsV2 Status Constants
    STREAMSV2_STATUS = {
        'ACTIVE': 'active',
        'INACTIVE': 'inactive',
        'ERROR': 'error',
        'DELETED': 'deleted',
        'STARTING': 'starting',
        'STOPPING': 'stopping',
        'PAUSED': 'paused',
        'RECOVERY': 'recovery'
    }

    # StreamsV2 Message Status Constants
    STREAMSV2_MESSAGE_STATUS = {
        'PENDING': 'pending',
        'PROCESSING': 'processing',
        'COMPLETED': 'completed',
        'FAILED': 'failed',
        'RETRYING': 'retrying',
        'CANCELLED': 'cancelled',
        'SKIPPED': 'skipped'
    }

    # StreamsV2 Error Handling
    STREAMSV2_ERROR_HANDLING = {
        'max_retries_per_message': 3,
        'retry_backoff': True,
        'retry_backoff_multiplier': 2,
        'retry_jitter': True,
        'retry_max_delay': 3600,  # 1 hour
        'error_types': {
            'connection': {'retryable': True, 'priority': 'high'},
            'validation': {'retryable': False, 'priority': 'medium'},
            'transformation': {'retryable': True, 'priority': 'high'},
            'delivery': {'retryable': True, 'priority': 'high'},
            'timeout': {'retryable': True, 'priority': 'low'}
        }
    }

    # StreamsV2 Monitoring and Alerting
    STREAMSV2_MONITORING = {
        'error_threshold': STREAMSV2_SETTINGS['ERROR_THRESHOLD'],
        'processing_delay_threshold': STREAMSV2_SETTINGS['PROCESSING_DELAY_THRESHOLD'],
        'metrics_retention_days': 7,
        'alert_channels': ['slack', 'email'],
        'notification_levels': ['error', 'warning', 'info'],
        'performance_metrics': [
            'messages_processed',
            'processing_time',
            'error_rate',
            'delivery_success_rate',
            'endpoint_health',
            'queue_depth',
            'memory_usage',
            'disk_usage'
        ],
        'alert_thresholds': {
            'error_rate': 0.1,  # 10%
            'processing_delay': 300,  # 5 minutes
            'queue_depth': 10000,
            'memory_usage': 0.9,  # 90%
            'disk_usage': 0.85  # 85%
        }
    }

    @classmethod
    def print_celery_config(cls):
        """Print Celery configuration details"""
        print(f"broker_url in Config: {cls.broker_url}")
        print(f"result_backend in Config: {cls.result_backend}")

    @classmethod
    def init_app(cls, app):
        """Initialize Flask application with configuration"""
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
                file_handler = RotatingFileHandler(
                    'logs/rails_health.log',
                    maxBytes=10240,
                    backupCount=10
                )
                file_handler.setFormatter(logging.Formatter(
                    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
                ))
                file_handler.setLevel(logging.INFO)
                app.logger.addHandler(file_handler)

        app.logger.setLevel(logging.INFO)
        app.logger.info('Rails Health startup')

Config.print_celery_config()