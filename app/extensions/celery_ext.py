from flask import Flask
from celery import Celery

# Create base celery instance
celery = Celery('app')

def init_celery(app=None):
    """Initialize Celery with Flask app configuration"""
    if app is None:
        return celery

    # Update celery config from Flask config
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        abstract = True
        
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)
        
        def on_failure(self, exc, task_id, args, kwargs, einfo):
            with app.app_context():
                app.logger.error(f'Celery task {task_id} failed: {exc}')
                super().on_failure(exc, task_id, args, kwargs, einfo)

    celery.Task = ContextTask

    # Configure celery task routes
    celery.conf.task_routes = {
        **app.config.get('CELERY_TASK_ROUTES', {}),
        **app.config.get('STREAMSV2_TASK_ROUTES', {})
    }

    # Configure celery beat schedule
    base_schedule = {
        'process-all-message-types': {
            'task': 'app.tasks.process_all_message_types',
            'schedule': app.config['PROCESS_PENDING_CONVERSIONS_INTERVAL'],
        },
        'cleanup-old-messages': {
            'task': 'app.tasks.cleanup_old_messages',
            'schedule': app.config['CLEANUP_OLD_MESSAGES_INTERVAL'],
            'args': (30,)
        },
        'check-stuck-messages': {
            'task': 'app.tasks.check_stuck_messages',
            'schedule': app.config['CHECK_STUCK_MESSAGES_INTERVAL'],
        },
        'fetch-fhir-data': {
            'task': 'app.tasks.fetch_fhir_data',
            'schedule': app.config['FETCH_FHIR_INTERVAL'],
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
        }
    }

    # Add StreamsV2 schedule
    streamsv2_schedule = app.config.get('STREAMSV2_BEAT_SCHEDULE', {})
    celery.conf.beat_schedule = {**base_schedule, **streamsv2_schedule}

    # Configure additional celery settings
    celery.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        broker_connection_retry_on_startup=True,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        task_track_started=True,
        task_time_limit=300,  # 5 minutes
        worker_prefetch_multiplier=1,
        result_expires=3600,  # 1 hour
        worker_send_task_events=True,  # Enable task events
        task_send_sent_event=True,  # Enable sent events
    )

    return celery