# app/extensions/celery_ext.py

from flask import Flask
from celery import Celery
import logging
from kombu import Exchange, Queue

logger = logging.getLogger(__name__)

# Create base celery instance
celery = Celery('app')

def init_celery(app=None):
    """Initialize Celery with Flask app configuration"""
    if app is None:
        return celery

    logger.info("Initializing Celery configuration")

    # Update celery config from Flask config
    celery.conf.update(app.config)

    # Define exchanges with better message routing
    stream_exchange = Exchange('streams', type='direct', durable=True)
    delivery_exchange = Exchange('delivery', type='direct', durable=True)
    celery_exchange = Exchange('celery', type='direct', durable=True)
    
    # Define all queues with improved configuration
    celery.conf.task_queues = [
        # StreamsV2 Queues - Enhanced configuration
        Queue('stream_processing', 
              stream_exchange, 
              routing_key='stream.process',
              queue_arguments={
                  'x-max-priority': 10,
                  'x-message-ttl': 3600000,  # 1 hour
                  'x-max-length': 100000,
                  'x-overflow': 'reject-publish'
              }),
        Queue('stream_monitoring', 
              stream_exchange, 
              routing_key='stream.monitor',
              queue_arguments={
                  'x-max-priority': 5,
                  'x-message-ttl': 1800000,  # 30 minutes
                  'x-max-length': 10000
              }),
        Queue('delivery_queue', 
              delivery_exchange, 
              routing_key='delivery',
              queue_arguments={
                  'x-max-priority': 10,
                  'x-message-ttl': 7200000,  # 2 hours
                  'x-max-length': 50000,
                  'x-dead-letter-exchange': 'delivery.dlx'
              }),
        Queue('stream_maintenance', 
              stream_exchange, 
              routing_key='stream.maintenance',
              queue_arguments={
                  'x-max-priority': 3,
                  'x-message-ttl': 86400000  # 24 hours
              }),
        
        # Base Application Queues
        Queue('fhir_queue', celery_exchange, routing_key='fhir'),
        Queue('conversion_queue', celery_exchange, routing_key='conversion'),
        Queue('pasted_message_parsing', celery_exchange, routing_key='pasted_parsing'),
        Queue('sftp_file_parsing', celery_exchange, routing_key='sftp_parsing'),
        Queue('file_parsing', celery_exchange, routing_key='file_parsing'),
        Queue('validation_queue', celery_exchange, routing_key='validation'),
        Queue('maintenance_queue', celery_exchange, routing_key='maintenance'),
        Queue('metrics_queue', celery_exchange, routing_key='metrics'),
        Queue('hl7v2_conversion', celery_exchange, routing_key='hl7v2'),
        Queue('clinical_notes_conversion', celery_exchange, routing_key='clinical_notes'),
        Queue('ccda_conversion', celery_exchange, routing_key='ccda'),
        Queue('x12_conversion', celery_exchange, routing_key='x12'),
    ]

    # Define updated task routes with priorities
    combined_routes = {
        # StreamsV2 Routes - Updated for chain execution
        'app.streamsv2.core.stream_tasks.process_stream_messages': {
            'queue': 'stream_processing',
            'priority': 10
        },
        'app.streamsv2.core.stream_tasks.transform_stream_messages': {
            'queue': 'stream_processing',
            'priority': 9
        },
        'app.streamsv2.core.stream_tasks.process_stream_destinations': {
            'queue': 'delivery_queue',
            'priority': 8
        },
        'app.streamsv2.core.stream_tasks.handle_chain_error': {
            'queue': 'stream_maintenance',
            'priority': 5
        },
        'app.streamsv2.core.stream_tasks.monitor_streams': {
            'queue': 'stream_monitoring'
        },
        'app.streamsv2.core.stream_tasks.process_delivery_retries': {
            'queue': 'delivery_queue'
        },
        'app.streamsv2.core.stream_tasks.cleanup_completed_deliveries': {
            'queue': 'stream_maintenance'
        },
        'app.streamsv2.core.stream_tasks.process_stuck_destinations': {
            'queue': 'stream_maintenance'
        },
        'app.streamsv2.core.stream_tasks.check_endpoint_health': {
            'queue': 'stream_monitoring'
        },
        'app.streamsv2.core.stream_tasks.update_stream_metrics': {
            'queue': 'stream_monitoring'
        },

        # Base Application Routes
        'app.tasks.fetch_fhir_data': {'queue': 'fhir_queue'},
        'app.tasks.process_pending_conversions': {'queue': 'conversion_queue'},
        'app.tasks.parse_pasted_message': {'queue': 'pasted_message_parsing'},
        'app.tasks.parse_sftp_file': {'queue': 'sftp_file_parsing'},
        'app.tasks.parse_files': {'queue': 'file_parsing'},
        'app.services.file_parser_service.extract_and_parse_file': {'queue': 'file_parsing'},
        'app.tasks.validate_fhir_messages': {'queue': 'validation_queue'},
        'app.tasks.periodic_cleanup': {'queue': 'maintenance_queue'},
        'app.tasks.log_conversion_metrics': {'queue': 'metrics_queue'},
        'app.tasks.hl7v2_to_fhir_conversion': {'queue': 'hl7v2_conversion'},
        'app.tasks.clinical_notes_to_fhir_conversion': {'queue': 'clinical_notes_conversion'},
        'app.tasks.ccda_to_fhir_conversion': {'queue': 'ccda_conversion'},
        'app.tasks.x12_to_fhir_conversion': {'queue': 'x12_conversion'},
        'app.tasks.process_hl7v2_messages': {'queue': 'hl7v2_conversion'},
        'app.tasks.cleanup_old_messages': {'queue': 'maintenance_queue'},
        'app.tasks.check_stuck_messages': {'queue': 'maintenance_queue'},
    }

    class ContextTask(celery.Task):
        abstract = True
        
        def __call__(self, *args, **kwargs):
            with app.app_context():
                logger.debug(f"Executing task {self.name} with args: {args}, kwargs: {kwargs}")
                try:
                    return self.run(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Task {self.name} failed: {str(e)}", exc_info=True)
                    raise
        
        def on_failure(self, exc, task_id, args, kwargs, einfo):
            with app.app_context():
                logger.error(f'Task {self.name}[{task_id}] failed: {exc}', exc_info=True)
                super().on_failure(exc, task_id, args, kwargs, einfo)
        
        def on_success(self, retval, task_id, args, kwargs):
            with app.app_context():
                logger.info(f'Task {self.name}[{task_id}] completed successfully')
                super().on_success(retval, task_id, args, kwargs)
        
        def on_retry(self, exc, task_id, args, kwargs, einfo):
            with app.app_context():
                logger.warning(f'Task {self.name}[{task_id}] being retried: {exc}')
                super().on_retry(exc, task_id, args, kwargs, einfo)

    celery.Task = ContextTask

    # Configure enhanced Celery settings
    celery.conf.update(
        # Task execution settings
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        
        # Task handling settings
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        task_track_started=True,
        worker_prefetch_multiplier=1,
        task_time_limit=300,  # 5 minutes
        worker_max_tasks_per_child=1000,
        task_inherit_parent_priority=True,  # Important for chains
        task_default_priority=5,
        
        # Broker settings
        broker_connection_retry_on_startup=True,
        broker_connection_max_retries=None,
        broker_transport_options={
            'priority_steps': list(range(10)),  # 0-9 priority levels
            'visibility_timeout': 43200  # 12 hours
        },
        
        # Result settings
        result_expires=3600,  # 1 hour
        
        # Chain execution settings
        task_always_eager=False,
        task_eager_propagates=True,
        
        # Route configuration
        task_routes=combined_routes,
        
        # Beat schedule from config
        beat_schedule=app.config.get('STREAMSV2_BEAT_SCHEDULE', {}),
        
        # Retry settings
        task_publish_retry=True,
        task_publish_retry_policy={
            'max_retries': 3,
            'interval_start': 0,
            'interval_step': 0.2,
            'interval_max': 0.2,
        },

        # Additional monitoring settings
        worker_send_task_events=True,
        task_send_sent_event=True,
        worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
        worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s] [%(task_name)s(%(task_id)s)] %(message)s',
        
        # Chain specific settings
        task_routes_flush_every=1,  # Ensure immediate route updates
        task_default_queue='stream_processing',  # Default queue for unlisted tasks
        task_create_missing_queues=True,  # Create queues if they don't exist
    )

    # Discover tasks
    celery.autodiscover_tasks([
        'app.streamsv2.core',
        'app.tasks',
        'app.services'
    ], force=True)

    logger.info(f"Registered task routes: {celery.conf.task_routes}")
    logger.info(f"Registered queues: {[q.name for q in celery.conf.task_queues]}")
    logger.info("Celery initialization completed")

    return celery