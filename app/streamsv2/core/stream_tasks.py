# app/streamsv2/core/stream_tasks.py

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import threading
from flask import current_app
from celery import shared_task
from app.extensions.celery_ext import celery
from app.extensions import mongo
from app.streamsv2.services.message_processor import StreamMessageProcessor
from app.utils.logging_utils import log_message_cycle
from pymongo.errors import PyMongoError
from app.streamsv2.services.destination_processor import DestinationProcessor

logger = logging.getLogger(__name__)

# Lock for synchronizing stream operations
stream_locks = {}

class TaskState:
    """Task processing states"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

def track_process_state(stream_uuid: str, state: str, details: Dict[str, Any] = None) -> None:
    """Track process state changes"""
    try:
        update_data = {
            "process_state": state,
            "updated_at": datetime.utcnow()
        }
        
        if details:
            update_data["process_details"] = details
            
        mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {"$set": update_data}
        )
        
    except Exception as e:
        logger.error(f"Error tracking process state: {str(e)}")

@celery.task(
    name="tasks.cleanup_stream",
    queue="stream_maintenance"
)
def cleanup_stream(stream_uuid: str):
    """Clean up stream with graceful shutdown"""
    logger.info(f"Starting cleanup for stream {stream_uuid}")
    
    try:
        with current_app.app_context():
            # Track cleanup start
            track_process_state(stream_uuid, TaskState.PROCESSING, {
                "action": "cleanup_started",
                "timestamp": datetime.utcnow().isoformat()
            })

            # Mark remaining messages as cancelled
            result = mongo.db.destination_queue.update_many(
                {
                    "stream_uuid": stream_uuid,
                    "status": {"$in": [TaskState.PENDING, TaskState.PROCESSING]}
                },
                {
                    "$set": {
                        "status": TaskState.CANCELLED,
                        "updated_at": datetime.utcnow(),
                        "cancelled_reason": "Stream stopped"
                    }
                }
            )
            
            # Update stream status
            mongo.db.streams_v2.update_one(
                {"uuid": stream_uuid},
                {
                    "$set": {
                        "status": "inactive",
                        "active": False,
                        "process_state": TaskState.COMPLETED,
                        "updated_at": datetime.utcnow(),
                        "cleanup_details": {
                            "cancelled_messages": result.modified_count,
                            "completed_at": datetime.utcnow().isoformat()
                        }
                    }
                }
            )

            # Log cleanup completion
            log_task_event(
                stream_uuid=stream_uuid,
                event_type="stream_cleanup_completed",
                details={
                    "cancelled_messages": result.modified_count,
                    "timestamp": datetime.utcnow().isoformat()
                },
                status="success"
            )

    except Exception as e:
        logger.error(f"Error in stream cleanup: {str(e)}")
        # Track error state
        track_process_state(stream_uuid, TaskState.FAILED, {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        })
        raise

def get_stream_lock(stream_uuid: str) -> threading.Lock:
    """Get or create a lock for stream operations"""
    if stream_uuid not in stream_locks:
        stream_locks[stream_uuid] = threading.Lock()
    return stream_locks[stream_uuid]

def log_task_event(stream_uuid: str, event_type: str, details: Dict[str, Any], status: str) -> None:
    """Helper function to log task events"""
    try:
        stream = mongo.db.streams_v2.find_one({"uuid": stream_uuid})
        if not stream:
            logger.error(f"Stream {stream_uuid} not found for logging")
            return

        log_message_cycle(
            message_uuid=stream_uuid,
            event_type=f"stream_{event_type}",
            details=details,
            status=status,
            organization_uuid=stream.get('organization_uuid')
        )
    except Exception as e:
        logger.error(f"Error logging task event: {str(e)}")

def update_stream_status(stream_uuid: str, status: str, error: Optional[str] = None) -> bool:
    """Update stream status"""
    try:
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow()
        }
        
        if error:
            update_data["last_error"] = error

        result = mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {"$set": update_data}
        )
        return result.modified_count > 0
    except Exception as e:
        logger.error(f"Error updating stream status: {str(e)}")
        return False

@celery.task(
    name="tasks.process_stream_messages",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    queue="stream_processing"
)
def process_stream_messages(self, stream_uuid: str):
    """Process pending messages for a stream"""
    logger.info(f"Starting message processing for stream {stream_uuid}")
    stream_lock = get_stream_lock(stream_uuid)

    try:
        with stream_lock, current_app.app_context():
            # Get stream configuration
            stream_config = mongo.db.streams_v2.find_one({
                "uuid": stream_uuid,
                "active": True,
                "deleted": {"$ne": True}
            })
            
            if not stream_config:
                logger.warning(f"Stream {stream_uuid} not found or inactive")
                return

            # Initialize processor and process messages
            processor = StreamMessageProcessor(stream_config)
            pending_messages = processor.get_pending_messages()
            
            processed = 0
            failed = 0
            
            for message in pending_messages:
                try:
                    success, error = processor.process_message(message)
                    if success:
                        processed += 1
                    else:
                        failed += 1
                        logger.error(f"Failed to process message {message.get('uuid')}: {error}")
                except Exception as e:
                    failed += 1
                    logger.error(f"Error processing message: {str(e)}")

            # Update stream metrics
            mongo.db.streams_v2.update_one(
                {"uuid": stream_uuid},
                {
                    "$inc": {
                        "metrics.messages_processed": processed,
                        "metrics.messages_failed": failed
                    },
                    "$set": {
                        "metrics.last_processed": datetime.utcnow(),
                        "last_active": datetime.utcnow()
                    }
                }
            )

    except Exception as e:
        logger.error(f"Error in stream processing: {str(e)}")
        update_stream_status(stream_uuid, "error", str(e))
        raise

@celery.task(
    name="tasks.process_destination_queue",
    queue="destination_processing"
)
def process_destination_queue():
    """Process messages in destination queue"""
    logger.info("Starting destination queue processing")
    
    try:
        # Get pending messages grouped by organization
        pending_messages = mongo.db.destination_queue.aggregate([
            {
                "$match": {
                    "status": "pending",
                    "retries": {"$lt": 3}
                }
            },
            {
                "$group": {
                    "_id": "$organization_uuid",
                    "messages": {"$push": "$$ROOT"}
                }
            }
        ])

        for group in pending_messages:
            org_uuid = group['_id']
            messages = group['messages']
            
            # Create processor for this organization
            processor = DestinationProcessor(org_uuid)
            
            # Process messages
            for message in messages:
                try:
                    # Update status to processing
                    mongo.db.destination_queue.update_one(
                        {"uuid": message["uuid"]},
                        {
                            "$set": {
                                "status": "processing",
                                "processing_started": datetime.utcnow()
                            }
                        }
                    )

                    # Process message - removed await
                    success, error = processor.process_message(message)
                    
                    if success:
                        # Update status to completed
                        mongo.db.destination_queue.update_one(
                            {"uuid": message["uuid"]},
                            {
                                "$set": {
                                    "status": "completed",
                                    "processing_completed": datetime.utcnow()
                                }
                            }
                        )
                    else:
                        # Update retry count
                        mongo.db.destination_queue.update_one(
                            {"uuid": message["uuid"]},
                            {
                                "$inc": {"retries": 1},
                                "$set": {
                                    "status": "pending",
                                    "last_error": error,
                                    "last_error_time": datetime.utcnow()
                                }
                            }
                        )

                except Exception as e:
                    logger.error(f"Error processing message {message['uuid']}: {str(e)}")
                    continue

    except Exception as e:
        logger.error(f"Error processing destination queue: {str(e)}")

@celery.task(
    name="tasks.monitor_streams",
    queue="stream_monitoring"
)
def monitor_streams():
    """Monitor and coordinate stream processing"""
    logger.info("Starting stream monitoring")
    
    try:
        # Get active streams
        active_streams = mongo.db.streams_v2.find({
            "status": "active",
            "deleted": {"$ne": True}
        })

        for stream in active_streams:
            try:
                # Queue stream processing
                process_stream_messages.delay(stream["uuid"])
            except Exception as e:
                logger.error(f"Error monitoring stream {stream['uuid']}: {str(e)}")

        # Monitor destination queue
        process_destination_queue.delay()

    except Exception as e:
        logger.error(f"Error in stream monitoring: {str(e)}")

# Initialize Celery Beat Schedule
def init_stream_tasks(app):
    """Initialize stream-related Celery beat tasks"""
    app.conf.beat_schedule.update({
        'monitor-streams': {
            'task': 'app.streamsv2.core.stream_tasks.monitor_streams',
            'schedule': app.config['STREAMSV2_PROCESSING_INTERVAL'],
            'options': {'queue': 'stream_monitoring'}
        },
        'process-destination-queue': {
            'task': 'app.streamsv2.core.stream_tasks.process_destination_queue',
            'schedule': 60.0,  # Run every minute
            'options': {'queue': 'destination_processing'}
        }
    })

    logger.info("Initialized stream tasks with beat schedule")