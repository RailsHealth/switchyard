import asyncio
from functools import partial
from typing import Dict, Any
from datetime import datetime, timedelta
import logging
from app.extensions.celery_ext import celery
from app.extensions import mongo
from app.streamsv2.services.message_processor import StreamMessageProcessor
from celery.exceptions import Retry

logger = logging.getLogger(__name__)

@celery.task(
    name="tasks.process_stream_messages",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    queue="stream_processing"
)
def process_stream_messages(self, stream_uuid: str):
    """Process pending messages for a specific stream"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_process_stream_messages(self, stream_uuid))

async def _process_stream_messages(self, stream_uuid: str):
    """Async implementation of message processing"""
    try:
        # Get stream configuration
        stream_config = mongo.db.streams_v2.find_one({
            "uuid": stream_uuid,
            "active": True,
            "deleted": {"$ne": True}
        })
        
        if not stream_config:
            logger.warning(f"Stream {stream_uuid} not found or inactive")
            return {
                "processed": 0,
                "failed": 0,
                "stream_uuid": stream_uuid,
                "status": "inactive"
            }
            
        # Initialize message processor
        processor = StreamMessageProcessor(stream_config)
        
        # Get pending messages
        messages = processor.get_pending_messages(limit=100)
        
        processed = 0
        failed = 0
        errors = []
        
        # Process each message
        for message in messages:
            try:
                success, error = await processor.process_message(message)
                if success:
                    processed += 1
                else:
                    failed += 1
                    errors.append({
                        "message_uuid": message['uuid'],
                        "error": error,
                        "timestamp": datetime.utcnow()
                    })
                    logger.error(f"Failed to process message {message['uuid']}: {error}")
                
            except Exception as e:
                failed += 1
                errors.append({
                    "message_uuid": message['uuid'],
                    "error": str(e),
                    "timestamp": datetime.utcnow()
                })
                logger.error(f"Error processing message {message['uuid']}: {str(e)}")
        
        # Update stream metrics
        mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {
                "$inc": {
                    "metrics.messages_processed": processed,
                    "metrics.messages_failed": failed,
                    "metrics.total_processed": processed,
                    "metrics.total_failed": failed
                },
                "$set": {
                    "metrics.last_processed": datetime.utcnow(),
                    "metrics.last_errors": errors[-5:] if errors else []  # Keep last 5 errors
                },
                "$push": {
                    "metrics.processing_history": {
                        "$each": [{
                            "timestamp": datetime.utcnow(),
                            "processed": processed,
                            "failed": failed
                        }],
                        "$slice": -100  # Keep last 100 entries
                    }
                }
            }
        )
        
        return {
            "processed": processed,
            "failed": failed,
            "stream_uuid": stream_uuid,
            "status": "completed",
            "errors": errors
        }
        
    except Exception as e:
        logger.error(f"Error in stream processing task: {str(e)}")
        raise self.retry(exc=e)

@celery.task(
    name="tasks.monitor_streams",
    queue="stream_monitoring"
)
def monitor_streams():
    """Monitor and start processing for active streams"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_monitor_streams())

async def _monitor_streams():
    """Async implementation of stream monitoring"""
    try:
        # Get all active streams
        active_streams = mongo.db.streams_v2.find({
            "active": True,
            "deleted": {"$ne": True}
        })
        
        results = []
        for stream in active_streams:
            try:
                # Check if stream needs processing
                needs_processing = await _check_stream_needs_processing(stream)
                
                if needs_processing:
                    # Queue processing task
                    task = process_stream_messages.apply_async(
                        args=[stream['uuid']],
                        queue=f"stream_{stream['uuid']}"
                    )
                    
                    results.append({
                        "stream_uuid": stream['uuid'],
                        "task_id": task.id,
                        "status": "queued"
                    })
                    
            except Exception as e:
                logger.error(f"Error monitoring stream {stream['uuid']}: {str(e)}")
                results.append({
                    "stream_uuid": stream['uuid'],
                    "error": str(e),
                    "status": "error"
                })
        
        return results
        
    except Exception as e:
        logger.error(f"Error in stream monitoring: {str(e)}")
        return {"status": "error", "error": str(e)}

async def _check_stream_needs_processing(stream: Dict[str, Any]) -> bool:
    """Async check if stream has pending messages"""
    try:
        # Initialize processor to get message filter
        processor = StreamMessageProcessor(stream)
        
        # Check for any pending messages
        pending_count = mongo.db.messages.count_documents(
            processor.get_message_filter()
        )
        
        return pending_count > 0
        
    except Exception as e:
        logger.error(f"Error checking stream {stream['uuid']}: {str(e)}")
        return False

@celery.task(
    name="tasks.cleanup_stream_processed_messages",
    queue="stream_maintenance"
)
def cleanup_stream_processed_messages(days: int = 30):
    """Cleanup old stream processing records"""
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        # Remove old processing records but keep message
        result = mongo.db.messages.update_many(
            {
                "processed_by_streams.processed_at": {"$lt": cutoff}
            },
            {
                "$pull": {
                    "processed_by_streams": {
                        "processed_at": {"$lt": cutoff}
                    }
                }
            }
        )
        
        logger.info(f"Cleaned up {result.modified_count} stream processing records")
        
        return {
            "status": "success",
            "records_cleaned": result.modified_count,
            "cutoff_date": cutoff.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up stream processing records: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

# Add to celery beat schedule
celery.conf.beat_schedule.update({
    'monitor-streams': {
        'task': 'tasks.monitor_streams',
        'schedule': 60.0  # Run every minute
    },
    'cleanup-stream-processed-messages': {
        'task': 'tasks.cleanup_stream_processed_messages',
        'schedule': 3600 * 24  # Run daily
    }
})