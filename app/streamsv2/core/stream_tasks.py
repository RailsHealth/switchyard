# app/streamsv2/core/stream_tasks.py

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
import json
from flask import current_app
from uuid import uuid4
from celery import chain, signature

from app.extensions.celery_ext import celery
from app.extensions import mongo
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.models.stream_status import StreamStatus, MessageStatus
from app.streamsv2.services.destination_handler import DestinationHandler
from app.streamsv2.services.message_qualifier import MessageQualifier
from app.streamsv2.utils.stream_logger import StreamLogger
from app.streamsv2.core.transformer import MessageTransformer 

logger = logging.getLogger(__name__)

def update_task_status(task_id: str, status: str, details: Optional[Dict] = None) -> None:
    """Update task status in MongoDB"""
    try:
        mongo.db.stream_tasks.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "status": status,
                    "updated_at": datetime.utcnow(),
                    **(details or {})
                }
            },
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error updating task status: {str(e)}")

@celery.task(name="streamsv2.monitor_streams", 
         queue="stream_monitoring",
         bind=True,
         max_retries=3)
def monitor_streams(self) -> None:
    """Monitor active streams and initiate processing chains"""
    logger.info("Starting stream monitoring cycle")
    cycle_start = datetime.utcnow()
    
    try:
        # Get active streams with atomic status check
        active_streams = list(mongo.db.streams_v2.find({
            "status": StreamStatus.ACTIVE,
            "active": True,
            "deleted": {"$ne": True},
            "$or": [
                {"last_processing_attempt": {"$exists": False}},
                {"last_processing_attempt": {
                    "$lte": datetime.utcnow() - timedelta(seconds=60)
                }}
            ]
        }))
        
        logger.info(f"Found {len(active_streams)} active streams")

        for stream in active_streams:
            try:
                # Check if there's an existing incomplete chain
                existing_chain = mongo.db.stream_tasks.find_one({
                    "stream_uuid": stream['uuid'],
                    "status": {"$in": ["INITIATED", "PROCESSING"]},
                    "created_at": {"$gte": datetime.utcnow() - timedelta(minutes=5)}
                })

                if existing_chain:
                    logger.info(f"Existing active chain found for stream {stream['uuid']}")
                    continue

                logger.info(f"Starting processing chain for stream {stream['uuid']}")

                # Create new task chain tracking document
                chain_id = str(uuid4())
                task_tracking = {
                    "chain_id": chain_id,
                    "stream_uuid": stream['uuid'],
                    "tasks": [
                        {
                            "name": "process_messages",
                            "status": "PENDING",
                            "task_id": None,
                            "start_time": None
                        },
                        {
                            "name": "transform_messages",
                            "status": "PENDING",
                            "task_id": None,
                            "start_time": None
                        },
                        {
                            "name": "process_destinations",
                            "status": "PENDING",
                            "task_id": None,
                            "start_time": None
                        }
                    ],
                    "endpoints": {
                        "source": stream['source_endpoint_uuid'],
                        "destination": stream['destination_endpoint_uuid']
                    },
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "status": "INITIATED",
                    "error": None
                }

                mongo.db.stream_tasks.insert_one(task_tracking)

                # Define tasks with proper queue assignments
                process_task = process_stream_messages.si(stream['uuid']).set(
                    queue='stream_processing'
                )
                transform_task = transform_stream_messages.s().set(
                    queue='stream_processing'
                )
                deliver_task = process_stream_destinations.s().set(
                    queue='delivery_queue'
                )

                # Create and execute the chain with error handler
                task_chain = chain(
                    process_task,
                    transform_task,
                    deliver_task
                )

                result = task_chain.apply_async(
                    link_error=handle_chain_error.si(chain_id, stream['uuid'])
                )

                logger.info(f"Task chain initiated for stream {stream['uuid']} - Chain ID: {chain_id}")

                # Update stream monitoring info
                mongo.db.streams_v2.update_one(
                    {"uuid": stream['uuid']},
                    {
                        "$set": {
                            "last_processing_attempt": datetime.utcnow(),
                            "monitoring": {
                                "last_check": datetime.utcnow(),
                                "status": "processing",
                                "current_chain_id": chain_id,
                                "task_chain": {
                                    "process_task_id": None,
                                    "transform_task_id": None,
                                    "deliver_task_id": None,
                                    "last_successful_task": None
                                }
                            }
                        }
                    }
                )

            except Exception as stream_error:
                logger.error(f"Error processing stream {stream.get('uuid')}: {str(stream_error)}")
                continue

        # Log monitoring cycle completion
        cycle_duration = (datetime.utcnow() - cycle_start).total_seconds()
        logger.info(f"Monitoring cycle completed in {cycle_duration:.2f}s")

    except Exception as e:
        logger.error(f"Error in stream monitoring: {str(e)}")
        raise self.retry(exc=e, countdown=30)


def handle_chain_error(self, exc, task_id, stream_uuid: str):
    """Handle chain execution errors with cancellation support"""
    logger.error(f"Chain error for stream {stream_uuid}: {exc}")
    try:
        # Get current stream status to check if this is a planned stop
        stream = mongo.db.streams_v2.find_one({"uuid": stream_uuid})
        if stream and stream.get('status') == StreamStatus.STOPPING:
            logger.info(f"Stream {stream_uuid} was intentionally stopped, not treating as error")
            return

        # Update stream status for unplanned errors
        mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {
                "$set": {
                    "status": StreamStatus.ERROR,
                    "last_error": str(exc),
                    "last_error_time": datetime.utcnow()
                }
            }
        )
        
        # Log error event
        StreamLogger(stream_uuid).log_event(
            "processing_chain_error",
            {
                "error": str(exc),
                "task_id": task_id
            },
            "error"
        )
    except Exception as e:
        logger.error(f"Error handling chain error: {str(e)}")

@celery.task(name="streamsv2.process_messages_chain", bind=True, max_retries=3)
def process_messages_chain(self, stream_uuid: str):
    """Initiates chain of stream processing tasks with improved error handling"""
    logger.info(f"Starting processing chain for stream {stream_uuid}")
    
    try:
        # Verify stream exists and is active with atomic check
        stream = mongo.db.streams_v2.find_one_and_update(
            {
                "uuid": stream_uuid,
                "status": StreamStatus.ACTIVE,
                "deleted": {"$ne": True}
            },
            {
                "$set": {
                    "last_processing_attempt": datetime.utcnow()
                }
            },
            return_document=True
        )
        
        if not stream:
            logger.error(f"Stream {stream_uuid} not found, not active, or deleted")
            return
        
        # Create processing chain with immutable signatures
        processing_chain = chain(
            process_stream_messages.si(stream_uuid),  # First task with explicit uuid
            transform_stream_messages.s(),  # Subsequent tasks get uuid from previous task
            process_stream_destinations.s()
        )
        
        # Apply the chain with error handler
        logger.info(f"Applying task chain for stream {stream_uuid}")
        result = processing_chain.apply_async(
            queue='stream_processing',
            link_error=handle_chain_error.s(stream_uuid)
        )
        logger.info(f"Task chain initiated with ID: {result.id}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error initiating processing chain: {str(e)}")
        
        try:
            # Update stream with error info
            mongo.db.streams_v2.update_one(
                {"uuid": stream_uuid},
                {
                    "$set": {
                        "last_error": str(e),
                        "last_error_time": datetime.utcnow()
                    }
                }
            )
        except Exception as update_error:
            logger.error(f"Error updating stream error info: {str(update_error)}")
        
        # Retry the task
        raise self.retry(exc=e, countdown=60)  # Retry after 1 minute

@celery.task(name="streamsv2.process_stream_messages", bind=True)
def process_stream_messages(self, stream_uuid: str) -> str:
    """Process messages for stream and return stream_uuid for chaining"""
    task_id = self.request.id
    logger.info(f"Starting message processing for stream {stream_uuid} (Task ID: {task_id})")
    
    try:
        # Update task status
        update_task_status(task_id, "STARTED", {
            "stream_uuid": stream_uuid,
            "start_time": datetime.utcnow().isoformat()
        })

        # Get stream configuration
        stream = mongo.db.streams_v2.find_one({"uuid": stream_uuid})
        if not stream:
            raise ValueError(f"Stream {stream_uuid} not found")

        # Filter stream document to only include fields expected by StreamConfig
        allowed_fields = {
            'uuid', 'name', 'organization_uuid', 'source_endpoint_uuid',
            'destination_endpoint_uuid', 'message_type', 'status', 'active',
            'created_at', 'updated_at', 'metrics', 'last_error', 'last_error_time'
        }
        
        filtered_stream = {k: v for k, v in stream.items() if k in allowed_fields}
        if '_id' in filtered_stream:
            del filtered_stream['_id']
        
        # Create stream config from filtered data
        config = StreamConfig(**filtered_stream)
        qualifier = MessageQualifier(config)
        
        # Find qualifying messages
        qualifying_messages = qualifier.find_qualifying_messages()
        qualifying_count = len(qualifying_messages)
        logger.info(f"Found {qualifying_count} qualifying messages")
        
        if qualifying_count > 0:
            # Mark messages as qualified
            qualified_count = qualifier.mark_messages_qualified(qualifying_messages)
            logger.info(f"Successfully qualified {qualified_count} messages")
            
            # Update stream metrics
            mongo.db.streams_v2.update_one(
                {"uuid": stream_uuid},
                {
                    "$inc": {
                        "metrics.messages_qualified": qualified_count
                    },
                    "$set": {
                        "metrics.last_qualification_time": datetime.utcnow(),
                        "monitoring.task_chain.process_task_id": task_id,
                        "monitoring.last_successful_task": "process_messages"
                    }
                }
            )

        # Update task completion status
        update_task_status(task_id, "COMPLETED", {
            "messages_found": qualifying_count,
            "messages_qualified": qualified_count if qualifying_count > 0 else 0,
            "stream_uuid": stream_uuid,
            "completion_time": datetime.utcnow().isoformat()
        })

        # Update last processing attempt
        mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {
                "$set": {
                    "last_processing_attempt": datetime.utcnow()
                }
            }
        )

        return stream_uuid

    except Exception as e:
        logger.error(f"Error processing stream {stream_uuid}: {str(e)}", exc_info=True)
        update_task_status(task_id, "FAILED", {
            "error": str(e),
            "stream_uuid": stream_uuid
        })
        
        # Update stream with error information
        mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {
                "$set": {
                    "monitoring.last_error": str(e),
                    "monitoring.last_error_time": datetime.utcnow(),
                    "monitoring.task_chain.last_failed_task": "process_messages"
                }
            }
        )
        
        raise self.retry(exc=e, countdown=180)


@celery.task(name="streamsv2.transform_stream_messages", bind=True)
def transform_stream_messages(self, stream_uuid: str) -> str:
    """Transform qualified messages and return stream_uuid for chaining"""
    task_id = self.request.id
    logger.info(f"Starting transformation task for stream {stream_uuid} (Task ID: {task_id})")
    
    try:
        update_task_status(task_id, "STARTED", {
            "stream_uuid": stream_uuid,
            "start_time": datetime.utcnow().isoformat()
        })

        # Get stream configuration
        stream = mongo.db.streams_v2.find_one({"uuid": stream_uuid})
        if not stream:
            raise ValueError(f"Stream {stream_uuid} not found")

        # Filter stream document to only include fields expected by StreamConfig
        allowed_fields = {
            'uuid', 'name', 'organization_uuid', 'source_endpoint_uuid',
            'destination_endpoint_uuid', 'message_type', 'status', 'active',
            'created_at', 'updated_at', 'metrics', 'last_error', 'last_error_time'
        }
        
        filtered_stream = {k: v for k, v in stream.items() if k in allowed_fields}
        if '_id' in filtered_stream:
            del filtered_stream['_id']
            
        config = StreamConfig(**filtered_stream)
        transformer = MessageTransformer(config)
        stream_logger = StreamLogger(stream_uuid)
        
        # Get qualified messages
        qualified_messages = list(mongo.db.messages.find({
            "stream_tracker": {
                "$elemMatch": {
                    "stream_uuid": stream_uuid,
                    "transformation_status": MessageStatus.QUALIFIED
                }
            }
        }))
        
        logger.info(f"Found {len(qualified_messages)} qualified messages to transform")
        
        transformed_count = 0
        for message in qualified_messages:
            try:
                # Transform message
                transformed = transformer.transform_message(message)
                if transformed:
                    # Create destination message
                    dest_message = {
                        "uuid": str(uuid4()),
                        "original_message_uuid": message['uuid'],
                        "stream_uuid": stream_uuid,
                        "organization_uuid": config.organization_uuid,
                        "transformed_content": transformed,
                        "destinations": [{
                            "endpoint_uuid": config.destination_endpoint_uuid,
                            "status": "READY",
                            "processing_details": {
                                "attempts": 0,
                                "last_attempt": None
                            }
                        }],
                        "overall_status": "PENDING",
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                    
                    if mongo.db.destination_messages.insert_one(dest_message):
                        transformed_count += 1
                        # Update message status
                        mongo.db.messages.update_one(
                            {
                                "uuid": message['uuid'],
                                "stream_tracker.stream_uuid": stream_uuid
                            },
                            {
                                "$set": {
                                    "stream_tracker.$.transformation_status": MessageStatus.TRANSFORMED,
                                    "stream_tracker.$.updated_at": datetime.utcnow()
                                }
                            }
                        )
                        logger.info(f"Successfully transformed message {message['uuid']}")
            except Exception as message_e:
                logger.error(f"Error transforming message {message['uuid']}: {str(message_e)}")
                stream_logger.log_event(
                    "message_transform_failed",
                    {
                        "message_uuid": message['uuid'],
                        "error": str(message_e)
                    },
                    "error"
                )
        
        logger.info(f"Transformed {transformed_count} messages")
        
        # Update task status
        update_task_status(task_id, "COMPLETED", {
            "transformed_count": transformed_count,
            "total_messages": len(qualified_messages),
            "completion_time": datetime.utcnow().isoformat()
        })

        # Update stream monitoring info
        mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {
                "$set": {
                    "monitoring.task_chain.transform_task_id": task_id,
                    "monitoring.last_successful_task": "transform_messages"
                }
            }
        )

        return stream_uuid

    except Exception as e:
        logger.error(f"Error in transform task for stream {stream_uuid}: {str(e)}")
        update_task_status(task_id, "FAILED", {"error": str(e)})
        
        # Update stream with error information
        mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {
                "$set": {
                    "monitoring.last_error": str(e),
                    "monitoring.last_error_time": datetime.utcnow(),
                    "monitoring.task_chain.last_failed_task": "transform_messages"
                }
            }
        )
        
        raise self.retry(exc=e)

@celery.task(name="streamsv2.process_delivery_retries",
             queue="delivery_queue",
             bind=True,
             max_retries=3)
def process_delivery_retries(self) -> None:
    """Process message delivery retries"""
    task_id = self.request.id
    update_task_status(task_id, "STARTED")
    
    try:
        # Get messages that need retry
        streams_requiring_retry = mongo.db.streams_v2.find({
            "status": "ACTIVE",
            "deleted": {"$ne": True}
        })

        retry_stats = {
            "total_retries": 0,
            "successful_retries": 0,
            "failed_retries": 0,
            "streams_processed": 0
        }

        for stream_doc in streams_requiring_retry:
            try:
                # Create StreamConfig and processor
                config = StreamConfig(**stream_doc)
                processor = StreamProcessor(config)
                
                # Process retries for this stream
                successful, failed = processor.handle_retry_messages()
                
                retry_stats["total_retries"] += successful + failed
                retry_stats["successful_retries"] += successful
                retry_stats["failed_retries"] += failed
                retry_stats["streams_processed"] += 1

                logger.info(
                    f"Processed retries for stream {config.uuid}: "
                    f"{successful} successful, {failed} failed"
                )

            except Exception as stream_error:
                logger.error(f"Error processing retries for stream {stream_doc.get('uuid')}: {str(stream_error)}")
                continue

        # Update task status with stats
        update_task_status(task_id, "COMPLETED", retry_stats)

    except Exception as e:
        logger.error(f"Error processing delivery retries: {str(e)}")
        update_task_status(task_id, "FAILED", {"error": str(e)})
        raise self.retry(exc=e)

@celery.task(name="streamsv2.process_stream_destinations", 
             bind=True,
             max_retries=3,
             queue="delivery_queue")
def process_stream_destinations(self, stream_uuid: str) -> str:
    """Process destinations for stream's transformed messages"""
    logger.info(f"[DELIVERY-TASK-START] Starting destination processing for stream {stream_uuid}")
    task_id = self.request.id
    logger.info(f"[DELIVERY-QUEUE-CHECK] Task {task_id} started in queue: {self.request.delivery_info.get('routing_key')}")
    logger.info(f"[DELIVERY-TASK-START] Starting destination processing for stream {stream_uuid}")
    chain_id = self.request.chain
    
    try:
        # Update task tracking status
        logger.info(f"[DELIVERY-TASK-STATUS] Updating task status to PROCESSING for chain {chain_id}")
        mongo.db.stream_tasks.update_one(
            {"chain_id": chain_id},
            {
                "$set": {
                    "tasks.$[task].status": "PROCESSING",
                    "tasks.$[task].task_id": task_id,
                    "tasks.$[task].start_time": datetime.utcnow(),
                    "status": "PROCESSING",
                    "updated_at": datetime.utcnow()
                }
            },
            array_filters=[{"task.name": "process_destinations"}]
        )

        # Get stream data first
        logger.info(f"[DELIVERY-TASK-FETCH] Fetching stream data for {stream_uuid}")
        stream = mongo.db.streams_v2.find_one({"uuid": stream_uuid})
        if not stream:
            logger.error(f"[DELIVERY-TASK-ERROR] Stream {stream_uuid} not found")
            raise ValueError(f"Stream {stream_uuid} not found")

        # Import within task to avoid circular dependency
        logger.info(f"[DELIVERY-TASK-IMPORT] Importing required modules")
        from app.streamsv2.models.stream_config import StreamConfig
        from app.streamsv2.core.stream_processor import StreamProcessor

        logger.info(f"[DELIVERY-TASK-INIT] Initializing stream configuration for {stream_uuid}")
        # Initialize stream configuration
        config = StreamConfig.from_dict(stream)
        
        logger.info(f"[DELIVERY-TASK-PROCESSOR] Creating StreamProcessor instance for {stream_uuid}")
        # Initialize processor
        processor = StreamProcessor(config)

        logger.info(f"[DELIVERY-TASK-PROCESS] Starting message processing for stream {stream_uuid}")
        # Process destination messages
        processed_count, error_count = processor.process_destination_messages()
        logger.info(f"[DELIVERY-TASK-METRICS] Processed {processed_count} messages, {error_count} errors for {stream_uuid}")

        # Update task completion status
        logger.info(f"[DELIVERY-TASK-UPDATE] Updating task completion status for chain {chain_id}")
        mongo.db.stream_tasks.update_one(
            {"chain_id": chain_id},
            {
                "$set": {
                    "tasks.$[task].status": "COMPLETED",
                    "tasks.$[task].completion_time": datetime.utcnow(),
                    "status": "COMPLETED",
                    "updated_at": datetime.utcnow(),
                    "stats": {
                        "processed_count": processed_count,
                        "error_count": error_count
                    }
                }
            },
            array_filters=[{"task.name": "process_destinations"}]
        )

        # Update stream metrics
        logger.info(f"[DELIVERY-TASK-METRICS-UPDATE] Updating stream metrics for {stream_uuid}")
        mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {
                "$set": {
                    "monitoring.task_chain.deliver_task_id": task_id,
                    "monitoring.last_successful_task": "process_destinations",
                    "monitoring.status": "active",
                    "monitoring.last_completion": datetime.utcnow()
                },
                "$inc": {
                    "metrics.messages_delivered": processed_count,
                    "metrics.delivery_errors": error_count
                }
            }
        )

        logger.info(f"[DELIVERY-TASK-COMPLETE] Completed destination processing for stream {stream_uuid}")
        return stream_uuid

    except Exception as e:
        error_msg = f"Error in destination processing for stream {stream_uuid}: {str(e)}"
        logger.error(f"[DELIVERY-TASK-ERROR] {error_msg}", exc_info=True)
        
        # Update error status
        try:
            logger.info(f"[DELIVERY-TASK-ERROR-UPDATE] Updating error status for chain {chain_id}")
            mongo.db.stream_tasks.update_one(
                {"chain_id": chain_id},
                {
                    "$set": {
                        "tasks.$[task].status": "ERROR",
                        "tasks.$[task].error": str(e),
                        "status": "ERROR",
                        "error": str(e),
                        "updated_at": datetime.utcnow()
                    }
                },
                array_filters=[{"task.name": "process_destinations"}]
            )

            # Update stream error status
            logger.info(f"[DELIVERY-TASK-STREAM-ERROR] Updating stream error status for {stream_uuid}")
            mongo.db.streams_v2.update_one(
                {"uuid": stream_uuid},
                {
                    "$set": {
                        "monitoring.last_error": str(e),
                        "monitoring.last_error_time": datetime.utcnow(),
                        "monitoring.task_chain.last_failed_task": "process_destinations"
                    }
                }
            )
        except Exception as update_error:
            logger.error(f"[DELIVERY-TASK-ERROR-UPDATE-FAILED] Error updating failure status: {str(update_error)}")

        # Get retry configuration and retry
        retry_delay = current_app.config.get('STREAMSV2_SETTINGS', {}).get('RETRY_DELAY', 300)
        logger.info(f"[DELIVERY-TASK-RETRY] Retrying task in {retry_delay} seconds for stream {stream_uuid}")
        raise self.retry(exc=e, countdown=retry_delay)

@celery.task(name="streamsv2.handle_chain_error",
             bind=True)
def handle_chain_error(self, exc, task_id, stream_uuid: str):
    """Handle chain execution errors with registry awareness"""
    logger.error(f"Chain error for stream {stream_uuid}: {exc}")
    try:
        error_details = {
            "error": str(exc),
            "task_id": task_id,
            "timestamp": datetime.utcnow()
        }

        # Get stream configuration
        stream = mongo.db.streams_v2.find_one({"uuid": stream_uuid})
        if stream:
            config = StreamConfig(**stream)
            processor = StreamProcessor(config)

            # Stop endpoints if needed
            try:
                if processor.endpoint_registry:
                    # Stop source endpoint
                    source_endpoint = processor.endpoint_registry.get_endpoint(config.source_endpoint_uuid)
                    if source_endpoint and source_endpoint.config.active:
                        processor.endpoint_registry.stop_endpoint(config.source_endpoint_uuid)

                    # Stop destination endpoint
                    dest_endpoint = processor.endpoint_registry.get_endpoint(config.destination_endpoint_uuid)
                    if dest_endpoint and dest_endpoint.config.active:
                        processor.endpoint_registry.stop_endpoint(config.destination_endpoint_uuid)
            except Exception as stop_error:
                logger.error(f"Error stopping endpoints: {str(stop_error)}")

        # Update stream status
        update_result = mongo.db.streams_v2.update_one(
            {"uuid": stream_uuid},
            {
                "$set": {
                    "status": StreamStatus.ERROR,
                    "active": False,
                    "updated_at": datetime.utcnow(),
                    "monitoring": {
                        "last_error": str(exc),
                        "last_error_time": datetime.utcnow(),
                        "last_failed_task_id": task_id,
                        "error_details": error_details
                    }
                },
                "$inc": {
                    "metrics.error_count": 1
                }
            }
        )
        
        if not update_result.modified_count:
            logger.error(f"Failed to update stream {stream_uuid} status for error handling")

        # Get current task chain status
        stream_tasks = mongo.db.stream_tasks.find_one({
            "stream_uuid": stream_uuid,
            "status": "INITIATED"
        })

        if stream_tasks:
            # Update task chain status
            mongo.db.stream_tasks.update_one(
                {"_id": stream_tasks["_id"]},
                {
                    "$set": {
                        "status": "FAILED",
                        "error": error_details,
                        "updated_at": datetime.utcnow()
                    }
                }
            )

        # Log error event
        stream_logger = StreamLogger(stream_uuid)
        stream_logger.log_event(
            "processing_chain_error",
            {
                "error": str(exc),
                "task_id": task_id,
                "task_chain_id": stream_tasks.get("chain_id") if stream_tasks else None,
                "status": "error"
            },
            "error"
        )

        # Try to clean up any pending tasks
        try:
            cleanup_pending_tasks(stream_uuid)
        except Exception as cleanup_error:
            logger.error(f"Error during task cleanup for stream {stream_uuid}: {str(cleanup_error)}")

    except Exception as e:
        logger.error(f"Error handling chain error for stream {stream_uuid}: {str(e)}")

def cleanup_pending_tasks(stream_uuid: str) -> None:
    """Helper function to clean up pending tasks for a stream"""
    try:
        # Mark any queued messages as failed
        mongo.db.messages.update_many(
            {
                "stream_tracker": {
                    "$elemMatch": {
                        "stream_uuid": stream_uuid,
                        "transformation_status": {"$in": [MessageStatus.QUEUED, MessageStatus.QUALIFIED]}
                    }
                }
            },
            {
                "$set": {
                    "stream_tracker.$.transformation_status": MessageStatus.ERROR,
                    "stream_tracker.$.updated_at": datetime.utcnow(),
                    "stream_tracker.$.error": "Chain execution failed"
                }
            }
        )

        # Update any pending destination messages
        mongo.db.destination_messages.update_many(
            {
                "stream_uuid": stream_uuid,
                "overall_status": "PENDING"
            },
            {
                "$set": {
                    "overall_status": "FAILED",
                    "error": "Chain execution failed",
                    "updated_at": datetime.utcnow()
                }
            }
        )

    except Exception as e:
        logger.error(f"Error cleaning up pending tasks: {str(e)}")

@celery.task(name="streamsv2.cleanup_completed_deliveries",
             queue="stream_maintenance",
             bind=True)
def cleanup_completed_deliveries(self, hours: int = 24) -> None:
    """Clean up successfully completed delivery records"""
    task_id = self.request.id
    update_task_status(task_id, "STARTED", {"retention_hours": hours})
    
    try:
        cleanup_stats = {
            "streams_processed": 0,
            "messages_cleaned": 0,
            "failed_streams": 0
        }

        # Get active streams
        active_streams = mongo.db.streams_v2.find({
            "status": "ACTIVE",
            "deleted": {"$ne": True}
        })

        for stream_doc in active_streams:
            try:
                config = StreamConfig(**stream_doc)
                processor = StreamProcessor(config)
                
                # Cleanup messages for this stream
                cleaned_count = processor.cleanup_delivered_messages(days=int(hours/24))
                
                if cleaned_count > 0:
                    cleanup_stats["messages_cleaned"] += cleaned_count
                    cleanup_stats["streams_processed"] += 1

            except Exception as stream_error:
                logger.error(f"Error cleaning up stream {stream_doc.get('uuid')}: {str(stream_error)}")
                cleanup_stats["failed_streams"] += 1
                continue

        logger.info(
            f"Cleanup completed: {cleanup_stats['messages_cleaned']} messages cleaned "
            f"across {cleanup_stats['streams_processed']} streams"
        )
        
        update_task_status(task_id, "COMPLETED", cleanup_stats)

    except Exception as e:
        logger.error(f"Error in cleanup task: {str(e)}")
        update_task_status(task_id, "FAILED", {"error": str(e)})
        raise self.retry(exc=e)

@celery.task(name="streamsv2.handle_chain_error",
             bind=True)

@celery.task(name="streamsv2.process_stuck_destinations",
             queue="stream_maintenance",
             bind=True)
def process_stuck_destinations(self) -> None:
    """Process stuck destination messages"""
    task_id = self.request.id
    update_task_status(task_id, "STARTED")
    
    try:
        timeout = datetime.utcnow() - timedelta(minutes=30)
        
        # Find stuck processing messages
        stuck_messages = list(mongo.db.destination_messages.find({
            "destinations": {
                "$elemMatch": {
                    "status": "PROCESSING",
                    "processing_details.last_attempt": {"$lt": timeout}
                }
            }
        }))

        processed_count = 0
        requeued_count = 0
        
        for message in stuck_messages:
            try:
                stream_logger = StreamLogger(message['stream_uuid'])
                
                # Reset status for retry
                result = mongo.db.destination_messages.update_one(
                    {"_id": message["_id"]},
                    {
                        "$set": {
                            "destinations.$[elem].status": "READY",
                            "destinations.$[elem].processing_details.last_attempt": None,
                            "destinations.$[elem].processing_details.error": "Message processing timeout",
                            "destinations.$[elem].processing_details.recovery_timestamp": datetime.utcnow()
                        }
                    },
                    array_filters=[{"elem.status": "PROCESSING"}]
                )
                
                if result.modified_count:
                    processed_count += 1
                    
                    # Re-queue for processing if appropriate
                    if message.get('processing_details', {}).get('attempts', 0) < current_app.config['STREAMSV2_MAX_RETRIES']:
                        process_delivery_retries.delay(message['stream_uuid'])
                        requeued_count += 1
                        
                        stream_logger.log_event(
                            "stuck_message_recovered",
                            {
                                "message_uuid": message['uuid'],
                                "requeued": True,
                                "attempts": message.get('processing_details', {}).get('attempts', 0)
                            },
                            "info"
                        )
                    
            except Exception as e:
                logger.error(f"Error processing stuck message {message.get('uuid')}: {str(e)}")

        update_task_status(task_id, "COMPLETED", {
            "stuck_messages": len(stuck_messages),
            "processed_count": processed_count,
            "requeued_count": requeued_count
        })

    except Exception as e:
        logger.error(f"Error processing stuck destinations: {str(e)}")
        update_task_status(task_id, "FAILED", {"error": str(e)})
        raise self.retry(exc=e)

@celery.task(name="streamsv2.check_endpoint_health",
             queue="stream_monitoring",
             bind=True)
def check_endpoint_health(self) -> None:
    task_id = self.request.id
    update_task_status(task_id, "STARTED")
    
    try:
        active_streams = list(mongo.db.streams_v2.find({
            "status": StreamStatus.ACTIVE,
            "deleted": {"$ne": True}
        }))

        affected_streams = 0
        for stream in active_streams:
            try:
                config = StreamConfig(**stream)
                stream_logger = StreamLogger(config.uuid)

                # Check endpoints directly
                source = mongo.db.endpoints.find_one({
                    "uuid": config.source_endpoint_uuid,
                    "deleted": {"$ne": True}
                })
                dest = mongo.db.endpoints.find_one({
                    "uuid": config.destination_endpoint_uuid,
                    "deleted": {"$ne": True}
                })

                if not source or source.get('status') != 'ACTIVE':
                    affected_streams += 1
                    error = "Source endpoint not active"
                elif not dest or dest.get('status') != 'ACTIVE':
                    affected_streams += 1
                    error = "Destination endpoint not active"
                else:
                    continue

                # Log issue
                stream_logger.log_event(
                    "endpoint_health_check_failed",
                    {
                        "error": error,
                        "source_endpoint": config.source_endpoint_uuid,
                        "destination_endpoint": config.destination_endpoint_uuid
                    },
                    "warning"
                )

                # Update stream metrics
                mongo.db.streams_v2.update_one(
                    {"uuid": config.uuid},
                    {
                        "$set": {
                            "metrics.last_error": error,
                            "metrics.last_error_time": datetime.utcnow(),
                            "metrics.endpoint_health_status": "ERROR"
                        }
                    }
                )

            except Exception as e:
                logger.error(f"Error checking endpoints for stream {stream.get('uuid')}: {str(e)}")

        update_task_status(task_id, "COMPLETED", {
            "streams_checked": len(active_streams),
            "affected_streams": affected_streams
        })

    except Exception as e:
        logger.error(f"Error in endpoint health check: {str(e)}")
        update_task_status(task_id, "FAILED", {"error": str(e)})
        raise self.retry(exc=e)

@celery.task(name="streamsv2.update_stream_metrics",
             queue="stream_monitoring",
             bind=True)
def update_stream_metrics(self) -> None:
    """Update comprehensive metrics for active streams"""
    task_id = self.request.id
    update_task_status(task_id, "STARTED")
    
    try:
        active_streams = list(mongo.db.streams_v2.find({
            "status": StreamStatus.ACTIVE,
            "deleted": {"$ne": True}
        }))

        updated_count = 0
        for stream in active_streams:
            try:
                config = StreamConfig(**stream)
                processor = StreamProcessor(config)
                
                # Get comprehensive metrics
                delivery_stats = processor.dest_handler.get_delivery_stats()
                queue_stats = processor.message_tracker.get_message_counts()
                
                # Calculate processing rates and performance metrics
                current_time = datetime.utcnow()
                time_window = timedelta(hours=1)
                recent_messages = mongo.db.message_cycle_logs.count_documents({
                    "stream_uuid": config.uuid,
                    "timestamp": {"$gte": current_time - time_window}
                })
                
                processing_rate = recent_messages / time_window.total_seconds() if time_window.total_seconds() > 0 else 0
                
                # Update stream metrics
                metrics_update = {
                    "metrics": {
                        "delivery_stats": delivery_stats,
                        "queue_stats": queue_stats,
                        "processing_rate": processing_rate,
                        "last_metrics_update": current_time,
                        "recent_messages": recent_messages,
                        "performance": {
                            "success_rate": (delivery_stats.get("completed", 0) / 
                                          (delivery_stats.get("total_messages", 1) or 1) * 100),
                            "error_rate": (delivery_stats.get("failed", 0) / 
                                        (delivery_stats.get("total_messages", 1) or 1) * 100),
                            "average_processing_time": processor.get_performance_metrics().get("average_time", 0)
                        }
                    }
                }
                
                # Update stream document
                result = mongo.db.streams_v2.update_one(
                    {"uuid": config.uuid},
                    {"$set": metrics_update}
                )
                
                if result.modified_count:
                    updated_count += 1
                    logger.info(f"Updated metrics for stream {config.uuid}")

            except Exception as e:
                logger.error(f"Error updating metrics for stream {stream.get('uuid')}: {str(e)}")

        update_task_status(task_id, "COMPLETED", {
            "streams_updated": updated_count,
            "total_streams": len(active_streams)
        })

    except Exception as e:
        logger.error(f"Error updating stream metrics: {str(e)}")
        update_task_status(task_id, "FAILED", {"error": str(e)})
        raise self.retry(exc=e)

def get_performance_metrics(processor):
    """Helper function to calculate performance metrics"""
    try:
        # Get recent message processing times
        current_time = datetime.utcnow()
        time_window = timedelta(minutes=30)
        
        recent_logs = mongo.db.message_cycle_logs.find({
            "stream_uuid": processor.config.uuid,
            "timestamp": {"$gte": current_time - time_window}
        }).sort("timestamp", -1)
        
        processing_times = []
        for log in recent_logs:
            if "processing_time" in log:
                processing_times.append(log["processing_time"])
        
        if not processing_times:
            return {"average_time": 0, "max_time": 0, "min_time": 0}
        
        return {
            "average_time": sum(processing_times) / len(processing_times),
            "max_time": max(processing_times),
            "min_time": min(processing_times)
        }
    except Exception as e:
        logger.error(f"Error calculating performance metrics: {str(e)}")
        return {"average_time": 0, "max_time": 0, "min_time": 0}