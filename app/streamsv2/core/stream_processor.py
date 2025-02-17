from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
import logging
import json
from flask import current_app
from uuid import uuid4
from celery import chain

from app.extensions import mongo
from app.extensions.celery_ext import celery
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.models.destination_message import DestinationMessage
from app.streamsv2.utils.stream_logger import StreamLogger
from app.streamsv2.services.message_qualifier import MessageQualifier
from app.streamsv2.core.transformer import MessageTransformer
from app.streamsv2.services.destination_handler import DestinationHandler
from app.streamsv2.services.monitoring import StreamMonitor
from app.streamsv2.models.stream_status import StreamStatus, MessageStatus
from app.streamsv2.core.stream_tasks import (
    process_stream_messages,
    transform_stream_messages,
    process_stream_destinations,
    handle_chain_error
)

logger = logging.getLogger(__name__)

class StreamProcessorError(Exception):
    """Base exception for stream processing errors"""
    pass

class StreamProcessor:
    def __init__(self, config: StreamConfig):
        """Initialize stream processor"""
        self.config = config
        self.qualifier = MessageQualifier(self.config)
        self.transformer = MessageTransformer(self.config)
        self.dest_handler = DestinationHandler(self.config)
        self.monitor = StreamMonitor(self.config)
        self.logger = StreamLogger(self.config.uuid)
        
        # Get endpoint registry
        from app.endpoints.registry import endpoint_registry
        self.endpoint_registry = endpoint_registry
        
        # Processing configuration
        self.batch_size = current_app.config.get('STREAMSV2_SETTINGS', {}).get('BATCH_SIZE', 100)
        self.processing_timeout = current_app.config.get('STREAMSV2_SETTINGS', {}).get('PROCESSING_TIMEOUT', 300)
        self._performance_metrics = {}
        # Add max_retries from config
        self.max_retries = current_app.config.get('STREAMSV2_SETTINGS', {}).get('MAX_RETRIES', 3)

    def start(self) -> bool:
        """Start stream processing"""
        try:
            logger.info(f"Starting stream {self.config.uuid}")
            
            # Check prerequisites including endpoint registry
            valid, error = self.check_prerequisites()
            if not valid:
                error_msg = f"Prerequisites check failed: {error}"
                logger.error(error_msg)
                raise StreamProcessorError(error_msg)

            # Update core stream status atomically
            result = mongo.db.streams_v2.find_one_and_update(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": StreamStatus.ACTIVE,
                        "active": True,
                        "updated_at": datetime.utcnow(),
                        "last_active": datetime.utcnow(),
                        "metrics": {
                            "processing_start_time": datetime.utcnow(),
                            "messages_processed": 0,
                            "messages_failed": 0,
                            "last_processing_time": None,
                            "success_rate": 0,
                            "error_rate": 0
                        }
                    }
                },
                return_document=True
            )
            
            if not result:
                raise StreamProcessorError("Failed to update stream status")

            try:
                # Initialize source and destination endpoints
                if not self._initialize_endpoints():
                    raise StreamProcessorError("Failed to initialize endpoints")

                # Define tasks with proper queue assignments
                process_task = process_stream_messages.si(self.config.uuid).set(
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

                # Execute chain with error handler
                chain_result = task_chain.apply_async(
                    link_error=handle_chain_error.si(None, None, self.config.uuid)
                )
                
                logger.info(f"Task chain initiated for stream {self.config.uuid} - Root task ID: {chain_result.id}")

                # Create task tracking document with endpoint info
                tracking_doc = {
                    "chain_id": chain_result.id,
                    "stream_uuid": self.config.uuid,
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
                        "source": self.config.source_endpoint_uuid,
                        "destination": self.config.destination_endpoint_uuid
                    },
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "status": "INITIATED",
                    "error": None
                }
                
                mongo.db.stream_tasks.insert_one(tracking_doc)

                # Update monitoring fields
                mongo.db.streams_v2.update_one(
                    {"uuid": self.config.uuid},
                    {
                        "$set": {
                            "last_processing_attempt": None,
                            "monitoring": {
                                "last_check": datetime.utcnow(),
                                "status": "active",
                                "current_chain_id": chain_result.id,
                                "task_chain": {
                                    "process_task_id": None,
                                    "transform_task_id": None,
                                    "deliver_task_id": None,
                                    "last_successful_task": None
                                },
                                "endpoints": {
                                    "source_status": "ACTIVE",
                                    "destination_status": "ACTIVE",
                                    "last_endpoint_check": datetime.utcnow()
                                }
                            }
                        }
                    }
                )

                # Log successful start with endpoint info
                self.logger.log_event(
                    "stream_started",
                    {
                        "task_id": chain_result.id,
                        "source_endpoint": {
                            "uuid": self.config.source_endpoint_uuid,
                            "type": self._get_endpoint_type(self.config.source_endpoint_uuid)
                        },
                        "destination_endpoint": {
                            "uuid": self.config.destination_endpoint_uuid,
                            "type": self._get_endpoint_type(self.config.destination_endpoint_uuid)
                        },
                        "performance_metrics": {}
                    },
                    "success"
                )
                
                return True

            except Exception as chain_error:
                logger.error(f"Failed to initiate task chain: {str(chain_error)}")
                self._handle_startup_failure(str(chain_error))
                raise StreamProcessorError(f"Task chain initialization failed: {str(chain_error)}")

        except Exception as e:
            logger.error(f"Error starting stream {self.config.uuid}: {str(e)}")
            self.logger.log_event(
                "stream_start_failed",
                {"error": str(e)},
                "error"
            )
            return False

    def check_prerequisites(self) -> Tuple[bool, Optional[str]]:
        """Check if stream can start processing"""
        try:
            # Get destination endpoint from registry - only destination needs to be active
            dest_endpoint = self.endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            if not dest_endpoint:
                return False, "Destination endpoint not found in registry"

            dest_status = dest_endpoint.get_status()
            if dest_status.get('status') != 'ACTIVE':
                return False, f"Destination endpoint not active: {dest_status.get('status')}"

            # Verify source endpoint exists but don't check its status
            source_endpoint = self.endpoint_registry.get_endpoint(self.config.source_endpoint_uuid)
            if not source_endpoint:
                return False, "Source endpoint not found in registry"

            # Check message type compatibility
            if not self._verify_message_type_compatibility(
                source_endpoint.config,
                dest_endpoint.config
            ):
                return False, "Message type incompatibility between endpoints"

            # Check if there are any qualifying messages
            qualifying_messages = self.qualifier.find_qualifying_messages(batch_size=1)
            if not qualifying_messages:
                logger.info(f"No qualifying messages found for stream {self.config.uuid}")
                # Return True anyway - we don't fail just because there are no messages yet
                return True, None

            return True, None

        except Exception as e:
            logger.error(f"Error checking prerequisites: {str(e)}")
            return False, str(e)

    def _initialize_endpoints(self) -> bool:
        """Initialize endpoints through registry"""
        try:
            # Get destination endpoint
            dest_endpoint = self.endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            if not dest_endpoint:
                logger.error("Destination endpoint not found in registry")
                return False
            
            # Check current status using mongo directly
            endpoint_status = mongo.db.endpoints.find_one(
                {"uuid": self.config.destination_endpoint_uuid},
                {"status": 1}
            )
            
            # Only try to start if not already active
            if endpoint_status.get('status') != 'ACTIVE':
                if not self.endpoint_registry.start_endpoint(self.config.destination_endpoint_uuid):
                    logger.error("Failed to start destination endpoint")
                    return False
                    
            return True

        except Exception as e:
            logger.error(f"Error initializing endpoints: {str(e)}")
            return False

    def _verify_message_type_compatibility(self, source_config, dest_config) -> bool:
        """
        Verify message type compatibility between source and destination endpoints
        
        Args:
            source_config: Source endpoint configuration
            dest_config: Destination endpoint configuration
            
        Returns:
            bool: True if message types are compatible
        """
        try:
            # Log initial check
            logger.info(f"Checking compatibility: source type={source_config.message_type}, "
                    f"source endpoint={source_config.endpoint_type}, "
                    f"dest endpoint={dest_config.endpoint_type}")

            # Get message type mappings from config
            message_types = current_app.config.get('STREAMSV2_MESSAGE_TYPES', {})
            
            # Look up source message type configuration
            source_type_config = message_types.get('source', {}).get(source_config.message_type)
            if not source_type_config:
                logger.error(f"No configuration found for source message type: {source_config.message_type}")
                return False

            # Get expected destination format
            dest_format = source_type_config.get('destination_format')
            if not dest_format:
                logger.error(f"No destination format specified for source type: {source_config.message_type}")
                return False

            # Get allowed destination endpoints for this format
            dest_type_config = message_types.get('destination', {}).get(dest_format)
            if not dest_type_config:
                logger.error(f"No configuration found for destination format: {dest_format}")
                return False

            allowed_endpoints = dest_type_config.get('allowed_endpoints', [])
            
            # Log compatibility check details
            logger.info(f"Compatibility check details: "
                    f"dest_format={dest_format}, "
                    f"allowed_endpoints={allowed_endpoints}, "
                    f"actual_endpoint={dest_config.endpoint_type}")

            # Check if destination endpoint type is allowed
            if dest_config.endpoint_type not in allowed_endpoints:
                logger.error(f"Destination endpoint type {dest_config.endpoint_type} not in allowed endpoints "
                            f"for format {dest_format}: {allowed_endpoints}")
                return False

            return True

        except Exception as e:
            logger.error(f"Error in message type compatibility check: {str(e)}")
            return False
    
    def stop(self) -> bool:
        """Stop stream processing and clean up resources"""
        try:
            logger.info(f"Stopping stream {self.config.uuid}")

            # Cancel any pending messages
            self.qualifier.cancel_messages()

            # Get destination endpoint from registry
            dest_endpoint = self.endpoint_registry.get_endpoint(
                self.config.destination_endpoint_uuid
            )
            if dest_endpoint:
                # Ensure endpoint is stopped
                self.endpoint_registry.stop_endpoint(
                    self.config.destination_endpoint_uuid
                )

            # Update stream status
            mongo.db.streams_v2.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": StreamStatus.INACTIVE,
                        "active": False,
                        "updated_at": datetime.utcnow()
                    }
                }
            )

            # Log stop event
            self.logger.log_event(
                "stream_stopped",
                {
                    "status": "success",
                    "timestamp": datetime.utcnow().isoformat()
                },
                "success"
            )

            logger.info(f"Stream {self.config.uuid} stopped successfully")
            return True

        except Exception as e:
            logger.error(f"Error stopping stream {self.config.uuid}: {str(e)}")
            self.logger.log_event(
                "stream_stop_failed",
                {
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                },
                "error"
            )
            return False
    
    def _handle_startup_failure(self, error: str) -> None:
        """Handle stream startup failure"""
        try:
            # Update stream status
            mongo.db.streams_v2.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": StreamStatus.ERROR,
                        "active": False,
                        "updated_at": datetime.utcnow(),
                        "last_error": error,
                        "last_error_time": datetime.utcnow()
                    }
                }
            )

            # Stop endpoints if needed
            try:
                source_endpoint = self.endpoint_registry.get_endpoint(self.config.source_endpoint_uuid)
                if source_endpoint and source_endpoint.config.active:
                    self.endpoint_registry.stop_endpoint(self.config.source_endpoint_uuid)
            except Exception as se:
                logger.error(f"Error stopping source endpoint: {str(se)}")

            try:
                dest_endpoint = self.endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
                if dest_endpoint and dest_endpoint.config.active:
                    self.endpoint_registry.stop_endpoint(self.config.destination_endpoint_uuid)
            except Exception as de:
                logger.error(f"Error stopping destination endpoint: {str(de)}")

        except Exception as e:
            logger.error(f"Error handling startup failure: {str(e)}")

    def _get_endpoint_type(self, endpoint_uuid: str) -> str:
        """Get endpoint type from registry"""
        try:
            endpoint = self.endpoint_registry.get_endpoint(endpoint_uuid)
            return endpoint.config.endpoint_type if endpoint else "UNKNOWN"
        except Exception:
            return "UNKNOWN"

# Additional helper methods remain unchanged
# Continue from previous StreamProcessor implementation
# Add these new methods for delivery management

    def process_destination_messages(self) -> Tuple[int, int]:
        """Process messages for delivery to destination endpoint"""
        try:
            logger.info(f"Processing destination messages for stream {self.config.uuid}")

            # Get destination endpoint
            endpoint = self.endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            if not endpoint:
                self.logger.log_event(
                    "delivery_error", 
                    {"error": "Destination endpoint not found in registry"}, 
                    "error"
                )
                return 0, 0

            # Verify endpoint is active
            if not endpoint.config.active:
                success = self.endpoint_registry.start_endpoint(self.config.destination_endpoint_uuid)
                if not success:
                    self.logger.log_event(
                        "delivery_error",
                        {"error": "Failed to start destination endpoint"},
                        "error"
                    )
                    return 0, 0

            # Get pending messages
            messages = self._get_pending_messages()
            if not messages:
                return 0, 0

            processed_count = error_count = 0
            
            # Process each message
            for message in messages:
                try:
                    message_uuid = message.get('uuid')
                    logger.debug(f"Processing message {message_uuid}")

                    # Update to processing status
                    self._update_destination_status(message_uuid, "PROCESSING")

                    # Use endpoint's delivery method
                    success, error = endpoint.handle_delivery(
                        message_uuid=message_uuid,
                        message_content=message.get('transformed_content')
                    )

                    if success:
                        processed_count += 1
                        self._update_destination_status(message_uuid, "COMPLETED")
                        self.logger.log_event(
                            "delivery_completed",
                            {
                                "message_uuid": message_uuid,
                                "endpoint_uuid": endpoint.config.uuid
                            },
                            "success"
                        )
                    else:
                        error_count += 1
                        self._handle_delivery_failure(message, error or "Delivery failed")

                except Exception as e:
                    error_count += 1
                    self._handle_delivery_failure(message, str(e))
                    logger.error(f"Error processing message {message.get('uuid')}: {str(e)}")

            # Update delivery metrics
            self._update_delivery_metrics(processed_count, error_count)
            
            logger.info(f"Completed processing messages: {processed_count} successful, {error_count} failed")
            return processed_count, error_count

        except Exception as e:
            logger.error(f"Error processing destination messages: {str(e)}")
            self.logger.log_event(
                "delivery_processing_error",
                {"error": str(e)},
                "error"
            )
            return 0, 0

    def _deliver_message(
        self,
        message: Dict[str, Any],
        endpoint: 'BaseEndpoint'
    ) -> Tuple[bool, Optional[str]]:
        """Deliver message through endpoint"""
        try:
            message_uuid = message['uuid']
            self._update_destination_status(message_uuid, "PROCESSING")
            
            self.logger.log_event(
                "delivery_attempt",
                {
                    "message_uuid": message_uuid,
                    "endpoint_uuid": endpoint.config.uuid,
                    "endpoint_type": endpoint.config.endpoint_type
                },
                "started"
            )

            success, error = endpoint.handle_delivery(
                message_uuid=message_uuid,
                message_content=message['transformed_content']
            )

            if success:
                self._update_destination_status(message_uuid, "COMPLETED")
                self.logger.log_event(
                    "delivery_completed",
                    {
                        "message_uuid": message_uuid,
                        "endpoint_uuid": endpoint.config.uuid
                    },
                    "success"
                )
            else:
                self.logger.log_event(
                    "delivery_failed",
                    {
                        "message_uuid": message_uuid,
                        "endpoint_uuid": endpoint.config.uuid,
                        "error": error
                    },
                    "error"
                )

            return success, error

        except Exception as e:
            return False, str(e)

    def _get_pending_messages(self) -> List[Dict[str, Any]]:
        """Get pending messages for delivery"""
        try:
            query = {
                "stream_uuid": self.config.uuid,
                "destinations": {
                    "$elemMatch": {
                        "endpoint_uuid": self.config.destination_endpoint_uuid,
                        "status": "READY"
                    }
                }
            }
            
            logger.info(f"[FETCH-START] Starting message fetch for stream {self.config.uuid}")
            logger.info(f"[FETCH-QUERY] Query parameters: {json.dumps(query, default=str)}")
            
            # First check if any messages exist
            count = mongo.db.destination_messages.count_documents(query)
            logger.info(f"[FETCH-COUNT] Found {count} total pending messages for stream {self.config.uuid}")
            
            if count == 0:
                logger.info(f"[FETCH-EMPTY] No pending messages found for stream {self.config.uuid}")
                return []

            # Get messages with batch limit
            messages = list(mongo.db.destination_messages.find(query).limit(self.batch_size))
            logger.info(f"[FETCH-BATCH] Retrieved {len(messages)} messages (batch_size: {self.batch_size})")
            
            # Log detailed message information
            if messages:
                sample_message = messages[0]
                logger.info(f"[FETCH-SAMPLE] First message details:")
                logger.info(f"[FETCH-SAMPLE] UUID: {sample_message.get('uuid')}")
                logger.info(f"[FETCH-SAMPLE] Original Message UUID: {sample_message.get('original_message_uuid')}")
                logger.info(f"[FETCH-SAMPLE] Status: {sample_message.get('destinations')[0].get('status') if sample_message.get('destinations') else 'No destinations'}")
                logger.info(f"[FETCH-SAMPLE] Overall Status: {sample_message.get('overall_status')}")
                logger.info(f"[FETCH-SAMPLE] Created At: {sample_message.get('created_at')}")

                # Log message UUIDs for tracking
                message_uuids = [msg.get('uuid') for msg in messages]
                logger.info(f"[FETCH-BATCH-UUIDS] Message UUIDs in batch: {message_uuids}")
            
            return messages

        except Exception as e:
            error_msg = f"Error fetching pending messages: {str(e)}"
            logger.error(f"[FETCH-ERROR] {error_msg}", exc_info=True)
            
            # Log additional error context
            logger.error(f"[FETCH-ERROR-CONTEXT] Stream UUID: {self.config.uuid}")
            logger.error(f"[FETCH-ERROR-CONTEXT] Endpoint UUID: {self.config.destination_endpoint_uuid}")
            
            # Log to stream event log
            self.logger.log_event(
                "message_fetch_error",
                {
                    "error": error_msg,
                    "stream_uuid": self.config.uuid,
                    "endpoint_uuid": self.config.destination_endpoint_uuid
                },
                "error"
            )
            return []

    def _update_destination_status(
    self,
    message_uuid: str,
    status: str,
    details: Optional[Dict] = None
) -> bool:
        """Update destination message status atomically"""
        try:
            update = {
                "destinations.$[dest].status": status,
                "destinations.$[dest].updated_at": datetime.utcnow()
            }

            if details:
                update["destinations.$[dest].processing_details"] = {
                    **details,
                    "updated_at": datetime.utcnow()
                }

            if status == "COMPLETED":
                update["overall_status"] = "COMPLETED"
            elif status in ["FAILED", "ERROR"]:
                update["overall_status"] = "FAILED"

            result = mongo.db.destination_messages.update_one(
                {
                    "uuid": message_uuid,
                    "destinations.endpoint_uuid": self.config.destination_endpoint_uuid
                },
                {"$set": update},
                array_filters=[{"dest.endpoint_uuid": self.config.destination_endpoint_uuid}]
            )

            return result.modified_count > 0

        except Exception as e:
            logger.error(f"Error updating destination status: {str(e)}")
            return False

    def _handle_delivery_failure(self, message: Dict[str, Any], error: str) -> None:
        """Handle delivery failure with proper retry logic"""
        try:
            message_uuid = message['uuid']
            attempts = self._get_delivery_attempts(message_uuid)

            # Update status based on retry attempts
            if attempts >= self.max_retries:
                status_update = {
                    "status": "FAILED",
                    "final_failure": True,
                    "last_error": error,
                    "attempts": attempts + 1,
                    "failed_at": datetime.utcnow()
                }
                log_event = "delivery_failed_final"
            else:
                next_retry = self._calculate_retry_time(attempts)
                status_update = {
                    "status": "RETRY_PENDING",
                    "last_error": error,
                    "attempts": attempts + 1,
                    "next_retry_at": next_retry,
                    "last_attempt": datetime.utcnow()
                }
                log_event = "delivery_failed_retry"

            # Update message status
            self._update_destination_status(message_uuid, status_update["status"], status_update)

            # Log failure event
            self.logger.log_event(
                log_event,
                {
                    "message_uuid": message_uuid,
                    "error": error,
                    "attempts": attempts + 1,
                    "next_retry": status_update.get("next_retry_at", None)
                },
                "error"
            )

        except Exception as e:
            logger.error(f"Error handling delivery failure: {str(e)}")
            # Try minimal status update
            self._update_destination_status(
                message.get('uuid'), 
                "FAILED",
                {"error": "Error handling failure state"}
            )

    def _get_delivery_attempts(self, message_uuid: str) -> int:
        """Get number of delivery attempts for message"""
        try:
            message = mongo.db.destination_messages.find_one(
                {"uuid": message_uuid},
                {"destinations": 1}
            )
            if not message:
                return 0

            for dest in message.get('destinations', []):
                if dest.get('endpoint_uuid') == self.config.destination_endpoint_uuid:
                    return dest.get('processing_details', {}).get('attempts', 0)
            return 0
        except Exception:
            return 0

    def _calculate_retry_time(self, attempts: int) -> datetime:
        """Calculate next retry time with exponential backoff"""
        retry_delay = self.processing_timeout * (2 ** attempts)
        return datetime.utcnow() + timedelta(seconds=retry_delay)

    def _update_delivery_metrics(self, processed: int, failed: int) -> None:
        """Update stream delivery metrics"""
        try:
            update = {
                "$inc": {
                    "metrics.messages_delivered": processed,
                    "metrics.delivery_errors": failed
                },
                "$set": {
                    "metrics.last_delivery_time": datetime.utcnow(),
                    "metrics.delivery_success_rate": (
                        processed / (processed + failed) if (processed + failed) > 0 else 0
                    )
                }
            }
            mongo.db.streams_v2.update_one({"uuid": self.config.uuid}, update)
        except Exception as e:
            self.logger.log_event("metrics_update_error", {"error": str(e)}, "error")

    def handle_retry_messages(self) -> Tuple[int, int]:
        """
        Process messages that need retry
        
        Returns:
            Tuple[int, int]: (successful_retries, remaining_failures)
        """
        try:
            # Get messages ready for retry
            retry_messages = list(mongo.db.destination_messages.find({
                "stream_uuid": self.config.uuid,
                "destinations": {
                    "$elemMatch": {
                        "endpoint_uuid": self.config.destination_endpoint_uuid,
                        "status": "FAILED",
                        "processing_details.next_retry_at": {"$lte": datetime.utcnow()},
                        "processing_details.final_failure": {"$ne": True}
                    }
                }
            }))

            if not retry_messages:
                return 0, 0

            # Process retries
            successful = 0
            failed = 0

            endpoint = self.endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            if not endpoint:
                return 0, len(retry_messages)

            for message in retry_messages:
                success, _ = self._deliver_message(message, endpoint)
                if success:
                    successful += 1
                else:
                    failed += 1

            return successful, failed

        except Exception as e:
            self.logger.log_event(
                "retry_processing_error",
                {"error": str(e)},
                "error"
            )
            return 0, 0

    def cleanup_delivered_messages(self, days: int = 7) -> int:
        """Cleanup old delivered messages"""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            result = mongo.db.destination_messages.delete_many({
                "stream_uuid": self.config.uuid,
                "overall_status": "COMPLETED",
                "updated_at": {"$lt": cutoff}
            })
            return result.deleted_count
        except Exception as e:
            self.logger.log_event(
                "cleanup_error",
                {"error": str(e)},
                "error"
            )
            return 0