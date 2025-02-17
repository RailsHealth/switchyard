# app/streamsv2/services/destination_handler.py

from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import logging
import time
import json
from flask import current_app

from app.extensions import mongo
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.utils.stream_logger import StreamLogger

logger = logging.getLogger(__name__)

class DeliveryError(Exception):
    """Custom exception for delivery failures"""
    pass

class DestinationStatus:
    """Status constants for destination messages"""
    READY = "READY"
    PROCESSING = "PROCESSING" 
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    RETRY_PENDING = "RETRY_PENDING"  # New
    DELIVERY_READY = "DELIVERY_READY"  # New

class DestinationHandler:
    """Handles message delivery to destinations"""

    def __init__(self, stream_config: StreamConfig):
        self.config = stream_config
        self.logger = StreamLogger(stream_config.uuid)
        self.batch_size = current_app.config.get('STREAMSV2_DELIVERY_SETTINGS', {}).get('batch_size', 50)
        self.max_retries = current_app.config.get('STREAMSV2_DELIVERY_SETTINGS', {}).get('retry_limit', 3)
        self.retry_delay = current_app.config.get('STREAMSV2_DELIVERY_SETTINGS', {}).get('retry_delay', 300)
        self.processing_timeout = current_app.config.get('STREAMSV2_DELIVERY_SETTINGS', {}).get('processing_timeout', 300)

    def process_message(self, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Process a message for delivery through registered endpoint
        
        Args:
            message: Message to deliver
            
        Returns:
            Tuple[bool, str]: (success, error_message)
        """
        message_uuid = message.get('uuid')
        logger.info(f"Processing message {message_uuid} for stream {self.config.uuid}")
        
        try:
            # First validate message structure
            if not self._validate_message(message):
                error_msg = "Invalid message structure"
                self._add_to_delivery_history(message_uuid, {
                    "status": DestinationStatus.FAILED,
                    "error": error_msg,
                    "timestamp": datetime.utcnow()
                })
                return False, error_msg

            # Get destination endpoint from registry with validation
            endpoint = self.endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            if not endpoint:
                error_msg = "Destination endpoint not found in registry"
                self._add_to_delivery_history(message_uuid, {
                    "status": DestinationStatus.FAILED,
                    "error": error_msg,
                    "timestamp": datetime.utcnow()
                })
                return False, error_msg

            # Verify endpoint status and configuration
            endpoint_status = endpoint.get_status()
            if endpoint_status.get('status') != 'ACTIVE':
                error_msg = f"Destination endpoint not active: {endpoint_status.get('status')}"
                self._add_to_delivery_history(message_uuid, {
                    "status": DestinationStatus.FAILED,
                    "error": error_msg,
                    "timestamp": datetime.utcnow(),
                    "endpoint_status": endpoint_status
                })
                return False, error_msg

            # Atomically update message to processing state
            if not self._acquire_message_lock(message_uuid):
                return False, "Message already being processed"

            # Start processing with tracking
            process_start = datetime.utcnow()
            self._add_to_delivery_history(message_uuid, {
                "status": DestinationStatus.PROCESSING,
                "timestamp": process_start,
                "endpoint_uuid": self.config.destination_endpoint_uuid,
                "endpoint_type": endpoint.config.endpoint_type,
                "attempt_number": self.get_attempt_count(message_uuid) + 1
            })

            try:
                # Prepare delivery metadata
                delivery_metadata = {
                    "message_uuid": message_uuid,
                    "stream_uuid": self.config.uuid,
                    "attempt": self.get_attempt_count(message_uuid),
                    "timestamp": datetime.utcnow().isoformat()
                }

                # Attempt delivery with timeout
                with timeout(self.processing_timeout):
                    success, error = endpoint.handle_delivery(
                        message_uuid=message_uuid,
                        message_content=message['transformed_content'],
                        metadata=delivery_metadata
                    )
            except TimeoutError:
                success = False
                error = "Delivery timeout exceeded"
            except Exception as delivery_error:
                success = False
                error = str(delivery_error)

            completion_time = datetime.utcnow()
            processing_duration = (completion_time - process_start).total_seconds()

            if success:
                # Update success status atomically
                success = self._update_completion_status(
                    message_uuid, 
                    completion_time,
                    processing_duration
                )
                if not success:
                    error = "Failed to update completion status"
                    success = False

            if not success:
                # Handle delivery failure with retry logic
                self._handle_delivery_failure(
                    message_uuid,
                    error,
                    processing_duration,
                    completion_time
                )

            # Log delivery metrics
            self._update_delivery_metrics(success, processing_duration)

            return success, error

        except Exception as e:
            logger.error(f"Error processing message {message_uuid}: {str(e)}", exc_info=True)
            self._handle_unexpected_error(message_uuid, str(e))
            return False, str(e)

    def _add_to_delivery_history(self, message_uuid: str, history_entry: Dict[str, Any]) -> None:
        """Add entry to message delivery history"""
        try:
            history_entry["recorded_at"] = datetime.utcnow()
            
            mongo.db.destination_messages.update_one(
                {"uuid": message_uuid},
                {
                    "$push": {
                        "delivery_history": history_entry
                    },
                    "$set": {
                        "updated_at": datetime.utcnow()
                    }
                }
            )
        except Exception as e:
            logger.error(f"Error adding to delivery history: {str(e)}")

    def get_attempt_count(self, message_uuid: str) -> int:
        """Get number of delivery attempts for message"""
        try:
            message = mongo.db.destination_messages.find_one(
                {"uuid": message_uuid},
                {"delivery_history": 1}
            )
            if not message:
                return 0
                
            return len([
                entry for entry in message.get('delivery_history', [])
                if entry.get('status') == DestinationStatus.PROCESSING
            ])
        except Exception:
            return 0

    def should_retry_delivery(self, message_uuid: str) -> bool:
        """Determine if message should be retried"""
        try:
            attempt_count = self.get_attempt_count(message_uuid)
            return attempt_count < self.max_retries
        except Exception:
            return False

    def get_pending_messages(self) -> List[Dict[str, Any]]:
        """Get messages pending delivery"""
        try:
            return list(mongo.db.destination_messages.find({
                "stream_uuid": self.config.uuid,
                "destinations": {
                    "$elemMatch": {
                        "endpoint_uuid": self.config.destination_endpoint_uuid,
                        "$or": [
                            {"status": DestinationStatus.DELIVERY_READY},
                            {
                                "status": DestinationStatus.RETRY_PENDING,
                                "processing_details.next_retry_at": {"$lte": datetime.utcnow()}
                            }
                        ]
                    }
                }
            }).limit(self.batch_size))

        except Exception as e:
            logger.error(f"Error getting pending messages: {str(e)}")
            return []

    def _validate_message(self, message: Dict[str, Any]) -> bool:
        """Validate message structure"""
        required_fields = ['uuid', 'transformed_content', 'destinations']
        return all(field in message for field in required_fields)

    def _get_destination_endpoint(self) -> Optional[Dict[str, Any]]:
        """Get destination endpoint configuration"""
        return mongo.db.endpoints.find_one({
            "uuid": self.config.destination_endpoint_uuid,
            "organization_uuid": self.config.organization_uuid,
            "deleted": {"$ne": True}
        })

    def _queue_for_delivery(self, message: Dict[str, Any], endpoint: Dict[str, Any]) -> bool:
        """Queue message for endpoint delivery"""
        try:
            # Prepare delivery request
            delivery_request = {
                "input": {
                    "content": message['transformed_content'],
                    "metadata": {
                        "stream_uuid": self.config.uuid,
                        "message_uuid": message['uuid'],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                }
            }

            # Update endpoint with delivery request
            result = mongo.db.endpoints.update_one(
                {"uuid": endpoint['uuid']},
                {"$set": delivery_request}
            )

            if result.modified_count > 0:
                logger.debug(f"Queued message {message['uuid']} for delivery")
                return True
            return False

        except Exception as e:
            logger.error(f"Error queuing delivery: {str(e)}")
            return False

    def _process_delivery(self, message_uuid: str) -> bool:
        try:
            start_time = datetime.utcnow()
            
            logger.info(f"Starting delivery for message {message_uuid}")
            
            # Get message and endpoint
            message = mongo.db.destination_messages.find_one({"uuid": message_uuid})
            endpoint = self._get_destination_endpoint()
            
            if not message or not endpoint:
                logger.error(f"Message or endpoint not found for {message_uuid}")
                return False

            # Get endpoint handler from registry
            from app.endpoints import endpoints_registry
            handler = endpoints_registry.get_endpoint(endpoint['uuid'])
            
            if not handler:
                logger.error(f"No handler found for endpoint {endpoint['uuid']}")
                return False

            logger.info(f"Delivering message {message_uuid} via {endpoint['endpoint_type']}")
            
            # Deliver using endpoint's existing instance
            result = handler.deliver_message(message['transformed_content'])
            
            if result:
                logger.info(f"Successfully delivered message {message_uuid}")
                self.mark_message_completed(message_uuid)
                return True

            logger.error(f"Delivery failed for message {message_uuid}")
            return False

        except Exception as e:
            logger.error(f"Error processing delivery: {str(e)}")
            return False
    
    def _add_to_delivery_history(self, message_uuid: str, history_entry: Dict[str, Any]) -> None:
        """Add entry to message delivery history"""
        try:
            mongo.db.destination_messages.update_one(
                {"uuid": message_uuid},
                {"$push": {"delivery_history": history_entry}}
            )
        except Exception as e:
            logger.error(f"Error adding to delivery history: {str(e)}")

    def mark_message_processing(self, message_uuid: str) -> bool:
        """Mark message as being processed"""
        try:
            result = mongo.db.destination_messages.update_one(
                {"uuid": message_uuid},
                {
                    "$set": {
                        "destinations.$[dest].status": DestinationStatus.PROCESSING,
                        "destinations.$[dest].processing_details.last_attempt": datetime.utcnow(),
                        "destinations.$[dest].processing_details.worker_id": None
                    }
                },
                array_filters=[{"dest.endpoint_uuid": self.config.destination_endpoint_uuid}]
            )
            
            if result.modified_count > 0:
                logger.debug(f"Marked message {message_uuid} as processing")
                return True
            return False

        except Exception as e:
            logger.error(f"Error marking message processing: {str(e)}")
            return False

    def _acquire_message_lock(self, message_uuid: str) -> bool:
        """Atomically acquire processing lock for message"""
        try:
            result = mongo.db.destination_messages.find_one_and_update(
                {
                    "uuid": message_uuid,
                    "destinations": {
                        "$elemMatch": {
                            "endpoint_uuid": self.config.destination_endpoint_uuid,
                            "status": {"$in": [
                                DestinationStatus.READY,
                                DestinationStatus.RETRY_PENDING
                            ]}
                        }
                    }
                },
                {
                    "$set": {
                        "destinations.$[dest].status": DestinationStatus.PROCESSING,
                        "destinations.$[dest].processing_details.start_time": datetime.utcnow(),
                        "destinations.$[dest].processing_details.locked_by": self.worker_id
                    }
                },
                array_filters=[{"dest.endpoint_uuid": self.config.destination_endpoint_uuid}],
                return_document=True
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Error acquiring message lock: {str(e)}")
            return False

    def _handle_delivery_failure(
        self, 
        message_uuid: str,
        error: str,
        processing_duration: float,
        completion_time: datetime
    ) -> None:
        """Handle message delivery failure with retry logic"""
        try:
            # Get current attempt count
            attempt_count = self.get_attempt_count(message_uuid)
            should_retry = attempt_count < self.max_retries

            # Calculate retry timing if needed
            next_retry = None
            if should_retry:
                retry_delay = self.retry_delay * (2 ** attempt_count)
                next_retry = datetime.utcnow() + timedelta(seconds=retry_delay)

            # Update failure status
            status_update = {
                "destinations.$[dest].status": (
                    DestinationStatus.RETRY_PENDING if should_retry 
                    else DestinationStatus.FAILED
                ),
                "destinations.$[dest].processing_details.error": error,
                "destinations.$[dest].processing_details.last_attempt": completion_time,
                "destinations.$[dest].processing_details.processing_duration": processing_duration,
                "destinations.$[dest].processing_details.attempts": attempt_count + 1
            }

            if next_retry:
                status_update["destinations.$[dest].processing_details.next_retry_at"] = next_retry
            else:
                status_update["destinations.$[dest].processing_details.final_failure"] = True

            # Update status atomically
            result = mongo.db.destination_messages.update_one(
                {"uuid": message_uuid},
                {
                    "$set": status_update,
                    "$push": {
                        "delivery_history": {
                            "status": status_update["destinations.$[dest].status"],
                            "timestamp": completion_time,
                            "error": error,
                            "processing_duration": processing_duration,
                            "attempt": attempt_count + 1,
                            "next_retry": next_retry
                        }
                    }
                },
                array_filters=[{"dest.endpoint_uuid": self.config.destination_endpoint_uuid}]
            )

            if not result.modified_count:
                logger.error(f"Failed to update failure status for message {message_uuid}")

            # Log failure event
            self.logger.log_event(
                "delivery_failed",
                {
                    "message_uuid": message_uuid,
                    "error": error,
                    "attempt": attempt_count + 1,
                    "processing_duration": processing_duration,
                    "retry_scheduled": bool(next_retry),
                    "next_retry": next_retry.isoformat() if next_retry else None
                },
                "error"
            )

        except Exception as e:
            logger.error(f"Error handling delivery failure: {str(e)}")

    def _schedule_retry(self, message: Dict[str, Any], current_time: datetime) -> None:
        """Schedule message retry with exponential backoff"""
        try:
            for dest in message['destinations']:
                if dest['endpoint_uuid'] == self.config.destination_endpoint_uuid:
                    attempts = dest['processing_details']['attempts']
                    if attempts < self.max_retries:
                        retry_delay = self.retry_delay * (2 ** (attempts - 1))
                        next_retry = current_time + timedelta(seconds=retry_delay)

                        mongo.db.destination_messages.update_one(
                            {"uuid": message['uuid']},
                            {
                                "$set": {
                                    "destinations.$[dest].processing_details.next_retry_at": next_retry
                                }
                            },
                            array_filters=[{"dest.endpoint_uuid": self.config.destination_endpoint_uuid}]
                        )
                        logger.info(f"Scheduled retry for message {message['uuid']} at {next_retry}")

        except Exception as e:
            logger.error(f"Error scheduling retry: {str(e)}")

    def wait_for_completion(self, timeout: int = None) -> bool:
        """Wait for all in-progress deliveries to complete"""
        try:
            timeout = timeout or self.processing_timeout
            start_time = datetime.utcnow()

            # Get endpoint for status checking
            from app.endpoints.registry import endpoint_registry
            endpoint = endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            if not endpoint:
                logger.error("Destination endpoint not found")
                return False

            while (datetime.utcnow() - start_time).total_seconds() < timeout:
                # Check endpoint status
                status = endpoint.get_status()
                if status.get('status') != 'ACTIVE':
                    logger.warning(f"Endpoint {self.config.destination_endpoint_uuid} not active")
                    return False

                # Check for in-progress messages
                count = mongo.db.destination_messages.count_documents({
                    "stream_uuid": self.config.uuid,
                    "destinations": {
                        "$elemMatch": {
                            "endpoint_uuid": self.config.destination_endpoint_uuid,
                            "status": DestinationStatus.PROCESSING
                        }
                    }
                })

                if count == 0:
                    return True

                time.sleep(5)

            logger.warning(f"Wait for completion timed out for stream {self.config.uuid}")
            return False

        except Exception as e:
            logger.error(f"Error waiting for completion: {str(e)}")
            return False

    def get_delivery_stats(self) -> Dict[str, Any]:
        """Get delivery statistics"""
        try:
            # Get base pipeline stats
            pipeline = [
                {
                    "$match": {"stream_uuid": self.config.uuid}
                },
                {
                    "$unwind": "$destinations"
                },
                {
                    "$match": {
                        "destinations.endpoint_uuid": self.config.destination_endpoint_uuid
                    }
                },
                {
                    "$group": {
                        "_id": "$destinations.status",
                        "count": {"$sum": 1},
                        "latest": {"$max": "$destinations.processing_details.last_attempt"}
                    }
                }
            ]

            results = list(mongo.db.destination_messages.aggregate(pipeline))
            
            # Get endpoint metrics from registry
            from app.endpoints.registry import endpoint_registry
            endpoint = endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            endpoint_metrics = endpoint.get_metrics() if endpoint else {}
            
            stats = {
                "total_messages": 0,
                "status_counts": {},
                "latest_activity": None,
                "endpoint_metrics": endpoint_metrics
            }

            for result in results:
                stats["status_counts"][result["_id"]] = result["count"]
                stats["total_messages"] += result["count"]
                if result.get("latest") and (
                    not stats["latest_activity"] or 
                    result["latest"] > stats["latest_activity"]
                ):
                    stats["latest_activity"] = result["latest"]

            return stats

        except Exception as e:
            logger.error(f"Error getting delivery stats: {str(e)}")
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def mark_message_completed(self, message_uuid: str) -> bool:
        """Mark message as successfully completed"""
        try:
            result = mongo.db.destination_messages.update_one(
                {"uuid": message_uuid},
                {
                    "$set": {
                        "destinations.$[dest].status": DestinationStatus.COMPLETED,
                        "destinations.$[dest].processing_details.completed_at": datetime.utcnow()
                    }
                },
                array_filters=[{"dest.endpoint_uuid": self.config.destination_endpoint_uuid}]
            )
            if result.modified_count > 0:
                logger.info(f"Marked message {message_uuid} as completed")
                return True
            return False
        except Exception as e:
            logger.error(f"Error marking message completed: {str(e)}")
            return False
    
    def cleanup_old_messages(self, days: int = 7) -> int:
        """Cleanup old completed messages"""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            result = mongo.db.destination_messages.delete_many({
                "stream_uuid": self.config.uuid,
                "destinations": {
                    "$elemMatch": {
                        "endpoint_uuid": self.config.destination_endpoint_uuid,
                        "status": DestinationStatus.COMPLETED,
                        "processing_details.completed_at": {"$lt": cutoff}
                    }
                }
            })
            logger.info(f"Cleaned up {result.deleted_count} old messages")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old messages: {str(e)}")
            return 0
    
    def check_batch_status(self, message_uuids: List[str]) -> Dict[str, str]:
        """Check status of a batch of messages"""
        try:
            messages = mongo.db.destination_messages.find({
                "uuid": {"$in": message_uuids},
                "stream_uuid": self.config.uuid
            })
            return {
                msg["uuid"]: next(
                    (dest["status"] for dest in msg["destinations"] 
                    if dest["endpoint_uuid"] == self.config.destination_endpoint_uuid),
                    "UNKNOWN"
                )
                for msg in messages
            }
        except Exception as e:
            logger.error(f"Error checking batch status: {str(e)}")
            return {}