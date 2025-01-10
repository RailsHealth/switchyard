# app/streamsv2/core/cloud_stream.py

from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import logging
from app.streamsv2.core.base_stream import BaseStream
from app.streamsv2.services.message_processor import StreamMessageProcessor
from app.endpoints.cloud_storage_endpoint import CloudStorageEndpoint
from app.extensions import mongo
from app.streamsv2.models.stream_status import StreamStatus

logger = logging.getLogger(__name__)

class CloudStream(BaseStream):
    """Implementation for cloud storage streams"""

    def __init__(self, config):
        """Initialize cloud stream"""
        super().__init__(config)
        self.message_processor = StreamMessageProcessor(config.__dict__)

    async def _validate_endpoints(self) -> Tuple[bool, bool]:
        """Validate source and destination endpoints"""
        try:
            # Validate source endpoint
            source = mongo.db.endpoints.find_one({
                "uuid": self.source_endpoint_uuid,
                "organization_uuid": self.organization_uuid,
                "deleted": {"$ne": True},
                "status": "ACTIVE"  # Check if endpoint is active
            })
            
            source_valid = bool(source)
            if not source_valid:
                logger.error(f"Source endpoint {self.source_endpoint_uuid} validation failed: not found or not active")
            
            # Validate destination endpoint (must be cloud storage)
            dest = mongo.db.endpoints.find_one({
                "uuid": self.destination_endpoint_uuid,
                "organization_uuid": self.organization_uuid,
                "endpoint_type": {"$in": ["aws_s3", "gcp_storage"]},
                "deleted": {"$ne": True},
                "status": "ACTIVE"  # Check if endpoint is active
            })
            
            dest_valid = bool(dest)
            if not dest_valid:
                logger.error(f"Destination endpoint {self.destination_endpoint_uuid} validation failed: not found or not active")
            
            # Log validation results
            if not source_valid:
                self._log_event(
                    "endpoint_validation_failed",
                    {
                        "endpoint_uuid": self.source_endpoint_uuid,
                        "type": "source",
                        "reason": "Endpoint not found or not active"
                    },
                    "error"
                )
            
            if not dest_valid:
                self._log_event(
                    "endpoint_validation_failed",
                    {
                        "endpoint_uuid": self.destination_endpoint_uuid,
                        "type": "destination",
                        "reason": "Endpoint not found, not active, or wrong type"
                    },
                    "error"
                )
            
            return source_valid, dest_valid
            
        except Exception as e:
            logger.error(f"Error validating endpoints: {str(e)}", exc_info=True)
            self._log_event(
                "endpoint_validation_error",
                {"error": str(e)},
                "error"
            )
            return False, False

    async def start(self) -> bool:
        """Start stream processing"""
        try:
            if self.status == StreamStatus.ACTIVE:
                logger.warning(f"Stream {self.uuid} is already active")
                return True

            # Validate endpoints before starting
            source_valid, dest_valid = await self._validate_endpoints()
            if not (source_valid and dest_valid):
                error_msg = "One or more endpoints are not active or invalid"
                logger.error(f"Stream {self.uuid}: {error_msg}")
                raise ValueError(error_msg)

            # Update status
            self.status = StreamStatus.ACTIVE
            self.active = True
            
            # Update database
            result = mongo.db.streams_v2.update_one(
                {"uuid": self.uuid},
                {
                    "$set": {
                        "status": self.status,
                        "active": self.active,
                        "last_active": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                        "metrics.last_started": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count:
                self._log_event(
                    "stream_started",
                    {"timestamp": datetime.utcnow().isoformat()},
                    "success"
                )
                logger.info(f"Stream {self.uuid} started successfully")
                return True
                
            logger.error(f"Failed to update stream {self.uuid} status in database")
            return False
            
        except Exception as e:
            error_msg = f"Failed to start stream: {str(e)}"
            logger.error(f"Stream {self.uuid}: {error_msg}", exc_info=True)
            self._log_event(
                "stream_start_failed",
                {
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                },
                "error"
            )
            raise ValueError(error_msg)

    async def stop(self) -> bool:
        """Stop stream processing"""
        try:
            if self.status != StreamStatus.ACTIVE:
                logger.warning(f"Stream {self.uuid} is not active")
                return True

            # Update status
            self.status = StreamStatus.INACTIVE
            self.active = False
            
            # Update database
            result = mongo.db.streams_v2.update_one(
                {"uuid": self.uuid},
                {
                    "$set": {
                        "status": self.status,
                        "active": self.active,
                        "updated_at": datetime.utcnow(),
                        "metrics.last_stopped": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count:
                self._log_event(
                    "stream_stopped",
                    {"timestamp": datetime.utcnow().isoformat()},
                    "success"
                )
                logger.info(f"Stream {self.uuid} stopped successfully")
                return True
                
            logger.error(f"Failed to update stream {self.uuid} status in database")
            return False
            
        except Exception as e:
            error_msg = f"Failed to stop stream: {str(e)}"
            logger.error(f"Stream {self.uuid}: {error_msg}", exc_info=True)
            self._log_event(
                "stream_stop_failed",
                {
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                },
                "error"
            )
            raise ValueError(error_msg)

    async def process_message(self, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Process a message using the message processor"""
        try:
            # Use message processor to handle the message
            success, error = await self.message_processor.process_message(message)
            
            # Update metrics based on result
            if success:
                self.update_metrics({
                    'messages_processed': self.metrics['messages_processed'] + 1,
                    'bytes_transferred': self.metrics['bytes_transferred'] + len(message.get('message', '')),
                    'last_successful_message': datetime.utcnow()
                })
            else:
                self.update_metrics({
                    'messages_failed': self.metrics['messages_failed'] + 1,
                    'last_error': error,
                    'last_error_timestamp': datetime.utcnow()
                })
            
            return success, error
            
        except Exception as e:
            error_msg = f"Error processing message: {str(e)}"
            self.update_metrics({
                'messages_failed': self.metrics['messages_failed'] + 1,
                'last_error': error_msg,
                'last_error_timestamp': datetime.utcnow()
            })
            return False, error_msg