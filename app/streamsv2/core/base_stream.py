# app/streamsv2/core/base_stream.py

from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import logging
from abc import ABC
from flask import current_app

from app.extensions import mongo
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.models.stream_status import StreamStatus, StatusManager
from app.streamsv2.utils.stream_logger import StreamLogger
from app.streamsv2.models.message_tracker import MessageTracker
from app.endpoints.registry import endpoint_registry

logger = logging.getLogger(__name__)

class StreamError(Exception):
    """Base exception for stream errors"""
    pass

class BaseStream(ABC):
    """
    Base class for all stream implementations.
    Handles core stream functionality and lifecycle management.
    """

    def __init__(self, config: StreamConfig):
        """Initialize base stream"""
        self.config = config
        self.uuid = config.uuid
        self.organization_uuid = config.organization_uuid
        self.source_endpoint_uuid = config.source_endpoint_uuid
        self.destination_endpoint_uuid = config.destination_endpoint_uuid
        self.message_type = config.message_type
        
        # Initialize components
        self.logger = StreamLogger(self.uuid)
        self.message_tracker = MessageTracker(self.uuid)
        self.active = False
        self.status = StreamStatus.INACTIVE
        
        # Get endpoint registry reference
        self.endpoint_registry = endpoint_registry

    @classmethod
    async def create(cls, stream_data: Dict[str, Any]) -> Optional['BaseStream']:
        """Create new stream instance"""
        try:
            # Create stream config
            config = StreamConfig(**stream_data)
            
            # Validate configuration
            is_valid, error = await config.validate()
            if not is_valid:
                raise StreamError(f"Invalid stream configuration: {error}")
            
            # Verify endpoints in registry
            if not endpoint_registry.get_endpoint(config.source_endpoint_uuid):
                raise StreamError("Source endpoint not found in registry")
            if not endpoint_registry.get_endpoint(config.destination_endpoint_uuid):
                raise StreamError("Destination endpoint not found in registry")
            
            # Create stream instance
            stream = cls(config)
            
            # Store in database with endpoint info
            if not await stream._store_configuration():
                raise StreamError("Failed to store stream configuration")
            
            return stream
            
        except Exception as e:
            logger.error(f"Error creating stream: {str(e)}")
            return None

    async def start(self) -> bool:
        """Start stream processing"""
        try:
            if self.status == StreamStatus.ACTIVE:
                logger.warning(f"Stream {self.uuid} is already active")
                return True

            # Initialize and validate endpoints through registry
            valid, error = await self._validate_endpoints()
            if not valid:
                raise StreamError(f"Endpoint validation failed: {error}")

            # Start endpoints if needed
            source_endpoint = self.endpoint_registry.get_endpoint(self.source_endpoint_uuid)
            if source_endpoint and not source_endpoint.config.active:
                if not self.endpoint_registry.start_endpoint(self.source_endpoint_uuid):
                    raise StreamError("Failed to start source endpoint")

            dest_endpoint = self.endpoint_registry.get_endpoint(self.destination_endpoint_uuid)
            if dest_endpoint and not dest_endpoint.config.active:
                if not self.endpoint_registry.start_endpoint(self.destination_endpoint_uuid):
                    raise StreamError("Failed to start destination endpoint")

            # Update status atomically
            result = await mongo.db.streams_v2.find_one_and_update(
                {
                    "uuid": self.uuid,
                    "status": {"$ne": StreamStatus.DELETED},
                    "deleted": {"$ne": True}
                },
                {
                    "$set": {
                        "status": StreamStatus.ACTIVE,
                        "active": True,
                        "last_active": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                        "endpoints": {
                            "source": {
                                "uuid": self.source_endpoint_uuid,
                                "status": "ACTIVE",
                                "last_check": datetime.utcnow()
                            },
                            "destination": {
                                "uuid": self.destination_endpoint_uuid,
                                "status": "ACTIVE",
                                "last_check": datetime.utcnow()
                            }
                        }
                    }
                },
                return_document=True
            )

            if not result:
                raise StreamError("Failed to update stream status")

            self.status = StreamStatus.ACTIVE
            self.active = True

            await self.logger.log_event(
                "stream_started",
                {
                    "source_endpoint": {
                        "uuid": self.source_endpoint_uuid,
                        "type": source_endpoint.config.endpoint_type if source_endpoint else "UNKNOWN"
                    },
                    "destination_endpoint": {
                        "uuid": self.destination_endpoint_uuid,
                        "type": dest_endpoint.config.endpoint_type if dest_endpoint else "UNKNOWN"
                    }
                },
                "success"
            )
            
            return True
            
        except Exception as e:
            await self.logger.log_event(
                "stream_start_failed",
                {"error": str(e)},
                "error"
            )
            return False

    async def stop(self, reason: str = "Stream stopped by user") -> bool:
        """Stop stream processing"""
        try:
            if self.status != StreamStatus.ACTIVE:
                logger.warning(f"Stream {self.uuid} is not active")
                return True

            # Update status to stopping
            await self._update_status(StreamStatus.STOPPING, reason)

            # Stop endpoints through registry
            try:
                if self.endpoint_registry.get_endpoint(self.source_endpoint_uuid):
                    self.endpoint_registry.stop_endpoint(self.source_endpoint_uuid)
                if self.endpoint_registry.get_endpoint(self.destination_endpoint_uuid):
                    self.endpoint_registry.stop_endpoint(self.destination_endpoint_uuid)
            except Exception as stop_error:
                logger.error(f"Error stopping endpoints: {str(stop_error)}")

            # Cancel qualified messages
            await self._cancel_qualified_messages(reason)

            # Complete in-progress work
            await self._complete_pending_work()

            # Update final status
            await self._update_status(StreamStatus.INACTIVE, "Stream stopped successfully")

            await self.logger.log_event(
                "stream_stopped",
                {"reason": reason},
                "success"
            )
            
            return True
            
        except Exception as e:
            await self.logger.log_event(
                "stream_stop_failed",
                {"error": str(e)},
                "error"
            )
            return False

    async def _validate_endpoints(self) -> Tuple[bool, Optional[str]]:
        """Validate source and destination endpoints through registry"""
        try:
            # Validate source endpoint
            source = self.endpoint_registry.get_endpoint(self.source_endpoint_uuid)
            if not source:
                return False, "Source endpoint not found in registry"

            source_status = source.get_status()
            if source_status.get('status') != 'ACTIVE':
                return False, f"Source endpoint not active: {source_status.get('status')}"

            # Validate destination endpoint
            dest = self.endpoint_registry.get_endpoint(self.destination_endpoint_uuid)
            if not dest:
                return False, "Destination endpoint not found in registry"

            dest_status = dest.get_status()
            if dest_status.get('status') != 'ACTIVE':
                return False, f"Destination endpoint not active: {dest_status.get('status')}"

            # Verify message type compatibility
            if not self._verify_message_type_compatibility(source.config, dest.config):
                return False, "Message type incompatibility between endpoints"

            return True, None
            
        except Exception as e:
            logger.error(f"Error validating endpoints: {str(e)}")
            return False, str(e)

    async def _store_configuration(self) -> bool:
        """Store stream configuration with endpoint details"""
        try:
            # Get endpoint details from registry
            source = self.endpoint_registry.get_endpoint(self.source_endpoint_uuid)
            dest = self.endpoint_registry.get_endpoint(self.destination_endpoint_uuid)

            stream_data = {
                "uuid": self.uuid,
                "name": self.config.name,
                "organization_uuid": self.organization_uuid,
                "source_endpoint_uuid": self.source_endpoint_uuid,
                "destination_endpoint_uuid": self.destination_endpoint_uuid,
                "message_type": self.message_type,
                "status": self.status,
                "active": self.active,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "endpoints": {
                    "source": {
                        "uuid": self.source_endpoint_uuid,
                        "type": source.config.endpoint_type if source else "UNKNOWN",
                        "status": source.get_status().get('status') if source else "UNKNOWN"
                    },
                    "destination": {
                        "uuid": self.destination_endpoint_uuid,
                        "type": dest.config.endpoint_type if dest else "UNKNOWN",
                        "status": dest.get_status().get('status') if dest else "UNKNOWN"
                    }
                },
                "metrics": {
                    "messages_qualified": 0,
                    "messages_transformed": 0,
                    "messages_delivered": 0,
                    "errors": {
                        "transformation_errors": 0,
                        "delivery_errors": 0
                    }
                },
                "delivery_tracking": {
                    "last_delivery_time": None,
                    "delivery_attempts": 0,
                    "failed_deliveries": 0,
                    "retry_count": 0
                },
                "deleted": False
            }
            
            result = await mongo.db.streams_v2.insert_one(stream_data)
            return result.acknowledged
            
        except Exception as e:
            logger.error(f"Error storing stream configuration: {str(e)}")
            return False

    async def get_status(self) -> Dict[str, Any]:
        """Get comprehensive stream and endpoint status"""
        try:
            stream = await mongo.db.streams_v2.find_one({"uuid": self.uuid})
            if not stream:
                raise StreamError(f"Stream {self.uuid} not found")

            # Get endpoint statuses from registry
            source_endpoint = self.endpoint_registry.get_endpoint(self.source_endpoint_uuid)
            dest_endpoint = self.endpoint_registry.get_endpoint(self.destination_endpoint_uuid)

            return {
                "uuid": self.uuid,
                "status": stream.get("status", StreamStatus.INACTIVE),
                "active": stream.get("active", False),
                "metrics": stream.get("metrics", {}),
                "delivery_tracking": stream.get("delivery_tracking", {}),
                "last_active": stream.get("last_active"),
                "updated_at": stream.get("updated_at"),
                "endpoints": {
                    "source": {
                        "uuid": self.source_endpoint_uuid,
                        "status": source_endpoint.get_status() if source_endpoint else {"status": "NOT_FOUND"},
                        "type": source_endpoint.config.endpoint_type if source_endpoint else "UNKNOWN"
                    },
                    "destination": {
                        "uuid": self.destination_endpoint_uuid,
                        "status": dest_endpoint.get_status() if dest_endpoint else {"status": "NOT_FOUND"},
                        "type": dest_endpoint.config.endpoint_type if dest_endpoint else "UNKNOWN"
                    }
                }
            }

        except Exception as e:
            logger.error(f"Error getting stream status: {str(e)}")
            return {
                "uuid": self.uuid,
                "status": "ERROR",
                "error": str(e)
            }

    def __str__(self) -> str:
        """String representation"""
        return f"Stream(uuid={self.uuid}, status={self.status})"