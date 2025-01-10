# app/streamsv2/core/base_stream.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import uuid
import logging
from flask import current_app
from app.extensions import mongo
from app.utils.logging_utils import log_message_cycle
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.models.stream_status import StreamStatus

logger = logging.getLogger(__name__)

class BaseStream(ABC):
    """Base class for all stream types"""

    def __init__(self, config: StreamConfig):
        """Initialize base stream"""
        self.config = config
        self.uuid = config.uuid
        self.organization_uuid = config.organization_uuid
        self.source_endpoint_uuid = config.source_endpoint_uuid
        self.destination_endpoint_uuid = config.destination_endpoint_uuid
        self.message_type = config.message_type
        
        # Initialize core properties
        self.active = False
        self.status = StreamStatus.INACTIVE
        self.last_processed = None
        self.metrics = {
            'messages_processed': 0,
            'messages_failed': 0,
            'bytes_transferred': 0,
            'last_error': None,
            'processing_history': []
        }

    @classmethod
    def create(cls, stream_data: Dict[str, Any]) -> 'BaseStream':
        """Create new stream instance"""
        try:
            # Create stream config
            config = StreamConfig(**stream_data)
            
            # Validate configuration
            is_valid, error = config.validate()
            if not is_valid:
                raise ValueError(f"Invalid stream configuration: {error}")
            
            # Create stream instance
            stream = cls(config)
            
            # Store in database
            result = stream._store_configuration()
            if not result:
                raise Exception("Failed to store stream configuration")
            
            return stream
            
        except Exception as e:
            logger.error(f"Error creating stream: {str(e)}")
            raise

    def _store_configuration(self) -> bool:
        """Store stream configuration in database"""
        try:
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
                "metrics": self.metrics,
                "metadata": self.config.metadata
            }
            
            result = mongo.db.streams_v2.insert_one(stream_data)
            return result.acknowledged
            
        except Exception as e:
            logger.error(f"Error storing stream configuration: {str(e)}")
            return False

    async def start(self) -> bool:
        """Start stream processing"""
        try:
            if self.status == StreamStatus.ACTIVE:
                logger.warning(f"Stream {self.uuid} is already active")
                return True

            # Validate endpoints before starting
            source_valid, dest_valid = await self._validate_endpoints()
            if not (source_valid and dest_valid):
                raise ValueError("Invalid endpoint configuration")

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
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count:
                self._log_event("stream_started", {}, "success")
                return True
                
            return False
            
        except Exception as e:
            self._log_event("stream_start_failed", {"error": str(e)}, "error")
            return False

    async def stop(self) -> bool:
        """Stop stream processing"""
        try:
            if self.status == StreamStatus.INACTIVE:
                logger.warning(f"Stream {self.uuid} is already inactive")
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
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.modified_count:
                self._log_event("stream_stopped", {}, "success")
                return True
                
            return False
            
        except Exception as e:
            self._log_event("stream_stop_failed", {"error": str(e)}, "error")
            return False

    @abstractmethod
    async def _validate_endpoints(self) -> Tuple[bool, bool]:
        """
        Validate source and destination endpoints
        Returns: Tuple of (source_valid, destination_valid)
        """
        pass

    @abstractmethod
    async def process_message(self, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Process a single message"""
        pass

    def update_metrics(self, metrics_data: Dict[str, Any]) -> None:
        """Update stream metrics"""
        try:
            # Update local metrics
            self.metrics.update(metrics_data)
            self.metrics['updated_at'] = datetime.utcnow()
            
            # Update database
            mongo.db.streams_v2.update_one(
                {"uuid": self.uuid},
                {
                    "$set": {
                        "metrics": self.metrics,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
        except Exception as e:
            logger.error(f"Error updating metrics for stream {self.uuid}: {str(e)}")

    def get_status(self) -> Dict[str, Any]:
        """Get current stream status and metrics"""
        try:
            stream_data = mongo.db.streams_v2.find_one({"uuid": self.uuid})
            if not stream_data:
                raise ValueError(f"Stream {self.uuid} not found")

            return {
                "uuid": self.uuid,
                "status": stream_data.get("status", StreamStatus.INACTIVE),
                "active": stream_data.get("active", False),
                "metrics": stream_data.get("metrics", {}),
                "last_updated": stream_data.get("updated_at")
            }
            
        except Exception as e:
            logger.error(f"Error getting status for stream {self.uuid}: {str(e)}")
            return {
                "uuid": self.uuid,
                "status": StreamStatus.ERROR,
                "error": str(e)
            }

    def _log_event(self, event_type: str, details: Dict[str, Any], status: str) -> None:
        """Log stream event"""
        try:
            log_message_cycle(
                message_uuid=str(uuid.uuid4()),  # Generate new UUID for stream events
                event_type=event_type,
                details={
                    "stream_uuid": self.uuid,
                    "stream_name": self.config.name,
                    **details
                },
                status=status,
                organization_uuid=self.organization_uuid
            )
        except Exception as e:
            logger.error(f"Error logging stream event: {str(e)}")