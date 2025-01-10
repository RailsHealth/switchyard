from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from flask import current_app
import uuid
import logging
from app.extensions import mongo
from .stream_status import StreamStatus, StreamStatusManager
from .stream_metrics import StreamMetrics

logger = logging.getLogger(__name__)

@dataclass
class StreamConfig:
    """
    Configuration container for streams
    
    Provides:
    - Configuration management
    - Validation logic
    - Database operations
    - Metrics tracking
    - Status management
    """
    uuid: str
    name: str
    organization_uuid: str
    source_endpoint_uuid: str
    destination_endpoint_uuid: str
    message_type: str
    active: bool = False
    status: str = StreamStatus.INACTIVE
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    last_active: Optional[datetime] = None
    deleted: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=lambda: StreamMetrics().to_dict())

    @classmethod
    def create(cls, data: Dict[str, Any]) -> StreamConfig:
        """Create new stream configuration"""
        try:
            # Ensure UUID
            if 'uuid' not in data:
                data['uuid'] = str(uuid.uuid4())

            # Create instance
            config = cls(**data)
            
            # Validate configuration
            is_valid, error = config.validate()
            if not is_valid:
                raise ValueError(f"Invalid stream configuration: {error}")

            # Store in database
            store_data = config.to_dict()
            store_data['created_at'] = datetime.utcnow()
            store_data['updated_at'] = datetime.utcnow()
            
            result = mongo.db.streams_v2.insert_one(store_data)
            if not result.acknowledged:
                raise ValueError("Failed to create stream configuration")

            logger.info(f"Created stream configuration: {config.uuid}")
            return config

        except Exception as e:
            logger.error(f"Error creating stream configuration: {str(e)}")
            raise

    @classmethod
    def get_by_uuid(cls, uuid: str, organization_uuid: str) -> Optional[StreamConfig]:
        """Get stream by UUID"""
        try:
            data = mongo.db.streams_v2.find_one({
                "uuid": uuid,
                "organization_uuid": organization_uuid,
                "deleted": {"$ne": True}
            })
            return cls(**data) if data else None
        except Exception as e:
            logger.error(f"Error fetching stream {uuid}: {str(e)}")
            return None

    @classmethod
    def list_by_organization(cls, organization_uuid: str, 
                           include_deleted: bool = False) -> List[StreamConfig]:
        """List all streams for an organization"""
        try:
            query = {"organization_uuid": organization_uuid}
            if not include_deleted:
                query["deleted"] = {"$ne": True}
                
            streams = mongo.db.streams_v2.find(query)
            return [cls(**data) for data in streams]
        except Exception as e:
            logger.error(f"Error listing streams: {str(e)}")
            return []

    @classmethod
    def get_active_streams(cls, organization_uuid: str) -> List[StreamConfig]:
        """Get all active streams for an organization"""
        try:
            streams = mongo.db.streams_v2.find({
                "organization_uuid": organization_uuid,
                "active": True,
                "status": StreamStatus.ACTIVE,
                "deleted": {"$ne": True}
            })
            return [cls(**data) for data in streams]
        except Exception as e:
            logger.error(f"Error fetching active streams: {str(e)}")
            return []

    def validate(self) -> tuple[bool, Optional[str]]:
        """Validate stream configuration"""
        try:
            # Check source endpoint exists and is valid
            source = mongo.db.endpoints.find_one({
                "uuid": self.source_endpoint_uuid,
                "organization_uuid": self.organization_uuid,
                "mode": "source",
                "deleted": {"$ne": True}
            })
            if not source:
                return False, "Invalid or missing source endpoint"

            # Check destination endpoint exists and is valid
            destination = mongo.db.endpoints.find_one({
                "uuid": self.destination_endpoint_uuid,
                "organization_uuid": self.organization_uuid,
                "endpoint_type": {"$in": ["aws_s3", "gcp_storage"]},
                "deleted": {"$ne": True}
            })
            if not destination:
                return False, "Invalid or missing destination endpoint"

            # Validate message type compatibility
            source_type = source.get('message_type')
            destination_type = f"{source_type} in JSON"
            
            # Check against config
            message_types = current_app.config['STREAMSV2_MESSAGE_TYPES']
            if source_type not in message_types['source']:
                return False, f"Unsupported source message type: {source_type}"
            
            if destination_type not in message_types['destination']:
                return False, f"Unsupported destination message type: {destination_type}"

            # Validate endpoint type compatibility
            source_config = message_types['source'][source_type]
            if source.get('endpoint_type') not in source_config['allowed_endpoints']:
                return False, f"Endpoint type {source.get('endpoint_type')} not allowed for {source_type}"

            # Validate stream name
            if not self.name or len(self.name) < 3:
                return False, "Stream name must be at least 3 characters long"

            # Validate status
            if not StreamStatusManager.validate_status(self.status):
                return False, f"Invalid stream status: {self.status}"

            return True, None

        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def update(self, updates: Dict[str, Any]) -> bool:
        """Update stream configuration"""
        try:
            # Ensure we can't update certain fields
            protected_fields = {'uuid', 'organization_uuid', 'created_at'}
            updates = {k: v for k, v in updates.items() if k not in protected_fields}
            
            # Add update timestamp
            updates['updated_at'] = datetime.utcnow()
            
            # If status is being updated, validate transition
            if 'status' in updates:
                current_status = StreamStatus(self.status)
                new_status = StreamStatus(updates['status'])
                if not StreamStatusManager.is_valid_transition(current_status, new_status):
                    raise ValueError(f"Invalid status transition: {current_status} -> {new_status}")

            # Update in database
            result = mongo.db.streams_v2.update_one(
                {
                    "uuid": self.uuid,
                    "organization_uuid": self.organization_uuid
                },
                {"$set": updates}
            )

            if result.modified_count > 0:
                # Update local instance
                for key, value in updates.items():
                    setattr(self, key, value)
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error updating stream config: {str(e)}")
            return False

    def delete(self) -> bool:
        """Mark stream as deleted"""
        try:
            return self.update({
                "deleted": True,
                "active": False,
                "status": StreamStatus.DELETED,
                "deleted_at": datetime.utcnow()
            })
        except Exception as e:
            logger.error(f"Error deleting stream: {str(e)}")
            return False

    def update_metrics(self, metrics_update: Dict[str, Any]) -> bool:
        """Update stream metrics"""
        try:
            current_metrics = StreamMetrics(**self.metrics)
            current_metrics.update_metrics(metrics_update)
            
            return self.update({
                "metrics": current_metrics.to_dict(),
                "last_active": datetime.utcnow()
            })
        except Exception as e:
            logger.error(f"Error updating stream metrics: {str(e)}")
            return False

    def get_endpoints(self) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Get source and destination endpoint details"""
        try:
            source = mongo.db.endpoints.find_one({
                "uuid": self.source_endpoint_uuid,
                "organization_uuid": self.organization_uuid
            })
            destination = mongo.db.endpoints.find_one({
                "uuid": self.destination_endpoint_uuid,
                "organization_uuid": self.organization_uuid
            })
            return source, destination
        except Exception as e:
            logger.error(f"Error fetching endpoints: {str(e)}")
            return None, None

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        return {
            "uuid": self.uuid,
            "name": self.name,
            "organization_uuid": self.organization_uuid,
            "source_endpoint_uuid": self.source_endpoint_uuid,
            "destination_endpoint_uuid": self.destination_endpoint_uuid,
            "message_type": self.message_type,
            "active": self.active,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_active": self.last_active,
            "deleted": self.deleted,
            "metadata": self.metadata,
            "metrics": self.metrics
        }

    def __str__(self) -> str:
        """String representation"""
        return (f"StreamConfig(uuid={self.uuid}, name={self.name}, "
                f"status={self.status}, active={self.active})")