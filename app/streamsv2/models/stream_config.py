from __future__ import annotations
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple, ClassVar
import logging
from uuid import UUID, uuid4
from flask import current_app

from app.extensions import mongo
from .stream_status import StreamStatus, StatusTransitionError, StatusManager
from app.utils.logging_utils import log_message_cycle

logger = logging.getLogger(__name__)

def generate_uuid() -> str:
    """Generate a new UUID string"""
    return str(uuid4())

@dataclass
class StreamConfig:
    """Stream configuration container"""
    # Class level constants
    MIN_NAME_LENGTH: ClassVar[int] = 3
    MAX_NAME_LENGTH: ClassVar[int] = 100
    RESERVED_NAMES: ClassVar[List[str]] = ['test', 'debug', 'temp', 'default']

    # Instance attributes
    uuid: str
    name: str
    organization_uuid: str
    source_endpoint_uuid: str
    destination_endpoint_uuid: str
    message_type: str
    status: str = field(default=StreamStatus.INACTIVE)
    active: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    last_active: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    deleted: bool = False
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None

    def __post_init__(self):
        """Validate and clean up after initialization"""
        # Ensure UUIDs are strings
        self.uuid = str(self.uuid)
        self.organization_uuid = str(self.organization_uuid)
        self.source_endpoint_uuid = str(self.source_endpoint_uuid)
        self.destination_endpoint_uuid = str(self.destination_endpoint_uuid)
        
        # Initialize metrics if None
        if not self.metrics:
            self.metrics = {
                "messages_processed": 0,
                "messages_failed": 0,
                "messages_pending": 0,
                "last_processing_time": None,
                "error_count": 0,
                "processing_stats": {
                    "batch_size": 0,
                    "average_processing_time": 0,
                    "success_rate": 0
                }
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StreamConfig':
        """Create StreamConfig from dictionary, handling extra fields"""
        try:
            # Get only fields that exist in the dataclass
            valid_fields = {field.name for field in fields(cls)}
            filtered_data = {k: v for k, v in data.items() if k in valid_fields}
            
            # Convert string dates to datetime objects if present
            date_fields = ['created_at', 'updated_at', 'last_active', 'last_error_time']
            for field_name in date_fields:
                if field_name in filtered_data and isinstance(filtered_data[field_name], str):
                    try:
                        filtered_data[field_name] = datetime.fromisoformat(filtered_data[field_name])
                    except (ValueError, TypeError):
                        filtered_data[field_name] = None
            
            return cls(**filtered_data)
        except Exception as e:
            logger.error(f"Error creating StreamConfig from dict: {str(e)}")
            raise ValueError(f"Invalid stream configuration data: {str(e)}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage"""
        return {
            "uuid": str(self.uuid),
            "name": self.name,
            "organization_uuid": str(self.organization_uuid),
            "source_endpoint_uuid": str(self.source_endpoint_uuid),
            "destination_endpoint_uuid": str(self.destination_endpoint_uuid),
            "message_type": self.message_type,
            "status": self.status,
            "active": self.active,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_active": self.last_active,
            "metadata": self.metadata,
            "deleted": self.deleted,
            "last_error": self.last_error,
            "last_error_time": self.last_error_time
        }
    def validate(self) -> Tuple[bool, Optional[str]]:
        """
        Validate stream configuration
        Returns: (is_valid, error_message)
        """
        try:
            # Validate name
            if not self.name:
                return False, "Stream name is required"
            
            if len(self.name) < self.MIN_NAME_LENGTH:
                return False, f"Stream name must be at least {self.MIN_NAME_LENGTH} characters"
                
            if len(self.name) > self.MAX_NAME_LENGTH:
                return False, f"Stream name cannot exceed {self.MAX_NAME_LENGTH} characters"

            if self.name.lower() in self.RESERVED_NAMES:
                return False, f"'{self.name}' is a reserved name"

            # Check name uniqueness within organization
            existing = mongo.db.streams_v2.find_one({
                "name": self.name,
                "organization_uuid": self.organization_uuid,
                "deleted": {"$ne": True},
                "uuid": {"$ne": self.uuid}  # Exclude self for updates
            })
            if existing:
                return False, "Stream name must be unique within organization"

            # Check source endpoint exists and is accessible
            source = mongo.db.endpoints.find_one({
                "uuid": self.source_endpoint_uuid,
                "organization_uuid": self.organization_uuid,
                "deleted": {"$ne": True}
            })
            if not source:
                return False, "Source endpoint not found or not accessible"

            # Check destination endpoint exists and is accessible
            dest = mongo.db.endpoints.find_one({
                "uuid": self.destination_endpoint_uuid,
                "organization_uuid": self.organization_uuid,
                "deleted": {"$ne": True}
            })
            if not dest:
                return False, "Destination endpoint not found or not accessible"

            # Validate message type compatibility
            message_types = current_app.config['STREAMSV2_MESSAGE_TYPES']
            
            source_type = source.get('message_type')
            if source_type not in message_types['source']:
                return False, f"Unsupported source message type: {source_type}"

            # Check if source endpoint type is allowed
            source_config = message_types['source'][source_type]
            if source.get('endpoint_type') not in source_config['allowed_endpoints']:
                return False, f"Endpoint type {source.get('endpoint_type')} not allowed for {source_type}"

            # Check if destination message type matches expected format
            expected_dest_type = source_config['destination_format']
            if self.message_type != expected_dest_type:
                return False, f"Invalid destination message type. Expected {expected_dest_type}"

            # Check if destination endpoint type is allowed
            dest_config = message_types['destination'].get(expected_dest_type)
            if not dest_config:
                return False, f"Unsupported destination message type: {expected_dest_type}"

            if dest.get('endpoint_type') not in dest_config['allowed_endpoints']:
                return False, f"Destination endpoint type {dest.get('endpoint_type')} not allowed for {expected_dest_type}"

            # Check endpoint limits
            for endpoint_uuid in [self.source_endpoint_uuid, self.destination_endpoint_uuid]:
                current_streams = mongo.db.streams_v2.count_documents({
                    "$or": [
                        {"source_endpoint_uuid": endpoint_uuid},
                        {"destination_endpoint_uuid": endpoint_uuid}
                    ],
                    "deleted": {"$ne": True},
                    "uuid": {"$ne": self.uuid}  # Exclude self for updates
                })
                
                if current_streams >= current_app.config['ORGANIZATION_ENDPOINT_LIMITS']['max_active_endpoints']:
                    return False, f"Maximum stream limit reached for endpoint {endpoint_uuid}"

            return True, None

        except Exception as e:
            logger.error(f"Error validating stream config: {str(e)}")
            return False, str(e)

    @classmethod
    def create(cls, data: Dict[str, Any]) -> Optional['StreamConfig']:
        """Create new stream configuration"""
        try:
            # Ensure required fields
            required_fields = ['name', 'organization_uuid', 
                             'source_endpoint_uuid', 'destination_endpoint_uuid',
                             'message_type']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

            # Generate UUID if not provided
            if 'uuid' not in data:
                data['uuid'] = generate_uuid()

            # Clean and validate UUIDs
            for field in ['organization_uuid', 'source_endpoint_uuid', 'destination_endpoint_uuid']:
                if field in data:
                    try:
                        # Validate UUID format
                        UUID(str(data[field]))
                        data[field] = str(data[field])
                    except ValueError:
                        raise ValueError(f"Invalid UUID format for {field}")

            # Add creation metadata
            data['created_at'] = datetime.utcnow()
            data['updated_at'] = datetime.utcnow()
            data['metadata'] = data.get('metadata', {})
            data['metadata'].update({
                'created_at': datetime.utcnow().isoformat(),
                'version': '2.0'
            })

            # Create instance using from_dict to handle fields safely
            config = cls.from_dict(data)
            
            # Validate configuration
            valid, error = config.validate()
            if not valid:
                raise ValueError(f"Invalid configuration: {error}")

            # Store in database with retry
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = mongo.db.streams_v2.insert_one(config.to_dict())
                    if result.acknowledged:
                        break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Retry {attempt + 1} storing stream configuration: {str(e)}")

            # Log creation
            log_message_cycle(
                message_uuid=generate_uuid(),
                event_type="stream_created",
                details={
                    "stream_uuid": config.uuid,
                    "stream_name": config.name,
                    "organization_uuid": config.organization_uuid,
                    "source_endpoint": config.source_endpoint_uuid,
                    "destination_endpoint": config.destination_endpoint_uuid
                },
                status="success"
            )

            logger.info(f"Created stream configuration: {config.uuid}")
            return config

        except Exception as e:
            logger.error(f"Error creating stream configuration: {str(e)}")
            return None
    def set_error(self, error: str) -> None:
        """Set error information"""
        self.last_error = error
        self.last_error_time = datetime.utcnow()
        self.status = StreamStatus.ERROR
        self.active = False
        self.updated_at = datetime.utcnow()

    def clear_error(self) -> None:
        """Clear error information"""
        self.last_error = None
        self.last_error_time = None
        self.updated_at = datetime.utcnow()

    def save(self) -> bool:
        """Save configuration changes to database"""
        try:
            self.updated_at = datetime.utcnow()
            
            # Validate before saving
            is_valid, error = self.validate()
            if not is_valid:
                raise ValueError(f"Invalid configuration: {error}")

            # Update with optimistic locking
            result = mongo.db.streams_v2.update_one(
                {
                    "uuid": self.uuid,
                    "updated_at": self.updated_at
                },
                {
                    "$set": self.to_dict(),
                    "$currentDate": {"updated_at": True}
                }
            )
            
            if result.modified_count > 0:
                logger.info(f"Updated stream configuration: {self.uuid}")
                return True
                
            if result.matched_count == 0:
                logger.warning(f"Stream {self.uuid} not found")
            else:
                logger.warning(f"Concurrent modification detected for stream {self.uuid}")
            return False
            
        except Exception as e:
            logger.error(f"Error saving stream config: {str(e)}")
            return False

    def update_status(self, new_status: str, reason: Optional[str] = None) -> bool:
        """
        Update stream status with validation and logging
        
        Args:
            new_status: Target status
            reason: Optional reason for status change
            
        Returns:
            bool: Success
        """
        try:
            # Validate transition
            if not StatusManager.validate_stream_transition(self.status, new_status):
                raise StatusTransitionError(
                    f"Invalid status transition: {self.status} → {new_status}"
                )

            # Check current status in database
            current = mongo.db.streams_v2.find_one(
                {"uuid": self.uuid},
                {"status": 1, "updated_at": 1}
            )
            if not current:
                raise ValueError("Stream not found")

            if current['status'] != self.status:
                raise StatusTransitionError(
                    f"Stream status changed externally: {self.status} → {current['status']}"
                )

            # Update status
            self.status = new_status
            self.active = (new_status == StreamStatus.ACTIVE)
            self.updated_at = datetime.utcnow()

            if new_status == StreamStatus.ACTIVE:
                self.last_active = datetime.utcnow()

            # Save changes
            update_data = {
                "status": new_status,
                "active": self.active,
                "updated_at": self.updated_at,
                "last_active": self.last_active,
                "status_history": {
                    "previous_status": current['status'],
                    "new_status": new_status,
                    "changed_at": datetime.utcnow(),
                    "reason": reason
                }
            }

            result = mongo.db.streams_v2.update_one(
                {"uuid": self.uuid},
                {"$set": update_data}
            )

            if result.modified_count > 0:
                # Log status change
                log_message_cycle(
                    message_uuid=generate_uuid(),
                    event_type="stream_status_changed",
                    details={
                        "stream_uuid": self.uuid,
                        "previous_status": current['status'],
                        "new_status": new_status,
                        "reason": reason
                    },
                    status="success"
                )
                logger.info(f"Updated stream {self.uuid} status to {new_status}")
                return True

            logger.warning(f"No status change made for stream {self.uuid}")
            return False

        except Exception as e:
            logger.error(f"Error updating stream status: {str(e)}")
            return False

    def mark_deleted(self, reason: Optional[str] = None) -> bool:
        """Mark stream as deleted with cleanup"""
        try:
            # Can't delete active stream
            if self.status == StreamStatus.ACTIVE:
                raise ValueError("Cannot delete active stream")

            self.deleted = True
            self.active = False
            self.status = StreamStatus.DELETED
            self.updated_at = datetime.utcnow()

            # Prepare deletion metadata
            update_data = {
                "deleted": True,
                "active": False,
                "status": StreamStatus.DELETED,
                "updated_at": self.updated_at,
                "deleted_at": datetime.utcnow(),
                "deletion_metadata": {
                    "reason": reason,
                    "previous_status": self.status,
                    "source_endpoint": self.source_endpoint_uuid,
                    "destination_endpoint": self.destination_endpoint_uuid,
                    "deleted_at": datetime.utcnow().isoformat()
                }
            }

            # Use transactions for deletion
            with mongo.cx.start_session() as session:
                with session.start_transaction():
                    # Mark stream as deleted
                    result = mongo.db.streams_v2.update_one(
                        {"uuid": self.uuid},
                        {"$set": update_data},
                        session=session
                    )

                    if result.modified_count == 0:
                        raise ValueError("Stream not found or already deleted")

                    # Cleanup associated resources
                    mongo.db.destination_messages.update_many(
                        {"stream_uuid": self.uuid},
                        {"$set": {"deleted": True}},
                        session=session
                    )

            # Log deletion
            log_message_cycle(
                message_uuid=generate_uuid(),
                event_type="stream_deleted",
                details={
                    "stream_uuid": self.uuid,
                    "stream_name": self.name,
                    "reason": reason
                },
                status="success"
            )

            logger.info(f"Marked stream {self.uuid} as deleted")
            return True

        except Exception as e:
            logger.error(f"Error marking stream deleted: {str(e)}")
            return False

    @classmethod
    def get_by_uuid(cls, uuid: str) -> Optional['StreamConfig']:
        """Get stream configuration by UUID"""
        try:
            # Validate UUID format
            try:
                UUID(str(uuid))
            except ValueError:
                logger.error(f"Invalid UUID format: {uuid}")
                return None

            data = mongo.db.streams_v2.find_one({
                "uuid": str(uuid),
                "deleted": {"$ne": True}
            })
            return cls.from_dict(data) if data else None
        except Exception as e:
            logger.error(f"Error fetching stream {uuid}: {str(e)}")
            return None

    @classmethod
    def get_active_streams(cls, organization_uuid: str) -> List['StreamConfig']:
        """Get all active streams for an organization"""
        try:
            # Validate organization UUID
            try:
                UUID(str(organization_uuid))
            except ValueError:
                logger.error(f"Invalid organization UUID format: {organization_uuid}")
                return []

            cursor = mongo.db.streams_v2.find({
                "organization_uuid": str(organization_uuid),
                "status": StreamStatus.ACTIVE,
                "active": True,
                "deleted": {"$ne": True}
            })
            
            return [cls.from_dict(data) for data in cursor]
        except Exception as e:
            logger.error(f"Error fetching active streams: {str(e)}")
            return []

    def get_status_metrics(self) -> Dict[str, Any]:
        """Get comprehensive status and metrics"""
        try:
            metrics = {
                "uuid": self.uuid,
                "name": self.name,
                "status": self.status,
                "active": self.active,
                "uptime": None,
                "message_stats": {
                    "processed": 0,
                    "failed": 0,
                    "pending": 0
                },
                "endpoints": {
                    "source": None,
                    "destination": None
                },
                "last_activity": None,
                "health_status": "unknown",
                "error_info": {
                    "last_error": self.last_error,
                    "last_error_time": self.last_error_time
                }
            }

            # Get endpoints status
            source, dest = self.get_endpoints()
            if source:
                metrics["endpoints"]["source"] = {
                    "status": source.get("status"),
                    "last_active": source.get("last_active")
                }
            if dest:
                metrics["endpoints"]["destination"] = {
                    "status": dest.get("status"),
                    "last_active": dest.get("last_active")
                }

            # Calculate uptime if active
            if self.status == StreamStatus.ACTIVE and self.last_active:
                metrics["uptime"] = (datetime.utcnow() - self.last_active).total_seconds()

            # Get message statistics
            message_stats = mongo.db.messages.aggregate([
                {
                    "$match": {
                        "stream_tracker.stream_uuid": self.uuid
                    }
                },
                {
                    "$group": {
                        "_id": "$stream_tracker.transformation_status",
                        "count": {"$sum": 1}
                    }
                }
            ])

            for stat in message_stats:
                if stat["_id"] == "TRANSFORMED":
                    metrics["message_stats"]["processed"] = stat["count"]
                elif stat["_id"] == "ERROR":
                    metrics["message_stats"]["failed"] = stat["count"]
                elif stat["_id"] in ["QUALIFIED", "QUEUED"]:
                    metrics["message_stats"]["pending"] += stat["count"]

            # Get latest activity
            latest_activity = mongo.db.message_cycle_logs.find_one(
                {"details.stream_uuid": self.uuid},
                sort=[("timestamp", -1)]
            )
            if latest_activity:
                metrics["last_activity"] = latest_activity["timestamp"]

            # Calculate health status
            metrics["health_status"] = self._calculate_health_status(metrics)

            return metrics

        except Exception as e:
            logger.error(f"Error getting status metrics for stream {self.uuid}: {str(e)}")
            return {"error": str(e)}

    def _calculate_health_status(self, metrics: Dict[str, Any]) -> str:
        """Calculate overall health status"""
        try:
            if self.status != StreamStatus.ACTIVE:
                return "inactive"

            # Check endpoint health
            source_status = metrics["endpoints"]["source"]["status"] if metrics["endpoints"]["source"] else None
            dest_status = metrics["endpoints"]["destination"]["status"] if metrics["endpoints"]["destination"] else None
            
            if not source_status or not dest_status:
                return "error"
            
            if source_status != "ACTIVE" or dest_status != "ACTIVE":
                return "degraded"

            # Check error rate
            total_messages = (metrics["message_stats"]["processed"] + 
                            metrics["message_stats"]["failed"])
            if total_messages > 0:
                error_rate = metrics["message_stats"]["failed"] / total_messages
                if error_rate > current_app.config["STREAMSV2_SETTINGS"]["ERROR_THRESHOLD"]:
                    return "degraded"

            # Check activity
            if metrics["last_activity"]:
                inactivity_period = (datetime.utcnow() - metrics["last_activity"]).total_seconds()
                if inactivity_period > current_app.config["STREAMSV2_SETTINGS"]["PROCESSING_DELAY_THRESHOLD"]:
                    return "stalled"

            return "healthy"

        except Exception as e:
            logger.error(f"Error calculating health status: {str(e)}")
            return "unknown"

    def get_endpoints(self) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Get source and destination endpoint details"""
        try:
            source = mongo.db.endpoints.find_one({
                "uuid": str(self.source_endpoint_uuid),
                "organization_uuid": str(self.organization_uuid)
            })
            dest = mongo.db.endpoints.find_one({
                "uuid": str(self.destination_endpoint_uuid),
                "organization_uuid": str(self.organization_uuid)
            })
            return source, dest
        except Exception as e:
            logger.error(f"Error fetching endpoints: {str(e)}")
            return None, None

    def __str__(self) -> str:
        """String representation"""
        return (f"StreamConfig(uuid={self.uuid}, name={self.name}, "
                f"status={self.status}, active={self.active})")