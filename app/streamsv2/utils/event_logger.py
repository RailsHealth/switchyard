# app/streamsv2/utils/event_logger.py

from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
from uuid import uuid4
from flask import current_app

from app.extensions import mongo

logger = logging.getLogger(__name__)

class EventTypes:
    """Event type constants"""
    # Stream Events
    STREAM_STARTED = "stream_started"
    STREAM_STOPPED = "stream_stopped"
    STREAM_ERROR = "stream_error"
    
    # Message Events
    MESSAGE_QUALIFIED = "message_qualified"
    MESSAGE_TRANSFORMED = "message_transformed"
    MESSAGE_DELIVERED = "message_delivered"
    MESSAGE_ERROR = "message_error"
    MESSAGE_CANCELLED = "message_cancelled"
    
    # Batch Events
    BATCH_STARTED = "batch_started"
    BATCH_COMPLETED = "batch_completed"
    BATCH_FAILED = "batch_failed"
    
    # Endpoint Events
    ENDPOINT_ERROR = "endpoint_error"
    ENDPOINT_INACTIVE = "endpoint_inactive"
    
    # Health Events
    HEALTH_CHECK = "health_check"
    ERROR_THRESHOLD = "error_threshold"
    PROCESSING_DELAY = "processing_delay"

class EventSeverity:
    """Event severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"

class EventLogger:
    """
    Handles event logging for streams.
    Manages logging of stream events and message lifecycle.
    """

    def __init__(self, organization_uuid: str):
        """Initialize event logger"""
        self.organization_uuid = organization_uuid

    async def log_event(
        self,
        event_type: str,
        details: Dict[str, Any],
        severity: str = EventSeverity.INFO,
        stream_uuid: Optional[str] = None,
        message_uuid: Optional[str] = None,
        endpoint_uuid: Optional[str] = None
    ) -> str:
        """Log an event to the event_logs collection"""
        try:
            event_id = str(uuid.uuid4())
            
            event = {
                "uuid": event_id,
                "organization_uuid": self.organization_uuid,
                "event_type": event_type,
                "severity": severity,
                "timestamp": datetime.utcnow(),
                "details": details
            }

            # Add optional identifiers
            if stream_uuid:
                event["stream_uuid"] = stream_uuid
            if message_uuid:
                event["message_uuid"] = message_uuid
            if endpoint_uuid:
                event["endpoint_uuid"] = endpoint_uuid

            # Store event
            await mongo.db.event_logs.insert_one(event)

            # Log to application logger for monitoring
            log_level = {
                EventSeverity.ERROR: logging.ERROR,
                EventSeverity.WARNING: logging.WARNING
            }.get(severity, logging.INFO)

            logger.log(
                log_level,
                f"Event [{event_type}] {severity}: {details}"
            )

            return event_id

        except Exception as e:
            logger.error(f"Error logging event: {str(e)}")
            return str(uuid.uuid4())

    async def log_message_lifecycle(
        self,
        message_uuid: str,
        stream_uuid: str,
        event_type: str,
        details: Dict[str, Any],
        severity: str = EventSeverity.INFO
    ) -> str:
        """Log message lifecycle event"""
        try:
            # Add lifecycle stage to details
            details['lifecycle_stage'] = self._get_lifecycle_stage(event_type)
            
            return await self.log_event(
                event_type,
                details,
                severity,
                stream_uuid=stream_uuid,
                message_uuid=message_uuid
            )
        except Exception as e:
            logger.error(f"Error logging message lifecycle: {str(e)}")
            return str(uuid.uuid4())

    def _get_lifecycle_stage(self, event_type: str) -> str:
        """Get lifecycle stage from event type"""
        stage_mapping = {
            EventTypes.MESSAGE_QUALIFIED: "qualification",
            EventTypes.MESSAGE_TRANSFORMED: "transformation",
            EventTypes.MESSAGE_DELIVERED: "delivery",
            EventTypes.MESSAGE_ERROR: "error",
            EventTypes.MESSAGE_CANCELLED: "cancelled"
        }
        return stage_mapping.get(event_type, "unknown")

    async def log_endpoint_event(
        self,
        endpoint_uuid: str,
        event_type: str,
        details: Dict[str, Any],
        severity: str = EventSeverity.INFO,
        stream_uuid: Optional[str] = None
    ) -> str:
        """Log endpoint-related event"""
        try:
            return await self.log_event(
                event_type,
                details,
                severity,
                stream_uuid=stream_uuid,
                endpoint_uuid=endpoint_uuid
            )
        except Exception as e:
            logger.error(f"Error logging endpoint event: {str(e)}")
            return str(uuid.uuid4())

    async def log_batch_completion(
        self,
        stream_uuid: str,
        batch_details: Dict[str, Any],
        success: bool
    ) -> str:
        """Log batch processing completion"""
        try:
            event_type = EventTypes.BATCH_COMPLETED if success else EventTypes.BATCH_FAILED
            severity = EventSeverity.INFO if success else EventSeverity.ERROR
            
            return await self.log_event(
                event_type,
                batch_details,
                severity,
                stream_uuid=stream_uuid
            )
        except Exception as e:
            logger.error(f"Error logging batch completion: {str(e)}")
            return str(uuid.uuid4())

    async def get_stream_events(
        self,
        stream_uuid: str,
        event_types: Optional[List[str]] = None,
        severity: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get events for a stream with filtering"""
        try:
            query = {
                "stream_uuid": stream_uuid,
                "organization_uuid": self.organization_uuid
            }

            if event_types:
                query["event_type"] = {"$in": event_types}
            if severity:
                query["severity"] = severity
            if start_time or end_time:
                query["timestamp"] = {}
                if start_time:
                    query["timestamp"]["$gte"] = start_time
                if end_time:
                    query["timestamp"]["$lte"] = end_time

            return await mongo.db.event_logs.find(query) \
                                          .sort("timestamp", -1) \
                                          .limit(limit) \
                                          .to_list(None)

        except Exception as e:
            logger.error(f"Error getting stream events: {str(e)}")
            return []

    async def get_recent_errors(
        self,
        stream_uuid: str,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get recent error events"""
        try:
            since = datetime.utcnow() - timedelta(hours=hours)
            
            return await self.get_stream_events(
                stream_uuid,
                severity=EventSeverity.ERROR,
                start_time=since
            )
        except Exception as e:
            logger.error(f"Error getting recent errors: {str(e)}")
            return []

    async def get_error_summary(
        self,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get error summary for recent events"""
        try:
            since = datetime.utcnow() - timedelta(hours=hours)
            
            pipeline = [
                {
                    "$match": {
                        "organization_uuid": self.organization_uuid,
                        "severity": EventSeverity.ERROR,
                        "timestamp": {"$gte": since}
                    }
                },
                {
                    "$group": {
                        "_id": "$event_type",
                        "count": {"$sum": 1},
                        "latest": {"$max": "$timestamp"},
                        "errors": {
                            "$push": {
                                "timestamp": "$timestamp",
                                "details": "$details"
                            }
                        }
                    }
                }
            ]

            results = await mongo.db.event_logs.aggregate(pipeline).to_list(None)
            
            return {
                "period_hours": hours,
                "total_errors": sum(r["count"] for r in results),
                "error_types": {
                    r["_id"]: {
                        "count": r["count"],
                        "latest": r["latest"],
                        "recent_errors": r["errors"][-5:]  # Last 5 errors
                    } for r in results
                }
            }

        except Exception as e:
            logger.error(f"Error getting error summary: {str(e)}")
            return {
                "period_hours": hours,
                "total_errors": 0,
                "error_types": {}
            }