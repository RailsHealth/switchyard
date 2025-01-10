from typing import Dict, Any, Optional
from datetime import datetime
import logging
from flask import current_app
from app.extensions import mongo
from app.utils.logging_utils import log_message_cycle
import uuid

logger = logging.getLogger(__name__)

class StreamLogger:
    """
    Stream-specific logging functionality
    
    Provides:
    - Stream event logging
    - Message cycle tracking
    - Error logging
    - Performance monitoring
    """

    def __init__(self, stream_uuid: str, organization_uuid: str):
        self.stream_uuid = stream_uuid
        self.organization_uuid = organization_uuid
        self.logger = logging.getLogger(f'stream-{stream_uuid}')

    def log_stream_event(
        self,
        event_type: str,
        details: Dict[str, Any],
        status: str,
        message_uuid: Optional[str] = None
    ) -> None:
        """
        Log stream-related event
        
        Args:
            event_type: Type of event (e.g., 'stream_started', 'message_processed')
            details: Additional event details
            status: Event status ('success', 'error', 'warning')
            message_uuid: Optional UUID of related message
        """
        try:
            # Create event log entry
            event_id = str(uuid.uuid4())
            log_entry = {
                "event_id": event_id,
                "stream_uuid": self.stream_uuid,
                "organization_uuid": self.organization_uuid,
                "event_type": event_type,
                "status": status,
                "timestamp": datetime.utcnow(),
                "details": details
            }
            
            if message_uuid:
                log_entry["message_uuid"] = message_uuid

            # Store in database
            mongo.db.stream_event_logs.insert_one(log_entry)

            # Also log to message cycle if message_uuid is provided
            if message_uuid:
                log_message_cycle(
                    message_uuid=message_uuid,
                    event_type=f"stream_{event_type}",
                    details={
                        "stream_uuid": self.stream_uuid,
                        "event_id": event_id,
                        **details
                    },
                    status=status,
                    organization_uuid=self.organization_uuid
                )

            # Log to application logger
            log_level = {
                'error': logging.ERROR,
                'warning': logging.WARNING
            }.get(status.lower(), logging.INFO)
            
            self.logger.log(
                log_level,
                f"Stream event: {event_type} - {status} - {details}"
            )

        except Exception as e:
            logger.error(f"Error logging stream event: {str(e)}")

    def log_error(
        self,
        error: Exception,
        context: Dict[str, Any],
        message_uuid: Optional[str] = None
    ) -> None:
        """
        Log stream error with context
        
        Args:
            error: Exception that occurred
            context: Error context information
            message_uuid: Optional UUID of related message
        """
        error_details = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "context": context,
            "timestamp": datetime.utcnow().isoformat()
        }

        self.log_stream_event(
            event_type="error",
            details=error_details,
            status="error",
            message_uuid=message_uuid
        )

    def log_performance(
        self,
        operation: str,
        duration: float,
        details: Dict[str, Any]
    ) -> None:
        """
        Log performance metrics
        
        Args:
            operation: Operation being measured
            duration: Duration in seconds
            details: Additional performance details
        """
        performance_data = {
            "operation": operation,
            "duration_seconds": duration,
            **details
        }

        self.log_stream_event(
            event_type="performance",
            details=performance_data,
            status="success"
        )

    @classmethod
    def get_stream_events(
        cls,
        stream_uuid: str,
        organization_uuid: str,
        event_type: Optional[str] = None,
        status: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get stream events with filtering
        
        Args:
            stream_uuid: Stream identifier
            organization_uuid: Organization identifier
            event_type: Optional event type filter
            status: Optional status filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            limit: Maximum number of events to return
        """
        try:
            # Build query
            query = {
                "stream_uuid": stream_uuid,
                "organization_uuid": organization_uuid
            }

            if event_type:
                query["event_type"] = event_type
            if status:
                query["status"] = status
            
            # Add time range if specified
            if start_time or end_time:
                query["timestamp"] = {}
                if start_time:
                    query["timestamp"]["$gte"] = start_time
                if end_time:
                    query["timestamp"]["$lte"] = end_time

            # Get events
            events = list(
                mongo.db.stream_event_logs.find(query)
                .sort("timestamp", -1)
                .limit(limit)
            )

            return events

        except Exception as e:
            logger.error(f"Error fetching stream events: {str(e)}")
            return []

    @classmethod
    def get_error_summary(
        cls,
        stream_uuid: str,
        organization_uuid: str,
        time_window: timedelta = timedelta(hours=24)
    ) -> Dict[str, Any]:
        """
        Get summary of stream errors
        
        Args:
            stream_uuid: Stream identifier
            organization_uuid: Organization identifier
            time_window: Time window for error summary
        """
        try:
            start_time = datetime.utcnow() - time_window
            
            # Get error events
            errors = cls.get_stream_events(
                stream_uuid=stream_uuid,
                organization_uuid=organization_uuid,
                event_type="error",
                status="error",
                start_time=start_time
            )

            # Group errors by type
            error_types = {}
            for error in errors:
                error_type = error['details'].get('error_type', 'unknown')
                if error_type not in error_types:
                    error_types[error_type] = 0
                error_types[error_type] += 1

            return {
                "total_errors": len(errors),
                "error_types": error_types,
                "time_window": time_window.total_seconds(),
                "latest_error": errors[0] if errors else None
            }

        except Exception as e:
            logger.error(f"Error getting error summary: {str(e)}")
            return {
                "total_errors": 0,
                "error_types": {},
                "error": str(e)
            }