# app/streamsv2/utils/stream_logger.py

from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
import uuid
from flask import current_app
import time

from app.extensions import mongo
from .event_logger import EventLogger, EventTypes, EventSeverity

logger = logging.getLogger(__name__)

class StreamLogger:
    """Stream-specific logging functionality with enhanced error tracking and metrics"""

    def __init__(self, stream_uuid: str):
        self.stream_uuid = stream_uuid
        self.logger = logging.getLogger(f'stream-{stream_uuid}')
        self.organization_uuid = self._get_organization_uuid()
        self.event_logger = EventLogger(self.organization_uuid) if self.organization_uuid else None
        self._performance_metrics = {}

    def _get_organization_uuid(self) -> Optional[str]:
        """Get organization UUID from stream configuration"""
        try:
            stream = mongo.db.streams_v2.find_one({"uuid": self.stream_uuid})
            return stream.get('organization_uuid') if stream else None
        except Exception as e:
            logger.error(f"Error getting organization UUID: {str(e)}")
            return None

    def log_event(self, event_type: str, details: Dict[str, Any], severity: str = EventSeverity.INFO) -> None:
        """Log stream event with enhanced context"""
        try:
            if not self.organization_uuid:
                logger.error("Unable to log event - missing organization_uuid")
                return

            # Add performance metrics if available
            if event_type in ['stream_started', 'stream_stopped', 'message_processed']:
                details['performance_metrics'] = self._get_performance_metrics()

            # Add error context for errors
            if severity == EventSeverity.ERROR:
                self._update_stream_error(details.get('error'))

            event_data = {
                "uuid": str(uuid.uuid4()),
                "stream_uuid": self.stream_uuid,
                "organization_uuid": self.organization_uuid,
                "event_type": event_type,
                "severity": severity,
                "timestamp": datetime.utcnow(),
                "details": details,
                "context": {
                    "stream_status": self._get_stream_status(),
                    "memory_usage": self._get_memory_usage()
                }
            }

            # Store event
            mongo.db.event_logs.insert_one(event_data)

            # Log to application logger
            log_level = {
                EventSeverity.ERROR: logging.ERROR,
                EventSeverity.WARNING: logging.WARNING
            }.get(severity, logging.INFO)

            self.logger.log(log_level, f"Event [{event_type}] {severity}: {details}")

        except Exception as e:
            logger.error(f"Error logging stream event: {str(e)}")

    def log_message_cycle(self, message_uuid: str, event_type: str, details: Dict[str, Any], status: str = "success") -> None:
        """Log message lifecycle event with cycle tracking"""
        try:
            start_time = time.time()
            
            severity = {
                "success": EventSeverity.INFO,
                "warning": EventSeverity.WARNING,
                "error": EventSeverity.ERROR
            }.get(status, EventSeverity.INFO)

            # Add message cycle metadata
            cycle_details = {
                "message_uuid": message_uuid,
                "stream_uuid": self.stream_uuid,
                "cycle_stage": self._get_lifecycle_stage(event_type),
                "processing_time": time.time() - start_time,
                **details
            }

            # Track performance metrics
            self._update_performance_metrics(event_type, cycle_details)

            # Log cycle event
            self.log_event(
                event_type=event_type,
                details=cycle_details,
                severity=severity
            )

            # Update message cycle tracking
            self._update_message_cycle_tracking(message_uuid, event_type, status)

        except Exception as e:
            logger.error(f"Error logging message cycle: {str(e)}")

    def _get_lifecycle_stage(self, event_type: str) -> str:
        """Get current lifecycle stage from event type"""
        stage_mapping = {
            "message_qualified": "qualification",
            "message_transformed": "transformation",
            "message_delivered": "delivery",
            "message_error": "error",
            "message_cancelled": "cancelled"
        }
        return stage_mapping.get(event_type, "unknown")

    def _update_performance_metrics(self, event_type: str, details: Dict[str, Any]) -> None:
        """Update performance metrics based on event"""
        try:
            current_time = time.time()
            
            if event_type not in self._performance_metrics:
                self._performance_metrics[event_type] = {
                    "count": 0,
                    "total_time": 0,
                    "min_time": float('inf'),
                    "max_time": 0,
                    "last_occurrence": None
                }

            metrics = self._performance_metrics[event_type]
            processing_time = details.get('processing_time', 0)

            metrics["count"] += 1
            metrics["total_time"] += processing_time
            metrics["min_time"] = min(metrics["min_time"], processing_time)
            metrics["max_time"] = max(metrics["max_time"], processing_time)
            metrics["last_occurrence"] = current_time

            # Update stream metrics in database
            self._update_stream_metrics(event_type, processing_time)

        except Exception as e:
            logger.error(f"Error updating performance metrics: {str(e)}")

    def _update_stream_metrics(self, event_type: str, processing_time: float) -> None:
        """Update stream-level metrics"""
        try:
            metrics_update = {
                "updated_at": datetime.utcnow(),
                f"metrics.{event_type}_count": 1,
                f"metrics.{event_type}_total_time": processing_time
            }

            if event_type == "message_error":
                metrics_update["metrics.error_count"] = 1

            mongo.db.streams_v2.update_one(
                {"uuid": self.stream_uuid},
                {
                    "$inc": metrics_update,
                    "$set": {
                        "metrics.last_processed": datetime.utcnow(),
                        "metrics.average_processing_time": processing_time
                    }
                }
            )

        except Exception as e:
            logger.error(f"Error updating stream metrics: {str(e)}")

    def _update_message_cycle_tracking(self, message_uuid: str, event_type: str, status: str) -> None:
        """Update message cycle tracking"""
        try:
            tracking_data = {
                "message_uuid": message_uuid,
                "stream_uuid": self.stream_uuid,
                "event_type": event_type,
                "status": status,
                "timestamp": datetime.utcnow()
            }

            mongo.db.message_cycle_logs.insert_one(tracking_data)

        except Exception as e:
            logger.error(f"Error updating message cycle tracking: {str(e)}")

    def _update_stream_error(self, error: str) -> None:
        """Update stream error tracking"""
        try:
            mongo.db.streams_v2.update_one(
                {"uuid": self.stream_uuid},
                {
                    "$set": {
                        "last_error": error,
                        "last_error_time": datetime.utcnow()
                    }
                }
            )
        except Exception as e:
            logger.error(f"Error updating stream error: {str(e)}")

    def _get_stream_status(self) -> Dict[str, Any]:
        """Get current stream status"""
        try:
            stream = mongo.db.streams_v2.find_one(
                {"uuid": self.stream_uuid},
                {"status": 1, "active": 1, "last_active": 1}
            )
            return {
                "status": stream.get("status"),
                "active": stream.get("active"),
                "last_active": stream.get("last_active")
            } if stream else {}
        except Exception as e:
            logger.error(f"Error getting stream status: {str(e)}")
            return {}

    def _get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage metrics"""
        try:
            import psutil
            process = psutil.Process()
            return {
                "memory_percent": process.memory_percent(),
                "memory_info": dict(process.memory_info()._asdict())
            }
        except Exception:
            return {}

    def _get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        return {
            event_type: {
                "count": metrics["count"],
                "avg_time": metrics["total_time"] / metrics["count"] if metrics["count"] > 0 else 0,
                "min_time": metrics["min_time"] if metrics["min_time"] != float('inf') else 0,
                "max_time": metrics["max_time"]
            }
            for event_type, metrics in self._performance_metrics.items()
        }

    def get_recent_activity(self, hours: int = 24, include_debug: bool = False) -> List[Dict[str, Any]]:
        """Get recent stream activity"""
        try:
            query = {
                "stream_uuid": self.stream_uuid,
                "timestamp": {"$gte": datetime.utcnow() - timedelta(hours=hours)}
            }

            if not include_debug:
                query["severity"] = {"$ne": "DEBUG"}

            return list(mongo.db.event_logs.find(query).sort("timestamp", -1))

        except Exception as e:
            logger.error(f"Error getting recent activity: {str(e)}")
            return []

    def get_error_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get summary of recent errors"""
        try:
            query = {
                "stream_uuid": self.stream_uuid,
                "severity": EventSeverity.ERROR,
                "timestamp": {"$gte": datetime.utcnow() - timedelta(hours=hours)}
            }

            errors = list(mongo.db.event_logs.find(query).sort("timestamp", -1))
            
            return {
                "total_errors": len(errors),
                "recent_errors": errors[:5],
                "error_types": self._group_errors(errors),
                "period_hours": hours
            }

        except Exception as e:
            logger.error(f"Error getting error summary: {str(e)}")
            return {
                "total_errors": 0,
                "recent_errors": [],
                "error_types": {},
                "period_hours": hours
            }

    def _group_errors(self, errors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Group errors by type for analysis"""
        error_types = {}
        
        for error in errors:
            error_type = error.get('event_type', 'unknown')
            if error_type not in error_types:
                error_types[error_type] = {
                    "count": 0,
                    "recent_examples": []
                }
            
            error_types[error_type]["count"] += 1
            if len(error_types[error_type]["recent_examples"]) < 3:
                error_types[error_type]["recent_examples"].append(error)
        
        return error_types