# app/streamsv2/services/monitoring.py

from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
from flask import current_app

from app.extensions import mongo
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.models.stream_status import StreamStatus, MessageStatus
from app.streamsv2.utils.stream_logger import StreamLogger
from app.endpoints.registry import endpoint_registry

logger = logging.getLogger(__name__)

class StreamHealthError(Exception):
    """Error indicating stream health issues"""
    pass

class StreamMonitor:
    """Monitors stream health and collects metrics"""

    def __init__(self, stream_config: StreamConfig):
        self.config = stream_config
        self.logger = StreamLogger(stream_config.uuid)
        
        # Configure monitoring thresholds
        self.thresholds = {
            'error_rate': 0.1,  # 10% error threshold
            'processing_delay': 300,  # 5 minutes max delay
            'processing_timeout': 3600,  # 1 hour message timeout
            'max_failed_deliveries': 100,  # Maximum allowed delivery failures
            'stuck_delivery_timeout': 1800  # 30 minutes for stuck deliveries
        }

    def check_stream_health(self) -> Dict[str, Any]:
        """Check overall stream health including delivery status"""
        try:
            health_status = {
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "issues": [],
                "metrics": {}
            }

            # Check endpoints status through registry
            endpoints_status = self._check_endpoints()
            if not endpoints_status["healthy"]:
                health_status["status"] = "warning"
                health_status["issues"].extend(endpoints_status["issues"])

            # Check error rates
            error_rates = self._check_error_rates()
            health_status["metrics"].update(error_rates)
            if error_rates["total_error_rate"] > self.thresholds["error_rate"]:
                health_status["status"] = "warning"
                health_status["issues"].append(
                    f"High error rate: {error_rates['total_error_rate']:.1%}"
                )

            # Check processing state
            processing_status = self._check_processing_state()
            if not processing_status["healthy"]:
                health_status["status"] = "warning"
                health_status["issues"].extend(processing_status["issues"])

            # Check delivery health
            delivery_health = self._check_delivery_health()
            if delivery_health["status"] != "healthy":
                health_status["status"] = "warning"
                health_status["issues"].extend(delivery_health["issues"])
            health_status["metrics"]["delivery"] = delivery_health["metrics"]

            # Log status
            self.logger.log_event(
                "health_check_completed",
                {
                    "status": health_status["status"],
                    "issues": health_status["issues"],
                    "metrics": health_status["metrics"]
                },
                health_status["status"]
            )

            return health_status

        except Exception as e:
            logger.error(f"Error checking stream health: {str(e)}")
            return {
                "status": "error",
                "timestamp": datetime.utcnow().isoformat(),
                "issues": [str(e)],
                "metrics": {}
            }

    def _check_endpoints(self) -> Dict[str, Any]:
        """Check source and destination endpoint health through registry"""
        try:
            issues = []
            
            # Get endpoints from registry
            source_endpoint = endpoint_registry.get_endpoint(self.config.source_endpoint_uuid)
            dest_endpoint = endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            
            if not source_endpoint or not dest_endpoint:
                issues.append("One or more endpoints not found in registry")
                return {
                    "healthy": False,
                    "issues": issues
                }

            # Check endpoint statuses
            source_status = source_endpoint.get_status()
            dest_status = dest_endpoint.get_status()

            if source_status.get('status') != 'ACTIVE':
                issues.append(f"Source endpoint not active: {source_status.get('status')}")
            if dest_status.get('status') != 'ACTIVE':
                issues.append(f"Destination endpoint not active: {dest_status.get('status')}")

            return {
                "healthy": len(issues) == 0,
                "issues": issues,
                "source_status": source_status,
                "destination_status": dest_status
            }

        except Exception as e:
            logger.error(f"Error checking endpoints: {str(e)}")
            return {
                "healthy": False,
                "issues": [f"Error checking endpoints: {str(e)}"]
            }

    def _check_error_rates(self) -> Dict[str, float]:
        """Calculate transformation and delivery error rates"""
        try:
            since = datetime.utcnow() - timedelta(hours=1)
            
            # Get transformation errors
            transform_total = mongo.db.messages.count_documents({
                "stream_tracker": {
                    "$elemMatch": {
                        "stream_uuid": self.config.uuid,
                        "updated_at": {"$gte": since}
                    }
                }
            })

            transform_errors = mongo.db.messages.count_documents({
                "stream_tracker": {
                    "$elemMatch": {
                        "stream_uuid": self.config.uuid,
                        "transformation_status": MessageStatus.ERROR,
                        "updated_at": {"$gte": since}
                    }
                }
            })

            # Get delivery errors
            delivery_total = mongo.db.destination_messages.count_documents({
                "stream_uuid": self.config.uuid,
                "updated_at": {"$gte": since}
            })

            delivery_errors = mongo.db.destination_messages.count_documents({
                "stream_uuid": self.config.uuid,
                "destinations": {
                    "$elemMatch": {
                        "endpoint_uuid": self.config.destination_endpoint_uuid,
                        "status": "FAILED",
                        "processing_details.last_attempt": {"$gte": since}
                    }
                }
            })

            # Calculate rates
            transform_rate = transform_errors / transform_total if transform_total > 0 else 0
            delivery_rate = delivery_errors / delivery_total if delivery_total > 0 else 0
            total_rate = (transform_errors + delivery_errors) / (transform_total + delivery_total) if (transform_total + delivery_total) > 0 else 0

            return {
                "transformation_error_rate": transform_rate,
                "delivery_error_rate": delivery_rate,
                "total_error_rate": total_rate
            }

        except Exception as e:
            logger.error(f"Error calculating error rates: {str(e)}")
            return {
                "transformation_error_rate": 0,
                "delivery_error_rate": 0,
                "total_error_rate": 0
            }

    def _check_processing_state(self) -> Dict[str, Any]:
        """Check message processing and delivery state"""
        try:
            issues = []
            current_time = datetime.utcnow()

            # Check for stuck messages in processing
            stuck_processing = mongo.db.messages.count_documents({
                "stream_tracker": {
                    "$elemMatch": {
                        "stream_uuid": self.config.uuid,
                        "transformation_status": MessageStatus.QUEUED,
                        "updated_at": {"$lt": current_time - timedelta(seconds=self.thresholds["processing_timeout"])}
                    }
                }
            })

            if stuck_processing > 0:
                issues.append(f"{stuck_processing} messages stuck in processing")

            # Check for stuck deliveries
            stuck_deliveries = mongo.db.destination_messages.count_documents({
                "stream_uuid": self.config.uuid,
                "destinations": {
                    "$elemMatch": {
                        "endpoint_uuid": self.config.destination_endpoint_uuid,
                        "status": "PROCESSING",
                        "processing_details.last_attempt": {"$lt": current_time - timedelta(seconds=self.thresholds["stuck_delivery_timeout"])}
                    }
                }
            })

            if stuck_deliveries > 0:
                issues.append(f"{stuck_deliveries} messages stuck in delivery")

            # Check processing delays
            delayed_messages = mongo.db.messages.count_documents({
                "stream_tracker": {
                    "$elemMatch": {
                        "stream_uuid": self.config.uuid,
                        "transformation_status": MessageStatus.QUALIFIED,
                        "qualified_at": {"$lt": current_time - timedelta(seconds=self.thresholds["processing_delay"])}
                    }
                }
            })

            if delayed_messages > 0:
                issues.append(f"{delayed_messages} messages delayed in processing")

            return {
                "healthy": len(issues) == 0,
                "issues": issues,
                "stuck_processing": stuck_processing,
                "stuck_deliveries": stuck_deliveries,
                "delayed_messages": delayed_messages
            }

        except Exception as e:
            logger.error(f"Error checking processing state: {str(e)}")
            return {
                "healthy": False,
                "issues": [f"Error checking processing state: {str(e)}"],
                "stuck_processing": 0,
                "stuck_deliveries": 0,
                "delayed_messages": 0
            }

    def _check_delivery_health(self) -> Dict[str, Any]:
        """Check delivery system health"""
        try:
            health_status = {
                "status": "healthy",
                "issues": [],
                "metrics": {}
            }

            # Get delivery metrics
            metrics = self._get_delivery_metrics()
            health_status["metrics"] = metrics

            # Check failure threshold
            if metrics.get("failed", 0) > self.thresholds["max_failed_deliveries"]:
                health_status["status"] = "warning"
                health_status["issues"].append(
                    f"High number of delivery failures: {metrics['failed']}"
                )

            # Check retry metrics
            retry_metrics = metrics.get("retry_metrics", {})
            if retry_metrics.get("pending_retries", 0) > 0:
                health_status["status"] = "warning"
                health_status["issues"].append(
                    f"{retry_metrics['pending_retries']} messages pending retry"
                )

            return health_status

        except Exception as e:
            logger.error(f"Error checking delivery health: {str(e)}")
            return {
                "status": "error",
                "issues": [str(e)],
                "metrics": {}
            }

    def _get_delivery_metrics(self) -> Dict[str, Any]:
        """Get comprehensive delivery metrics"""
        try:
            # Get base metrics
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
                        "avg_time": {
                            "$avg": {
                                "$cond": [
                                    {"$eq": ["$destinations.status", "COMPLETED"]},
                                    {
                                        "$subtract": [
                                            "$destinations.processing_details.completed_at",
                                            "$destinations.processing_details.last_attempt"
                                        ]
                                    },
                                    None
                                ]
                            }
                        }
                    }
                }
            ]

            results = list(mongo.db.destination_messages.aggregate(pipeline))
            
            metrics = {
                "processed": 0,
                "failed": 0,
                "in_progress": 0,
                "pending": 0,
                "average_delivery_time": 0
            }

            for result in results:
                status = result["_id"].lower()
                count = result["count"]
                
                if status == "completed":
                    metrics["processed"] = count
                    if result["avg_time"]:
                        metrics["average_delivery_time"] = result["avg_time"] / 1000
                elif status == "failed":
                    metrics["failed"] = count
                elif status == "processing":
                    metrics["in_progress"] = count
                elif status in ["ready", "retry_pending"]:
                    metrics["pending"] += count

            # Get endpoint metrics
            endpoint = endpoint_registry.get_endpoint(self.config.destination_endpoint_uuid)
            if endpoint:
                metrics["endpoint_metrics"] = endpoint.get_metrics()

            # Add retry metrics
            retry_metrics = self._get_retry_metrics()
            metrics.update(retry_metrics)

            return metrics

        except Exception as e:
            logger.error(f"Error getting delivery metrics: {str(e)}")
            return {}

    def _get_retry_metrics(self) -> Dict[str, Any]:
        """Get retry statistics"""
        try:
            pipeline = [
                {
                    "$match": {
                        "stream_uuid": self.config.uuid,
                        "destinations.status": "RETRY_PENDING"
                    }
                },
                {
                    "$unwind": "$destinations"
                },
                {
                    "$match": {
                        "destinations.status": "RETRY_PENDING"
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_attempts": {"$avg": "$destinations.processing_details.attempts"},
                        "max_attempts": {"$max": "$destinations.processing_details.attempts"},
                        "count": {"$sum": 1}
                    }
                }
            ]

            results = list(mongo.db.destination_messages.aggregate(pipeline))
            
            if not results:
                return {
                    "retry_metrics": {
                        "average_attempts": 0,
                        "max_attempts": 0,
                        "pending_retries": 0
                    }
                }

            result = results[0]
            return {
                "retry_metrics": {
                    "average_attempts": round(result["avg_attempts"], 2),
                    "max_attempts": result["max_attempts"],
                    "pending_retries": result["count"]
                }
            }

        except Exception as e:
            logger.error(f"Error getting retry metrics: {str(e)}")
            return {
                "retry_metrics": {
                    "error": str(e)
                }
            }