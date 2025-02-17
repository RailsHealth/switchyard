# app/streamsv2/models/destination_message.py

from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json
import logging
from uuid import uuid4

from app.extensions import mongo
from app.utils.logging_utils import log_message_cycle
from app.streamsv2.models.stream_status import MessageStatus

logger = logging.getLogger(__name__)

class DestinationStatus:
    """Destination message status constants"""
    READY = "READY"           # Initial state, ready for processing
    PROCESSING = "PROCESSING" # Currently being processed
    COMPLETED = "COMPLETED"   # Successfully delivered
    FAILED = "FAILED"        # Delivery failed, may be retried
    CANCELLED = "CANCELLED"   # Processing cancelled, no retry
    RETRY_PENDING = "RETRY_PENDING"  # Waiting for retry attempt
    DELIVERY_READY = "DELIVERY_READY"  # Ready for delivery attempt

@dataclass
class DestinationMessage:
    """
    Represents a message ready for destination delivery.
    Handles state management and delivery tracking.
    """
    uuid: str
    original_message_uuid: str
    stream_uuid: str
    organization_uuid: str
    transformed_content: Dict[str, Any]
    destinations: List[Dict[str, Any]]
    overall_status: str = "PENDING"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # New delivery tracking fields
    delivery_attempts: int = field(default=0)
    last_delivery_time: Optional[datetime] = field(default=None)
    next_retry_time: Optional[datetime] = field(default=None)
    final_failure: bool = field(default=False)
    delivery_history: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def create(cls, message_data: Dict[str, Any]) -> Optional[str]:
        """Create new destination message"""
        try:
            # Set required fields
            message_uuid = str(uuid4())
            now = datetime.utcnow()
            
            # Initialize destinations list
            destinations = message_data.get('destinations', [])
            for dest in destinations:
                dest.update({
                    "status": DestinationStatus.DELIVERY_READY,  # Updated initial status
                    "processing_details": {
                        "attempts": 0,
                        "last_attempt": None,
                        "next_retry_at": None,
                        "error": None,
                        "delivery_start": None,
                        "delivery_complete": None
                    }
                })

            # Prepare message document
            message_doc = {
                "uuid": message_uuid,
                "original_message_uuid": str(message_data["original_message_uuid"]),
                "stream_uuid": str(message_data["stream_uuid"]),
                "organization_uuid": str(message_data["organization_uuid"]),
                "transformed_content": message_data["transformed_content"],
                "destinations": destinations,
                "overall_status": "PENDING",
                "created_at": now,
                "updated_at": now,
                "delivery_attempts": 0,
                "last_delivery_time": None,
                "next_retry_time": None,
                "final_failure": False,
                "delivery_history": []
            }

            result = mongo.db.destination_messages.insert_one(message_doc)
            if not result.acknowledged:
                raise ValueError("Failed to create destination message")

            # Log creation
            log_message_cycle(
                message_uuid=message_uuid,
                event_type="destination_message_created",
                details={
                    "stream_uuid": message_data["stream_uuid"],
                    "original_message_uuid": message_data["original_message_uuid"]
                },
                status="success"
            )

            return message_uuid

        except Exception as e:
            logger.error(f"Error creating destination message: {str(e)}")
            return None

    @classmethod
    def get_by_uuid(cls, uuid: str) -> Optional['DestinationMessage']:
        """Get destination message by UUID"""
        try:
            doc = mongo.db.destination_messages.find_one({"uuid": str(uuid)})
            if not doc:
                return None
            return cls(**doc)
        except Exception as e:
            logger.error(f"Error getting destination message: {str(e)}")
            return None

    def update_destination_status(
        self,
        endpoint_uuid: str,
        status: str,
        details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update status for specific destination"""
        try:
            now = datetime.utcnow()
            update_data = {
                "destinations.$[dest].status": status,
                "destinations.$[dest].processing_details.last_attempt": now,
                "updated_at": now
            }

            if details:
                for key, value in details.items():
                    update_data[f"destinations.$[dest].processing_details.{key}"] = value

            # Update delivery tracking fields
            if status == DestinationStatus.PROCESSING:
                update_data["delivery_attempts"] = self.delivery_attempts + 1
                update_data["last_delivery_time"] = now
            elif status == DestinationStatus.COMPLETED:
                update_data["next_retry_time"] = None
                update_data["final_failure"] = False

            # Add to delivery history
            history_entry = {
                "status": status,
                "timestamp": now,
                "endpoint_uuid": endpoint_uuid,
                "details": details or {}
            }
            update_data["$push"] = {"delivery_history": history_entry}

            result = mongo.db.destination_messages.update_one(
                {"uuid": str(self.uuid)},
                {"$set": update_data},
                array_filters=[{"dest.endpoint_uuid": str(endpoint_uuid)}]
            )

            if result.modified_count > 0:
                self._update_overall_status()
                
                # Log status update
                log_message_cycle(
                    message_uuid=self.uuid,
                    event_type="destination_status_updated",
                    details={
                        "endpoint_uuid": endpoint_uuid,
                        "status": status,
                        "attempts": self.delivery_attempts + 1,
                        **(details if details else {}) 
                    },
                    status="success"
                )
                return True

            return False

        except Exception as e:
            logger.error(f"Error updating destination status: {str(e)}")
            return False

    def update_delivery_status(
        self,
        status: str,
        error: Optional[str] = None,
        retry_info: Optional[Dict] = None
    ) -> bool:
        """Update delivery status with error tracking"""
        try:
            now = datetime.utcnow()
            update_data = {
                "updated_at": now,
                "overall_status": status
            }
            
            if error:
                update_data["last_error"] = error
                update_data["last_error_time"] = now
                
            if retry_info:
                update_data["next_retry_time"] = retry_info.get("next_retry_time")
                update_data["delivery_attempts"] = retry_info.get("attempts", self.delivery_attempts + 1)
                
            if status == MessageStatus.DELIVERED:
                update_data["last_delivery_time"] = now

            # Add to delivery history
            history_entry = {
                "status": status,
                "timestamp": now,
                "error": error,
                "retry_info": retry_info
            }
            update_data["$push"] = {"delivery_history": history_entry}

            result = mongo.db.destination_messages.update_one(
                {"uuid": str(self.uuid)},
                {
                    "$set": update_data
                }
            )
            
            return result.modified_count > 0

        except Exception as e:
            logger.error(f"Error updating delivery status: {str(e)}")
            return False

    def _update_overall_status(self) -> None:
        """Update overall message status based on destinations"""
        try:
            message = mongo.db.destination_messages.find_one({"uuid": str(self.uuid)})
            if not message:
                return

            destinations = message.get('destinations', [])
            if not destinations:
                return

            statuses = [d['status'] for d in destinations]
            
            if all(s == DestinationStatus.COMPLETED for s in statuses):
                overall_status = "COMPLETED"
            elif any(s == DestinationStatus.PROCESSING for s in statuses):
                overall_status = "IN_PROGRESS"
            elif any(s == DestinationStatus.RETRY_PENDING for s in statuses):
                overall_status = "RETRY_PENDING"
            elif any(s == DestinationStatus.FAILED for s in statuses):
                overall_status = "FAILED"
            else:
                overall_status = "PENDING"

            mongo.db.destination_messages.update_one(
                {"uuid": str(self.uuid)},
                {"$set": {"overall_status": overall_status}}
            )

        except Exception as e:
            logger.error(f"Error updating overall status: {str(e)}")

    def mark_destination_failed(
        self,
        endpoint_uuid: str,
        error: str,
        retry: bool = True,
        max_retries: int = 3,
        retry_delay: int = 300
    ) -> bool:
        """Mark destination as failed with retry information"""
        try:
            now = datetime.utcnow()
            details = {
                "error": {
                    "message": error,
                    "timestamp": now
                },
                "attempts": self.delivery_attempts + 1
            }

            if retry and self.delivery_attempts < max_retries:
                retry_delay = retry_delay * (2 ** self.delivery_attempts)
                details["next_retry_at"] = now + timedelta(seconds=retry_delay)
                details["retry_scheduled"] = True
                status = DestinationStatus.RETRY_PENDING
            else:
                details["final_failure"] = True
                status = DestinationStatus.FAILED
            
            return self.update_destination_status(
                endpoint_uuid,
                status,
                details
            )

        except Exception as e:
            logger.error(f"Error marking destination failed: {str(e)}")
            return False

    def mark_destination_completed(
        self,
        endpoint_uuid: str,
        delivery_details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Mark destination as completed with delivery details"""
        try:
            details = {
                "completed_at": datetime.utcnow(),
                "delivery_complete": True,
                **(delivery_details or {})
            }
            
            return self.update_destination_status(
                endpoint_uuid,
                DestinationStatus.COMPLETED,
                details
            )

        except Exception as e:
            logger.error(f"Error marking destination completed: {str(e)}")
            return False

    def cancel_destination(
        self,
        endpoint_uuid: str,
        reason: str = "Processing cancelled"
    ) -> bool:
        """Cancel destination processing"""
        try:
            details = {
                "cancelled_at": datetime.utcnow(),
                "cancel_reason": reason,
                "final_status": True
            }
            
            return self.update_destination_status(
                endpoint_uuid,
                DestinationStatus.CANCELLED,
                details
            )

        except Exception as e:
            logger.error(f"Error cancelling destination: {str(e)}")
            return False

    def schedule_retry(
        self,
        endpoint_uuid: str,
        retry_delay: int = 300,
        max_retries: int = 3
    ) -> bool:
        """Schedule message for retry"""
        try:
            if self.delivery_attempts >= max_retries:
                return self.mark_destination_failed(
                    endpoint_uuid,
                    "Max retries exceeded",
                    retry=False
                )
                
            retry_delay = retry_delay * (2 ** self.delivery_attempts)
            next_retry = datetime.utcnow() + timedelta(seconds=retry_delay)
            
            details = {
                "next_retry_at": next_retry,
                "attempts": self.delivery_attempts + 1,
                "retry_scheduled": True
            }
            
            return self.update_destination_status(
                endpoint_uuid,
                DestinationStatus.RETRY_PENDING,
                details
            )
            
        except Exception as e:
            logger.error(f"Error scheduling retry: {str(e)}")
            return False

    def get_delivery_history(self) -> List[Dict[str, Any]]:
        """Get delivery attempt history"""
        try:
            return self.delivery_history
        except Exception as e:
            logger.error(f"Error getting delivery history: {str(e)}")
            return []

    @classmethod
    def cleanup_old_messages(cls, stream_uuid: str, days: int = 30) -> int:
        """Clean up old completed messages"""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            result = mongo.db.destination_messages.delete_many({
                "stream_uuid": str(stream_uuid),
                "overall_status": "COMPLETED",
                "updated_at": {"$lt": cutoff}
            })
            return result.deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old messages: {str(e)}")
            return 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format"""
        return {
            "uuid": str(self.uuid),
            "original_message_uuid": str(self.original_message_uuid),
            "stream_uuid": str(self.stream_uuid),
            "organization_uuid": str(self.organization_uuid),
            "transformed_content": self.transformed_content,
            "destinations": self.destinations,
            "overall_status": self.overall_status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "delivery_attempts": self.delivery_attempts,
            "last_delivery_time": self.last_delivery_time,
            "next_retry_time": self.next_retry_time,
            "final_failure": self.final_failure,
            "delivery_history": self.delivery_history
        }

    def __str__(self) -> str:
        """String representation"""
        return f"DestinationMessage(uuid={self.uuid}, status={self.overall_status})"