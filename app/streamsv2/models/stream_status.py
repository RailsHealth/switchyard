# app/streamsv2/models/stream_status.py

from enum import Enum
from typing import Dict, List, Optional, Set
from datetime import datetime

class StreamStatus:
    """Stream status constants"""
    # Core states
    INACTIVE = "INACTIVE"
    ACTIVE = "ACTIVE"
    STARTING = "STARTING"  
    ERROR = "ERROR"
    DELETED = "DELETED"
    STOPPING = "STOPPING"
    CANCELLING = "CANCELLING"
    
    # Delivery states
    DELIVERY_PENDING = "DELIVERY_PENDING"
    DELIVERING = "DELIVERING"
    DELIVERY_FAILED = "DELIVERY_FAILED"

class MessageStatus:
    """Message status constants"""
    # Core transformation states
    QUALIFIED = "QUALIFIED"
    QUEUED = "QUEUED"
    TRANSFORMED = "TRANSFORMED"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"
    
    # Delivery states
    DELIVERY_READY = "DELIVERY_READY"
    DELIVERING = "DELIVERING"
    DELIVERY_FAILED = "DELIVERY_FAILED"
    RETRY_PENDING = "RETRY_PENDING"
    DELIVERED = "DELIVERED"

class StatusTransitionError(Exception):
    """Invalid status transition error"""
    pass

class StatusValidationError(Exception):
    """Status validation error"""
    pass

class CancellationError(Exception):
    """Cancellation operation error"""
    pass

class StatusManager:
    """
    Manages status transitions and validations.
    Ensures valid state changes and maintains system consistency.
    """

    # Define valid stream status transitions
    STREAM_TRANSITIONS = {
        StreamStatus.INACTIVE: [StreamStatus.ACTIVE],
        StreamStatus.ACTIVE: [
            StreamStatus.STOPPING, 
            StreamStatus.CANCELLING, 
            StreamStatus.ERROR,
            StreamStatus.DELIVERY_PENDING
        ],
        StreamStatus.STARTING: [
            StreamStatus.ACTIVE,
            StreamStatus.ERROR
        ],
        StreamStatus.STOPPING: [StreamStatus.INACTIVE, StreamStatus.ERROR],
        StreamStatus.CANCELLING: [StreamStatus.INACTIVE, StreamStatus.ERROR],
        StreamStatus.ERROR: [StreamStatus.INACTIVE],
        StreamStatus.DELETED: [],  # Terminal state
        # Delivery state transitions
        StreamStatus.DELIVERY_PENDING: [
            StreamStatus.DELIVERING,
            StreamStatus.ERROR,
            StreamStatus.CANCELLING
        ],
        StreamStatus.DELIVERING: [
            StreamStatus.ACTIVE,
            StreamStatus.DELIVERY_FAILED,
            StreamStatus.ERROR
        ],
        StreamStatus.DELIVERY_FAILED: [
            StreamStatus.DELIVERY_PENDING,
            StreamStatus.ERROR,
            StreamStatus.INACTIVE
        ]
    }

    # Define valid message status transitions
    MESSAGE_TRANSITIONS = {
        MessageStatus.QUALIFIED: [
            MessageStatus.QUEUED, 
            MessageStatus.CANCELLED, 
            MessageStatus.SKIPPED
        ],
        MessageStatus.QUEUED: [
            MessageStatus.TRANSFORMED, 
            MessageStatus.ERROR, 
            MessageStatus.CANCELLED
        ],
        MessageStatus.TRANSFORMED: [
            MessageStatus.ERROR,
            MessageStatus.DELIVERY_READY
        ],
        MessageStatus.ERROR: [
            MessageStatus.QUALIFIED,
            MessageStatus.RETRY_PENDING
        ],
        MessageStatus.CANCELLED: [MessageStatus.QUALIFIED],
        MessageStatus.SKIPPED: [MessageStatus.QUALIFIED],
        # Delivery state transitions
        MessageStatus.DELIVERY_READY: [
            MessageStatus.DELIVERING,
            MessageStatus.CANCELLED
        ],
        MessageStatus.DELIVERING: [
            MessageStatus.DELIVERED,
            MessageStatus.DELIVERY_FAILED,
            MessageStatus.RETRY_PENDING
        ],
        MessageStatus.DELIVERY_FAILED: [
            MessageStatus.RETRY_PENDING,
            MessageStatus.ERROR
        ],
        MessageStatus.RETRY_PENDING: [
            MessageStatus.DELIVERY_READY,
            MessageStatus.ERROR
        ],
        MessageStatus.DELIVERED: []  # Terminal state
    }

    # Status precedence for conflict resolution
    STATUS_PRECEDENCE = {
        StreamStatus.DELETED: 0,
        StreamStatus.ERROR: 1,
        StreamStatus.DELIVERY_FAILED: 2,
        StreamStatus.CANCELLING: 3,
        StreamStatus.STOPPING: 4,
        StreamStatus.DELIVERING: 5,
        StreamStatus.DELIVERY_PENDING: 6,
        StreamStatus.ACTIVE: 7,
        StreamStatus.STARTING: 8,
        StreamStatus.INACTIVE: 9
    }

    @classmethod
    def validate_stream_transition(cls, current: str, target: str, force: bool = False) -> bool:
        """
        Validate stream status transition
        
        Args:
            current: Current status
            target: Target status
            force: Allow forced transitions in emergency
            
        Returns:
            bool: Whether transition is valid
        """
        if not cls.is_valid_stream_status(current) or not cls.is_valid_stream_status(target):
            return False

        if force and target != StreamStatus.DELETED:
            return True

        return target in cls.STREAM_TRANSITIONS.get(current, [])

    @classmethod
    def validate_message_transition(cls, current: str, target: str) -> bool:
        """
        Validate message status transition
        
        Args:
            current: Current status
            target: Target status
            
        Returns:
            bool: Whether transition is valid
        """
        if not cls.is_valid_message_status(current) or not cls.is_valid_message_status(target):
            return False
            
        return target in cls.MESSAGE_TRANSITIONS.get(current, [])

    @classmethod
    def validate_delivery_transition(cls, current: str, target: str) -> bool:
        """
        Validate delivery status transition
        
        Args:
            current: Current delivery status
            target: Target delivery status
            
        Returns:
            bool: Whether transition is valid
        """
        delivery_states = cls.get_delivery_states()
        
        if current not in delivery_states or target not in delivery_states:
            return False
            
        # Use message transitions for delivery validation
        return target in cls.MESSAGE_TRANSITIONS.get(current, [])

    @classmethod
    def get_valid_stream_transitions(cls, current: str) -> List[str]:
        """Get list of valid stream transitions from current status"""
        if not cls.is_valid_stream_status(current):
            return []
        return cls.STREAM_TRANSITIONS.get(current, [])

    @classmethod
    def get_valid_message_transitions(cls, current: str) -> List[str]:
        """Get list of valid message transitions from current status"""
        if not cls.is_valid_message_status(current):
            return []
        return cls.MESSAGE_TRANSITIONS.get(current, [])

    @classmethod
    def get_delivery_states(cls) -> Set[str]:
        """Get all delivery-related states"""
        message_delivery_states = {
            MessageStatus.DELIVERY_READY,
            MessageStatus.DELIVERING,
            MessageStatus.DELIVERY_FAILED,
            MessageStatus.RETRY_PENDING,
            MessageStatus.DELIVERED
        }
        stream_delivery_states = {
            StreamStatus.DELIVERY_PENDING,
            StreamStatus.DELIVERING,
            StreamStatus.DELIVERY_FAILED
        }
        return message_delivery_states.union(stream_delivery_states)

    @classmethod
    def is_valid_stream_status(cls, status: str) -> bool:
        """Check if status is a valid stream status"""
        return hasattr(StreamStatus, status)

    @classmethod
    def is_valid_message_status(cls, status: str) -> bool:
        """Check if status is a valid message status"""
        return hasattr(MessageStatus, status)

    @classmethod
    def get_terminal_states(cls) -> Set[str]:
        """Get all terminal states that have no valid transitions"""
        return {status for status, transitions in cls.STREAM_TRANSITIONS.items() 
                if not transitions}

    @classmethod
    def get_cancellation_states(cls) -> Set[str]:
        """Get all states that indicate cancellation"""
        return {StreamStatus.CANCELLING, StreamStatus.STOPPING}

    @classmethod
    def get_delivery_terminal_states(cls) -> Set[str]:
        """Get terminal states for delivery process"""
        return {
            MessageStatus.DELIVERED,
            MessageStatus.ERROR,
            MessageStatus.CANCELLED
        }

    @classmethod
    def resolve_status_conflict(cls, status1: str, status2: str) -> str:
        """
        Resolve conflict between two statuses using precedence rules
        
        Args:
            status1: First status
            status2: Second status
            
        Returns:
            Resolved status
        
        Raises:
            StatusValidationError: If either status is invalid
        """
        if not (cls.is_valid_stream_status(status1) and cls.is_valid_stream_status(status2)):
            raise StatusValidationError("Invalid status in conflict resolution")

        return status1 if cls.STATUS_PRECEDENCE[status1] < cls.STATUS_PRECEDENCE[status2] else status2

    @staticmethod
    def get_status_display_with_reason(
        status: str,
        reason: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, str]:
        """
        Get display information for status with optional reason

        Args:
            status: Status to display
            reason: Optional reason for status
            timestamp: Optional timestamp of status change
            
        Returns:
            Display information dictionary
        """
        display = {
            # Core stream states
            StreamStatus.ACTIVE: {
                "label": "Active",
                "description": "Stream is processing messages",
                "color": "green"
            },
            StreamStatus.INACTIVE: {
                "label": "Inactive",
                "description": "Stream is not processing messages",
                "color": "gray"
            },
            StreamStatus.ERROR: {
                "label": "Error",
                "description": "Stream encountered an error",
                "color": "red"
            },
            StreamStatus.DELETED: {
                "label": "Deleted",
                "description": "Stream has been deleted",
                "color": "gray"
            },
            StreamStatus.STOPPING: {
                "label": "Stopping",
                "description": "Stream is gracefully stopping",
                "color": "orange"
            },
            StreamStatus.CANCELLING: {
                "label": "Cancelling",
                "description": "Stream is being cancelled",
                "color": "orange"
            },
            # Delivery states
            StreamStatus.DELIVERY_PENDING: {
                "label": "Delivery Pending",
                "description": "Stream has messages pending delivery",
                "color": "blue"
            },
            StreamStatus.DELIVERING: {
                "label": "Delivering",
                "description": "Stream is delivering messages",
                "color": "yellow"
            },
            StreamStatus.DELIVERY_FAILED: {
                "label": "Delivery Failed",
                "description": "Stream encountered delivery failures",
                "color": "red"
            },
            MessageStatus.DELIVERY_READY: {
                "label": "Ready for Delivery",
                "description": "Message is ready for delivery",
                "color": "blue"
            },
            MessageStatus.DELIVERING: {
                "label": "Delivering",
                "description": "Message delivery in progress",
                "color": "yellow"
            },
            MessageStatus.DELIVERY_FAILED: {
                "label": "Delivery Failed",
                "description": "Message delivery failed",
                "color": "red"
            },
            MessageStatus.RETRY_PENDING: {
                "label": "Retry Pending",
                "description": "Message waiting for retry",
                "color": "orange"
            },
            MessageStatus.DELIVERED: {
                "label": "Delivered",
                "description": "Message successfully delivered",
                "color": "green"
            }
        }.get(status, {
            "label": "Unknown",
            "description": "Unknown status",
            "color": "gray"
        })

        if reason:
            display["description"] = f"{display['description']} - {reason}"

        if timestamp:
            display["timestamp"] = timestamp.isoformat()

        return display

    @classmethod
    def can_retry(cls, status: str) -> bool:
        """Check if status allows retry operations"""
        retry_states = {
            MessageStatus.ERROR,
            MessageStatus.CANCELLED,
            MessageStatus.SKIPPED,
            MessageStatus.DELIVERY_FAILED,
            MessageStatus.RETRY_PENDING
        }
        return status in retry_states

    @classmethod
    def can_cancel(cls, status: str) -> bool:
        """Check if status can be cancelled"""
        cancellable_states = {
            MessageStatus.QUALIFIED,
            MessageStatus.QUEUED,
            MessageStatus.DELIVERY_READY,
            MessageStatus.RETRY_PENDING
        }
        return status in cancellable_states

    @classmethod
    def can_retry_delivery(cls, status: str) -> bool:
        """Check if delivery can be retried from current status"""
        retry_states = {
            MessageStatus.DELIVERY_FAILED,
            MessageStatus.RETRY_PENDING,
            MessageStatus.ERROR
        }
        return status in retry_states

    @classmethod
    def transition_status(
        cls,
        current: str,
        target: str,
        status_type: str = "stream",
        force: bool = False
    ) -> bool:
        """
        Safely transition between statuses
        
        Args:
            current: Current status
            target: Target status
            status_type: 'stream', 'message', or 'delivery'
            force: Allow forced transitions
            
        Returns:
            bool: Success of transition
            
        Raises:
            StatusTransitionError: If transition is invalid
        """
        if status_type == "stream":
            valid = cls.validate_stream_transition(current, target, force)
        elif status_type == "delivery":
            valid = cls.validate_delivery_transition(current, target)
        else:
            valid = cls.validate_message_transition(current, target)

        if not valid:
            raise StatusTransitionError(
                f"Invalid {status_type} status transition: {current} → {target}"
            )
            
        return True