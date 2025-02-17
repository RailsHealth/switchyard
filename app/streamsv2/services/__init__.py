"""
Service layer components for stream processing operations.
"""

from .destination_handler import DestinationHandler
from .message_qualifier import MessageQualifier
from .monitoring import StreamMonitor

__all__ = [
    'DestinationHandler',
    'MessageQualifier',
    'StreamMonitor'
]

# Service exceptions
class ServiceError(Exception):
    """Base exception for service layer errors"""
    pass

class QualificationError(ServiceError):
    """Message qualification errors"""
    pass

class DeliveryError(ServiceError):
    """Message delivery errors"""
    pass

class MonitoringError(ServiceError):
    """Stream monitoring errors"""
    pass