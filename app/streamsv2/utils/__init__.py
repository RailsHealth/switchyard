"""
Utility functions and helper classes for stream processing.
"""

from .event_logger import EventLogger, EventTypes, EventSeverity
from .stream_logger import StreamLogger

__all__ = [
    'EventLogger',
    'EventTypes',
    'EventSeverity',
    'StreamLogger'
]

# Utility functions
def format_error(error: Exception) -> str:
    """Format exception for logging"""
    return f"{type(error).__name__}: {str(error)}"

def get_stream_identifier(stream_uuid: str) -> str:
    """Get formatted stream identifier for logging"""
    return f"Stream[{stream_uuid}]"

def format_message_identifier(message_uuid: str) -> str:
    """Get formatted message identifier for logging"""
    return f"Message[{message_uuid}]"