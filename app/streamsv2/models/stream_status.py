from enum import Enum
from typing import List, Dict, Optional

class StreamStatus(str, Enum):
    """Status enumeration for streams"""
    INACTIVE = "INACTIVE"
    STARTING = "STARTING"
    ACTIVE = "ACTIVE"
    STOPPING = "STOPPING"
    ERROR = "ERROR"
    DELETED = "DELETED"

class StreamStatusManager:
    """Manages stream status transitions and validation"""
    
    # Valid status transitions
    VALID_TRANSITIONS = {
        StreamStatus.INACTIVE: [StreamStatus.STARTING, StreamStatus.DELETED],
        StreamStatus.STARTING: [StreamStatus.ACTIVE, StreamStatus.ERROR, StreamStatus.STOPPING],
        StreamStatus.ACTIVE: [StreamStatus.STOPPING, StreamStatus.ERROR],
        StreamStatus.STOPPING: [StreamStatus.INACTIVE, StreamStatus.ERROR],
        StreamStatus.ERROR: [StreamStatus.INACTIVE, StreamStatus.DELETED],
        StreamStatus.DELETED: []  # Terminal state
    }

    @classmethod
    def is_valid_transition(cls, current: StreamStatus, target: StreamStatus) -> bool:
        """Check if status transition is valid"""
        return target in cls.VALID_TRANSITIONS.get(current, [])

    @classmethod
    def get_valid_transitions(cls, current: StreamStatus) -> List[StreamStatus]:
        """Get list of valid transitions from current status"""
        return cls.VALID_TRANSITIONS.get(current, [])

    @classmethod
    def validate_status(cls, status: str) -> Optional[StreamStatus]:
        """Validate and convert string to StreamStatus"""
        try:
            return StreamStatus(status.upper())
        except ValueError:
            return None

    @staticmethod
    def get_status_display(status: StreamStatus) -> Dict[str, str]:
        """Get display information for status"""
        DISPLAY_INFO = {
            StreamStatus.INACTIVE: {
                "label": "Inactive",
                "description": "Stream is not processing messages",
                "color": "gray"
            },
            StreamStatus.STARTING: {
                "label": "Starting",
                "description": "Stream is initializing",
                "color": "blue"
            },
            StreamStatus.ACTIVE: {
                "label": "Active",
                "description": "Stream is processing messages",
                "color": "green"
            },
            StreamStatus.STOPPING: {
                "label": "Stopping",
                "description": "Stream is shutting down",
                "color": "orange"
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
            }
        }
        return DISPLAY_INFO.get(status, DISPLAY_INFO[StreamStatus.INACTIVE])