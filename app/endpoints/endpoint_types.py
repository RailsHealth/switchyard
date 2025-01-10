# app/endpoints/endpoint_types.py

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
import threading
import uuid

class EndpointMetrics:
    """Base metrics structure for all endpoint types"""
    
    def __init__(self):
        self._metrics = {
            # Common metrics for all endpoints
            'messages_received': 0,
            'messages_sent': 0,
            'errors': 0,
            'connection_errors': 0,
            'start_time': None,
            'last_active': None,
            
            # MLLP specific metrics
            'messages_processed': 0,
            'messages_failed': 0,
            'parse_errors': 0,
            'protocol_errors': 0,
            'last_message_time': None,
            
            # CloudStorage specific metrics
            'files_uploaded': 0,
            'bytes_uploaded': 0,
            'upload_errors': 0,
            'last_upload_time': None,
            
            # SFTP specific metrics
            'files_processed': 0,
            'files_failed': 0,
            'files_skipped': 0,
            'invalid_extensions': 0,
            'bytes_transferred': 0,
            'processing_errors': 0,
            'last_file_time': None,
            
            # HTTPS/FHIR specific metrics
            'resources_fetched': 0,
            'fetch_errors': 0,
            'successful_connections': 0,
            'failed_connections': 0,
            'last_fetch_time': None
        }
        self._lock = threading.Lock()

    def get(self, key: str, default: Any = None) -> Any:
        """Get a metric value safely"""
        with self._lock:
            return self._metrics.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a metric value safely"""
        with self._lock:
            self._metrics[key] = value

    def increment(self, key: str, value: int = 1) -> None:
        """Increment a metric value safely"""
        with self._lock:
            if key in self._metrics and isinstance(self._metrics[key], (int, float)):
                self._metrics[key] += value

    def update(self, metrics: Dict[str, Any]) -> None:
        """Update multiple metrics safely"""
        with self._lock:
            self._metrics.update(metrics)

    def get_all(self) -> Dict[str, Any]:
        """Get all metrics safely"""
        with self._lock:
            return self._metrics.copy()

    def reset(self) -> None:
        """Reset all metrics to default values"""
        with self._lock:
            self.__init__()  # Reinitialize defaults

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary format"""
        return self.get_all()


@dataclass
class EndpointConfig:
    """Configuration container for all endpoint types"""
    uuid: str
    name: str
    mode: str
    message_type: str
    endpoint_type: str
    organization_uuid: str
    active: bool = False
    is_test: bool = False
    status: str = "inactive"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_active: Optional[datetime] = None
    deleted: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    deidentification_requested: bool = False
    purging_requested: bool = False

    # Valid message types per endpoint type
    VALID_MESSAGE_TYPES = {
        # Source endpoint types
        'mllp': ['HL7v2'],
        'sftp': ['CCDA', 'X12', 'Clinical Notes'],
        'https': ['FHIR'],
        
        # Destination endpoint types
        'aws_s3': ['HL7v2 in JSON', 'CCDA in JSON', 'X12 in JSON', 'Clinical Notes in JSON'],
        'gcp_storage': ['HL7v2 in JSON', 'CCDA in JSON', 'X12 in JSON', 'Clinical Notes in JSON']
    }

    # Valid modes
    VALID_MODES = ['source', 'destination']

    # Valid endpoint types
    VALID_ENDPOINT_TYPES = ['mllp', 'sftp', 'https', 'aws_s3', 'gcp_storage']

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EndpointConfig':
        """Create config from dictionary"""
        config_data = {
            'uuid': data.get('uuid', str(uuid.uuid4())),
            'name': data['name'],
            'mode': data['mode'],
            'message_type': data['message_type'],
            'endpoint_type': data['endpoint_type'],
            'organization_uuid': data['organization_uuid'],
            'active': data.get('active', False),
            'is_test': data.get('is_test', False),
            'status': data.get('status', 'inactive'),
            'created_at': data.get('created_at'),
            'updated_at': data.get('updated_at'),
            'last_active': data.get('last_active'),
            'deleted': data.get('deleted', False),
            'metadata': data.get('metadata', {}),
            'deidentification_requested': data.get('deidentification_requested', False),
            'purging_requested': data.get('purging_requested', False)
        }
        return cls(**config_data)

    def validate(self) -> tuple[bool, Optional[str]]:
        """Validate configuration"""
        try:
            # Check required fields
            required_fields = ['uuid', 'name', 'mode', 'endpoint_type', 'organization_uuid']
            for field in required_fields:
                if not getattr(self, field):
                    return False, f"Missing required field: {field}"

            # Validate mode
            if self.mode not in self.VALID_MODES:
                return False, f"Invalid mode: {self.mode}"

            # Validate endpoint type
            if self.endpoint_type not in self.VALID_ENDPOINT_TYPES:
                return False, f"Invalid endpoint type: {self.endpoint_type}"

            # Validate message type based on mode and endpoint type
            if self.endpoint_type in ['aws_s3', 'gcp_storage']:
                if self.mode != 'destination':
                    return False, f"Cloud storage endpoints must be in destination mode"
                
                if not self.message_type.endswith('in JSON'):
                    return False, f"Cloud storage endpoints require JSON format message types"

            valid_types = self.VALID_MESSAGE_TYPES.get(self.endpoint_type, [])
            if self.message_type not in valid_types:
                return False, (f"Invalid message type {self.message_type} for "
                             f"endpoint type {self.endpoint_type}")

            # Validate metadata format
            if not isinstance(self.metadata, dict):
                return False, "Metadata must be a dictionary"

            return True, None

        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        return {
            'uuid': self.uuid,
            'name': self.name,
            'mode': self.mode,
            'message_type': self.message_type,
            'endpoint_type': self.endpoint_type,
            'organization_uuid': self.organization_uuid,
            'active': self.active,
            'is_test': self.is_test,
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'last_active': self.last_active,
            'deleted': self.deleted,
            'metadata': self.metadata,
            'deidentification_requested': self.deidentification_requested,
            'purging_requested': self.purging_requested
        }


class EndpointStatus:
    """Status constants for endpoints"""
    # Status constants
    INACTIVE = "INACTIVE"
    STARTING = "STARTING"
    ACTIVE = "ACTIVE"
    STOPPING = "STOPPING"
    ERROR = "ERROR"
    DELETED = "DELETED"

    # Valid status transitions
    VALID_TRANSITIONS = {
        INACTIVE: [STARTING, DELETED],
        STARTING: [ACTIVE, ERROR, STOPPING],
        ACTIVE: [STOPPING, ERROR],
        STOPPING: [INACTIVE, ERROR],
        ERROR: [INACTIVE, DELETED],
        DELETED: []  # Terminal state
    }

    @classmethod
    def is_valid_status(cls, status: str) -> bool:
        """Check if status is valid"""
        return status in cls.VALID_TRANSITIONS

    @classmethod
    def is_valid_transition(cls, current: str, target: str) -> bool:
        """Check if status transition is valid"""
        if not (cls.is_valid_status(current) and cls.is_valid_status(target)):
            return False
        return target in cls.VALID_TRANSITIONS.get(current, [])