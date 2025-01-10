from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import uuid
from typing import Dict, Any, Tuple, Optional, List

@dataclass
class CloudStorageMessageFormat:
    """Message format handler for cloud storage destinations"""
    
    REQUIRED_FIELDS = {
        'message_id': str,
        'message': {
            'content': str,
            'type': str,
            'version': str,
            'event': {
                'type': str,
                'trigger': str
            }
        },
        'metadata': {
            'timestamp': str,
            'sending_facility': str,
            'receiving_facility': str,
            'message_control_id': str
        }
    }

    @staticmethod
    def extract_hl7_segments(message: str) -> Dict[str, str]:
        """Extract key segments from HL7 message"""
        try:
            segments = message.split('\r')
            msh = segments[0].split('|') if segments else []
            
            message_type = msh[8].split('^') if len(msh) > 8 else ['', '']
            
            return {
                'version': msh[11] if len(msh) > 11 else "",
                'sending_facility': msh[4] if len(msh) > 4 else "",
                'receiving_facility': msh[6] if len(msh) > 6 else "",
                'message_control_id': msh[9] if len(msh) > 9 else "",
                'event_type': message_type[0],
                'event_trigger': message_type[1] if len(message_type) > 1 else ""
            }
        except Exception:
            return {
                'version': "",
                'sending_facility': "",
                'receiving_facility': "",
                'message_control_id': "",
                'event_type': "",
                'event_trigger': ""
            }

    @classmethod
    def format_hl7v2_message(cls, message: Dict[str, Any], stream_uuid: str) -> Dict[str, Any]:
        """Format HL7v2 message for cloud storage"""
        message_content = message.get('message', '')
        segments = cls.extract_hl7_segments(message_content)

        return {
            "message_id": str(uuid.uuid4()),
            "message": {
                "content": message_content,
                "type": "HL7v2",
                "version": segments['version'],
                "event": {
                    "type": segments['event_type'],
                    "trigger": segments['event_trigger']
                }
            },
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "sending_facility": segments['sending_facility'],
                "receiving_facility": segments['receiving_facility'],
                "message_control_id": segments['message_control_id'],
                "source_endpoint_uuid": message.get('endpoint_uuid'),
                "stream_uuid": stream_uuid,
                "original_message_uuid": message.get('uuid')
            }
        }

    @classmethod
    def validate_format(cls, formatted_message: Dict) -> Tuple[bool, Optional[str]]:
        """Validate message format"""
        try:
            # Validate basic structure
            if not all(k in formatted_message for k in ['message_id', 'message', 'metadata']):
                return False, "Missing required top-level fields"

            # Validate message section
            message = formatted_message['message']
            if not all(k in message for k in ['content', 'type', 'version', 'event']):
                return False, "Missing required message fields"

            # Validate event section
            event = message['event']
            if not all(k in event for k in ['type', 'trigger']):
                return False, "Missing required event fields"

            # Validate metadata section
            metadata = formatted_message['metadata']
            required_metadata = [
                'timestamp', 
                'sending_facility', 
                'receiving_facility', 
                'message_control_id',
                'source_endpoint_uuid',
                'stream_uuid',
                'original_message_uuid'
            ]
            if not all(k in metadata for k in required_metadata):
                return False, "Missing required metadata fields"

            return True, None

        except Exception as e:
            return False, f"Validation error: {str(e)}"