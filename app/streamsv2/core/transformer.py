# app/streamsv2/core/transformer.py

from typing import Dict, Any, Optional
from datetime import datetime
import logging
from uuid import uuid4
from flask import current_app

from app.extensions import mongo
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.utils.stream_logger import StreamLogger

logger = logging.getLogger(__name__)

class TransformationError(Exception):
    """Custom exception for transformation errors"""
    pass

class MessageTransformer:
    """Handles message transformation to consistent JSON format"""

    def __init__(self, stream_config: StreamConfig):
        self.config = stream_config
        self.logger = StreamLogger(stream_config.uuid)

    def transform_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform message to standardized JSON format"""
        try:
            # Validate input message
            if not self._validate_input_message(message):
                raise TransformationError("Invalid input message format")

            # Get message type and validate
            message_type = message.get('type')  # Changed from message_type to type
            if not message_type:
                raise TransformationError("Message type not specified")

            # Transform based on message type
            transform_func = self._get_transform_function(message_type)
            if not transform_func:
                raise TransformationError(f"Unsupported message type: {message_type}")

            # Start transformation
            start_time = datetime.utcnow()
            
            # Create basic transformed structure even if parsing failed
            transformed = {
                "message_id": str(uuid4()),
                "message": {
                    "content": message.get('message'),
                    "type": message_type,
                    "version": message.get('version', '2.3'),  # Default to 2.3 if not specified
                    "parsed": message.get('parsed', False),
                    "parsing_error": message.get('parsing_error')
                },
                "metadata": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "original_message_uuid": message.get('uuid'),
                    "source_endpoint": message.get('endpoint_uuid'),
                    "organization_uuid": message.get('organization_uuid')
                }
            }

            # Log successful transformation
            self.logger.log_event(
                "message_transformed",
                {
                    "message_uuid": message.get('uuid'),
                    "message_type": message_type,
                    "transformation_time": (datetime.utcnow() - start_time).total_seconds()
                },
                "success"
            )

            return transformed

        except TransformationError as e:
            self._log_transformation_error(message.get('uuid'), str(e))
            return None
        except Exception as e:
            self._log_transformation_error(message.get('uuid'), f"Unexpected error: {str(e)}")
            return None

    def _get_transform_function(self, message_type: str):
        """Get appropriate transformation function for message type"""
        transform_map = {
            'HL7v2': self._transform_hl7v2,
            'CCDA': self._transform_ccda,
            'X12': self._transform_x12,
            'Clinical Notes': self._transform_clinical_notes
        }
        return transform_map.get(message_type)

    def _transform_hl7v2(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Transform HL7v2 message to JSON format"""
        try:
            content = message.get('message', '')
            if not content:
                raise TransformationError("Empty HL7 message")

            # Extract HL7 segments
            segments = content.split('\r')
            if not segments:
                raise TransformationError("Invalid HL7 message format")

            # Parse MSH segment
            msh = segments[0].split('|') if segments else []
            if len(msh) < 12:
                raise TransformationError("Invalid MSH segment")

            # Extract event type and trigger
            message_type = msh[8].split('^') if len(msh) > 8 else ['', '']
            event_type = message_type[0] if message_type else ''
            event_trigger = message_type[1] if len(message_type) > 1 else ''

            return {
                "message_id": str(uuid4()),
                "message": {
                    "content": content,
                    "type": "HL7v2",
                    "version": msh[11] if len(msh) > 11 else "",
                    "event": {
                        "type": event_type,
                        "trigger": event_trigger
                    }
                },
                "metadata": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "sending_facility": msh[4] if len(msh) > 4 else "",
                    "receiving_facility": msh[6] if len(msh) > 6 else "",
                    "message_control_id": msh[9] if len(msh) > 9 else "",
                }
            }

        except TransformationError:
            raise
        except Exception as e:
            raise TransformationError(f"Error transforming HL7v2: {str(e)}")

    def _log_transformation_error(self, message_uuid: str, error: str) -> None:
        """Log transformation error"""
        self.logger.log_event(
            "transformation_failed",
            {
                "message_uuid": message_uuid,
                "error": error,
                "timestamp": datetime.utcnow().isoformat()
            },
            "error"
        )

    async def _transform_ccda(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Transform CCDA to JSON format"""
        try:
            content = message.get('message', '')
            if not content:
                raise TransformationError("Empty CCDA message")

            # Extract basic CCDA metadata
            # This would be enhanced with actual CCDA parsing
            return {
                "message_id": str(uuid4()),
                "message": {
                    "content": content,
                    "type": "CCDA",
                    "version": "2.1",  # Would be extracted from document
                    "event": {
                        "type": "CCDA",
                        "trigger": "Document"
                    }
                },
                "metadata": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "sending_facility": message.get('source', ''),
                    "receiving_facility": message.get('destination', ''),
                    "message_control_id": message.get('document_id', ''),
                    "document_type": message.get('document_type', '')
                }
            }

        except Exception as e:
            raise TransformationError(f"Error transforming CCDA: {str(e)}")

    async def _transform_x12(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Transform X12 to JSON format"""
        try:
            content = message.get('message', '')
            if not content:
                raise TransformationError("Empty X12 message")

            # Extract X12 envelope information
            # This would be enhanced with actual X12 parsing
            return {
                "message_id": str(uuid4()),
                "message": {
                    "content": content,
                    "type": "X12",
                    "version": "5010",  # Would be extracted from document
                    "event": {
                        "type": "X12",
                        "trigger": "Claim"  # Would be determined from content
                    }
                },
                "metadata": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "sending_facility": message.get('sender_id', ''),
                    "receiving_facility": message.get('receiver_id', ''),
                    "message_control_id": message.get('control_number', ''),
                    "transaction_type": message.get('transaction_type', '')
                }
            }

        except Exception as e:
            raise TransformationError(f"Error transforming X12: {str(e)}")

    async def _transform_clinical_notes(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Clinical Notes to JSON format"""
        try:
            content = message.get('message', '')
            if not content:
                raise TransformationError("Empty Clinical Notes message")

            return {
                "message_id": str(uuid4()),
                "message": {
                    "content": content,
                    "type": "Clinical Notes",
                    "version": "1.0",
                    "event": {
                        "type": "Clinical Note",
                        "trigger": "Document"
                    }
                },
                "metadata": {
                    "timestamp": datetime.utcnow().isoformat(),
                    "sending_facility": message.get('facility', ''),
                    "receiving_facility": message.get('destination', ''),
                    "message_control_id": message.get('document_id', ''),
                    "document_type": message.get('note_type', '')
                }
            }

        except Exception as e:
            raise TransformationError(f"Error transforming Clinical Notes: {str(e)}")

    def _validate_input_message(self, message: Dict[str, Any]) -> bool:
        """Validate minimum message structure"""
        try:
            # Only check essential fields
            required_fields = ['uuid', 'message', 'type']  # Changed from message_type to type
            if not all(field in message for field in required_fields):
                return False
                
            # Ensure message content exists and isn't empty
            if not message.get('message'):
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating message: {str(e)}")
            return False
        
    async def _log_transformation_error(self, message_uuid: str, error: str) -> None:
        """Log transformation error"""
        await self.logger.log_event(
            "transformation_failed",
            {
                "message_uuid": message_uuid,
                "error": error,
                "timestamp": datetime.utcnow().isoformat()
            },
            "error"
        )