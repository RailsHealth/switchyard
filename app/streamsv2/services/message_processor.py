from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import logging
import json
import os
from flask import current_app
from app.extensions import mongo
from app.streamsv2.models.message_format import CloudStorageMessageFormat
from app.utils.logging_utils import log_message_cycle
from app.endpoints.cloud_storage_endpoint import CloudStorageEndpoint

logger = logging.getLogger(__name__)

class StreamMessageProcessor:
    """Processes messages for a stream"""

    def __init__(self, stream_config: Dict[str, Any]):
        """Initialize processor with stream configuration"""
        self.stream_config = stream_config
        self.organization_uuid = stream_config['organization_uuid']
        self.stream_uuid = stream_config['uuid']
        self.source_endpoint_uuid = stream_config['source_endpoint_uuid']
        self.destination_endpoint_uuid = stream_config['destination_endpoint_uuid']
        
        # Get endpoint details
        self.source_endpoint = self._get_endpoint(self.source_endpoint_uuid)
        self.destination_endpoint = self._get_endpoint(self.destination_endpoint_uuid)
        
        if not self.source_endpoint or not self.destination_endpoint:
            raise ValueError("Invalid endpoint configuration")
            
        # Set message types and formats
        self.source_type = self.source_endpoint['message_type']
        self.destination_type = f"{self.source_type} in JSON"
        
        # Validate message type compatibility
        self._validate_message_types()

    def _validate_message_types(self) -> None:
        """Validate message type compatibility between endpoints"""
        # Get supported types from config
        message_types = current_app.config['STREAMSV2_MESSAGE_TYPES']
        
        # Validate source type
        if self.source_type not in message_types['source']:
            raise ValueError(f"Unsupported source message type: {self.source_type}")
            
        # Validate destination type
        if self.destination_type not in message_types['destination']:
            raise ValueError(f"Unsupported destination message type: {self.destination_type}")
            
        # Validate endpoint compatibility
        source_config = message_types['source'][self.source_type]
        if self.source_endpoint['endpoint_type'] not in source_config['allowed_endpoints']:
            raise ValueError(f"Endpoint type {self.source_endpoint['endpoint_type']} not allowed for {self.source_type}")
            
        dest_config = message_types['destination'][self.destination_type]
        if self.destination_endpoint['endpoint_type'] not in dest_config['allowed_endpoints']:
            raise ValueError(f"Endpoint type {self.destination_endpoint['endpoint_type']} not allowed for {self.destination_type}")

    def _get_endpoint(self, endpoint_uuid: str) -> Optional[Dict]:
        """Get endpoint details from database"""
        return mongo.db.endpoints.find_one({
            "uuid": endpoint_uuid,
            "organization_uuid": self.organization_uuid,
            "deleted": {"$ne": True}
        })

    def get_message_filter(self) -> Dict[str, Any]:
        """Create filter for finding relevant messages"""
        return {
            "endpoint_uuid": self.source_endpoint_uuid,
            "organization_uuid": self.organization_uuid,
            "type": self.source_type,
            "processed_by_streams": {
                "$not": {"$elemMatch": {"stream_uuid": self.stream_uuid}}
            }
        }

    def get_pending_messages(self, limit: int = 100) -> List[Dict]:
        """Get pending messages for processing"""
        return list(mongo.db.messages.find(
            self.get_message_filter()
        ).limit(limit))

    async def process_message(self, message: Dict) -> Tuple[bool, Optional[str]]:
        """Process a single message"""
        try:
            # Format message for destination
            formatted_message = self._format_message(message)
            
            # Validate format
            is_valid, error = CloudStorageMessageFormat.validate_format(formatted_message)
            if not is_valid:
                raise ValueError(f"Invalid message format: {error}")

            # Send to destination
            success = await self._send_to_destination(formatted_message)
            if not success:
                raise Exception("Failed to send message to destination")

            # Mark message as processed by this stream
            self._mark_message_processed(message['_id'], success=True)
            
            # Log successful processing
            self._log_event(
                message_uuid=message['uuid'],
                event_type="message_processed",
                details={
                    "destination_endpoint": self.destination_endpoint_uuid,
                    "formatted_message_id": formatted_message['message_id'],
                    "source_type": self.source_type,
                    "destination_type": self.destination_type
                },
                status="success"
            )

            return True, None

        except Exception as e:
            error_msg = f"Error processing message: {str(e)}"
            logger.error(error_msg)
            
            # Mark message processing failed
            self._mark_message_processed(message['_id'], success=False, error=error_msg)
            
            # Log error
            self._log_event(
                message_uuid=message['uuid'],
                event_type="message_processing_error",
                details={"error": error_msg},
                status="error"
            )
            
            return False, error_msg

    def _format_message(self, message: Dict) -> Dict:
        """Format message for destination endpoint"""
        # Map source types to formatting functions
        format_handlers = {
            'HL7v2': CloudStorageMessageFormat.format_hl7v2_message,
            'CCDA': self._format_ccda_message,
            'X12': self._format_x12_message,
            'Clinical Notes': self._format_clinical_notes_message
        }
        
        handler = format_handlers.get(self.source_type)
        if not handler:
            raise ValueError(f"No formatter available for message type: {self.source_type}")
            
        return handler(message, self.stream_uuid)

    def _format_ccda_message(self, message: Dict, stream_uuid: str) -> Dict:
        """Format CCDA message for cloud storage"""
        return {
            "message_id": str(uuid.uuid4()),
            "message": {
                "content": message['message'],
                "type": "CCDA",
                "version": message.get('version', ''),
                "event": {
                    "type": "CCDA",
                    "trigger": message.get('event_type', '')
                }
            },
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "source_endpoint_uuid": message['endpoint_uuid'],
                "stream_uuid": stream_uuid,
                "original_message_uuid": message['uuid'],
                "filename": message.get('filename'),
                "content_type": "CCDA"
            }
        }

    def _format_x12_message(self, message: Dict, stream_uuid: str) -> Dict:
        """Format X12 message for cloud storage"""
        return {
            "message_id": str(uuid.uuid4()),
            "message": {
                "content": message['message'],
                "type": "X12",
                "version": message.get('version', ''),
                "event": {
                    "type": "X12",
                    "trigger": message.get('event_type', '')
                }
            },
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "source_endpoint_uuid": message['endpoint_uuid'],
                "stream_uuid": stream_uuid,
                "original_message_uuid": message['uuid'],
                "filename": message.get('filename'),
                "content_type": "X12"
            }
        }

    def _format_clinical_notes_message(self, message: Dict, stream_uuid: str) -> Dict:
        """Format Clinical Notes message for cloud storage"""
        return {
            "message_id": str(uuid.uuid4()),
            "message": {
                "content": message['message'],
                "type": "Clinical Notes",
                "version": message.get('version', ''),
                "event": {
                    "type": "Clinical Notes",
                    "trigger": message.get('event_type', '')
                }
            },
            "metadata": {
                "timestamp": datetime.utcnow().isoformat(),
                "source_endpoint_uuid": message['endpoint_uuid'],
                "stream_uuid": stream_uuid,
                "original_message_uuid": message['uuid'],
                "filename": message.get('filename'),
                "content_type": "Clinical Notes"
            }
        }

    async def _send_to_destination(self, formatted_message: Dict) -> bool:
        """Send formatted message to destination endpoint"""
        try:
            # Get destination endpoint instance
            endpoint_instance = CloudStorageEndpoint.create(self.destination_endpoint)
            if not endpoint_instance:
                raise ValueError("Failed to create destination endpoint instance")

            # Create temporary file with formatted message
            temp_path = f"/tmp/{formatted_message['message_id']}.json"
            with open(temp_path, 'w') as f:
                json.dump(formatted_message, f)

            # Upload to cloud storage
            remote_path = self._get_remote_path(formatted_message)
            result = await endpoint_instance.upload_file(
                temp_path,
                remote_path,
                metadata={
                    "stream_uuid": self.stream_uuid,
                    "message_type": formatted_message['message']['type'],
                    "source_type": self.source_type,
                    "destination_type": self.destination_type
                }
            )

            # Cleanup
            os.remove(temp_path)

            return result is not None

        except Exception as e:
            logger.error(f"Error sending to destination: {str(e)}")
            return False

    def _get_remote_path(self, formatted_message: Dict) -> str:
        """Generate remote path for cloud storage"""
        timestamp = datetime.fromisoformat(formatted_message['metadata']['timestamp'])
        return (
            f"{self.stream_uuid}/"
            f"{self.source_type}/"
            f"{timestamp.strftime('%Y/%m/%d')}/"
            f"{formatted_message['message_id']}.json"
        )

    def _mark_message_processed(self, message_id: str, success: bool, error: Optional[str] = None) -> None:
        """Mark message as processed by this stream"""
        update_data = {
            "stream_uuid": self.stream_uuid,
            "processed_at": datetime.utcnow(),
            "success": success,
            "source_type": self.source_type,
            "destination_type": self.destination_type
        }
        if error:
            update_data["error"] = error

        mongo.db.messages.update_one(
            {"_id": message_id},
            {
                "$push": {
                    "processed_by_streams": update_data
                }
            }
        )

    def _log_event(self, message_uuid: str, event_type: str, details: Dict, status: str) -> None:
        """Log stream processing event"""
        log_message_cycle(
            message_uuid=message_uuid,
            event_type=event_type,
            details={
                "stream_uuid": self.stream_uuid,
                "source_type": self.source_type,
                "destination_type": self.destination_type,
                **details
            },
            status=status,
            organization_uuid=self.organization_uuid
        )