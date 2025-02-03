# app/streamsv2/services/destination_processor.py

from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime
import json
from flask import current_app
from app.extensions import mongo
from app.utils.logging_utils import log_message_cycle

logger = logging.getLogger(__name__)

class DestinationProcessor:
    """Handle destination queue message processing"""
    
    def __init__(self, organization_uuid: str):
        """Initialize processor"""
        self.organization_uuid = organization_uuid
        self.processing_metrics = {
            'messages_processed': 0,
            'messages_failed': 0,
            'last_processed': None
        }

    def process_message(self, message: Dict) -> Tuple[bool, Optional[str]]:
        """
        Process a message from destination queue
        
        Args:
            message: Message from destination queue
            
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            # Get destination endpoint config
            endpoint = mongo.db.endpoints.find_one({
                "uuid": message['endpoint_uuid'],
                "organization_uuid": self.organization_uuid
            })
            
            if not endpoint:
                return False, "Destination endpoint not found"

            # Process based on endpoint type
            success = False
            error = None
            
            try:
                if endpoint['endpoint_type'] in ['aws_s3', 'gcp_storage']:
                    success, error = self._process_cloud_storage(message, endpoint)
                elif endpoint['endpoint_type'] == 'sftp':
                    success, error = self._process_sftp(message, endpoint)
                else:
                    return False, f"Unsupported endpoint type: {endpoint['endpoint_type']}"

                # Update metrics
                if success:
                    self.processing_metrics['messages_processed'] += 1
                    self.processing_metrics['last_processed'] = datetime.utcnow()
                else:
                    self.processing_metrics['messages_failed'] += 1

                # Log the event
                log_message_cycle(
                    message_uuid=message['uuid'],
                    event_type="message_processed" if success else "message_failed",
                    details={
                        "destination_type": endpoint['endpoint_type'],
                        "endpoint_uuid": endpoint['uuid'],
                        "error": error if not success else None
                    },
                    status="success" if success else "error",
                    organization_uuid=self.organization_uuid
                )

                return success, error

            except Exception as e:
                self._handle_processing_error(message, e)
                return False, str(e)

        except Exception as e:
            logger.error(f"Error processing message {message.get('uuid')}: {str(e)}")
            return False, str(e)

    def _process_cloud_storage(self, message: Dict, endpoint: Dict) -> Tuple[bool, Optional[str]]:
        """Process cloud storage destination"""
        try:
            # For early version, just simulate processing
            logger.info(f"Processing cloud storage message {message['uuid']}")
            
            # In future versions, this would include:
            # 1. Format message for cloud storage
            # 2. Generate storage path
            # 3. Upload to cloud storage
            # 4. Handle encryption if configured
            # 5. Update message with storage location
            
            # Simulate successful processing
            return True, None

        except Exception as e:
            logger.error(f"Error processing cloud storage message: {str(e)}")
            return False, str(e)

    def _process_sftp(self, message: Dict, endpoint: Dict) -> Tuple[bool, Optional[str]]:
        """Process SFTP destination"""
        try:
            # For early version, just simulate processing
            logger.info(f"Processing SFTP message {message['uuid']}")
            
            # In future versions, this would include:
            # 1. Format message for SFTP
            # 2. Establish SFTP connection
            # 3. Upload file
            # 4. Verify upload
            # 5. Update message with file location
            
            # Simulate successful processing
            return True, None

        except Exception as e:
            logger.error(f"Error processing SFTP message: {str(e)}")
            return False, str(e)

    def _handle_processing_error(self, message: Dict, error: Exception) -> None:
        """Handle message processing error"""
        try:
            # Log the error
            log_message_cycle(
                message_uuid=message['uuid'],
                event_type="message_processing_failed",
                details={
                    "error": str(error),
                    "message_type": message.get('type')
                },
                status="error",
                organization_uuid=self.organization_uuid
            )

            # Increment error metrics
            self.processing_metrics['messages_failed'] += 1

        except Exception as e:
            logger.error(f"Error handling processing error: {str(e)}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get processor metrics"""
        return {
            "messages_processed": self.processing_metrics['messages_processed'],
            "messages_failed": self.processing_metrics['messages_failed'],
            "last_processed": self.processing_metrics['last_processed'].isoformat() 
                if self.processing_metrics['last_processed'] else None
        }

    def get_processing_status(self) -> Dict[str, Any]:
        """Get current processing status"""
        try:
            return {
                "organization_uuid": self.organization_uuid,
                "metrics": self.get_metrics(),
                "status": "active" if self.processing_metrics['last_processed'] else "inactive",
                "last_updated": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting processing status: {str(e)}")
            return {
                "organization_uuid": self.organization_uuid,
                "status": "error",
                "error": str(e)
            }