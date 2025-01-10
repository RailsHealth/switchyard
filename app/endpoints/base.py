# app/endpoints/base.py

import logging
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
from flask import current_app
from abc import ABC, abstractmethod
from app.extensions import mongo
from app.utils.logging_utils import log_message_cycle
from app.endpoints.endpoint_types import EndpointConfig, EndpointMetrics, EndpointStatus

class BaseEndpoint(ABC):
    """Base class for all endpoints with common functionality"""

    def __init__(self, config: EndpointConfig, **kwargs):
        """
        Initialize base endpoint with configuration
        
        Args:
            config: EndpointConfig instance containing core configuration
            **kwargs: Additional endpoint-specific configuration parameters
        """
        # Store configuration
        self.config = config
        self.extra_config = kwargs
        
        # Database connections
        self.db = mongo.db
        self.messages_collection = self.db.messages
        self.logs_collection = self.db.logs
        
        # Setup logging
        self.logger = logging.getLogger(f'{self.__class__.__name__}-{config.uuid}')
        self.logger.setLevel(logging.DEBUG)

        # Initialize metrics
        self.metrics = EndpointMetrics()

    @classmethod
    def create(cls, config_data: Dict[str, Any]) -> 'BaseEndpoint':
        """
        Factory method to create new endpoint instance
        
        Args:
            config_data: Complete configuration dictionary
            
        Returns:
            New endpoint instance
            
        Raises:
            ValueError: If configuration is invalid
        """
        try:
            # Create EndpointConfig from dictionary
            config = EndpointConfig.from_dict(config_data)
            
            # Validate configuration
            is_valid, error = config.validate()
            if not is_valid:
                raise ValueError(f"Invalid endpoint configuration: {error}")
            
            # Extract non-config parameters
            extra_config = {k: v for k, v in config_data.items() 
                          if k not in EndpointConfig.__annotations__}

            # Create endpoint instance
            endpoint = cls(config=config, **extra_config)
            
            # Store configuration
            endpoint._store_configuration()
            
            return endpoint

        except Exception as e:
            current_app.logger.error(f"Error creating endpoint: {str(e)}")
            raise ValueError(f"Failed to create endpoint: {str(e)}")

    def _store_configuration(self) -> None:
        """
        Store endpoint configuration in database
        
        Raises:
            Exception: If storage fails
        """
        endpoint_data = self.config.to_dict()
        endpoint_data.update({
            "config": self.extra_config,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "status": EndpointStatus.INACTIVE,
            "metadata": self.config.metadata or {}
        })

        try:
            result = self.db.endpoints.insert_one(endpoint_data)
            if not result.acknowledged:
                raise Exception("Failed to store endpoint configuration")
            self.log_info(f"Endpoint configuration stored: {self.config.uuid}")
        except Exception as e:
            self.log_error(f"Error storing endpoint configuration: {str(e)}")
            raise

    def start(self) -> bool:
        """
        Start endpoint operations
        
        Returns:
            Boolean indicating success
        """
        try:
            # Update database status
            self.db.endpoints.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": EndpointStatus.ACTIVE,
                        "last_active": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            # Update metrics
            self.metrics.set('start_time', datetime.utcnow())
            self.log_info(f"Endpoint {self.config.uuid} started")
            return True
            
        except Exception as e:
            self.log_error(f"Error starting endpoint: {str(e)}")
            self._set_error_status()
            return False

    def stop(self) -> bool:
        """
        Stop endpoint operations
        
        Returns:
            Boolean indicating success
        """
        try:
            self.db.endpoints.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": EndpointStatus.INACTIVE,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            self.log_info(f"Endpoint {self.config.uuid} stopped")
            return True
            
        except Exception as e:
            self.log_error(f"Error stopping endpoint: {str(e)}")
            self._set_error_status()
            return False

    def get_status(self) -> Dict[str, Any]:
        """
        Get endpoint status and basic metrics
        
        Returns:
            Dictionary containing status information
        """
        try:
            endpoint = self.db.endpoints.find_one({"uuid": self.config.uuid})
            if not endpoint:
                raise ValueError(f"Endpoint not found: {self.config.uuid}")

            return {
                "uuid": self.config.uuid,
                "name": self.config.name,
                "status": endpoint.get("status", EndpointStatus.INACTIVE),
                "last_active": endpoint.get("last_active"),
                "messages_processed": self.metrics.get("messages_received"),
                "errors": self.metrics.get("errors"),
                "uptime": (
                    datetime.utcnow() - self.metrics.get('start_time')
                    if self.metrics.get('start_time')
                    else None
                )
            }
        except Exception as e:
            self.log_error(f"Error getting endpoint status: {str(e)}")
            return {"error": str(e)}

    def get_metrics(self) -> Dict[str, Any]:
        """Get endpoint metrics with actual totals from database"""
        try:
            # Get basic metrics from metrics store
            metrics = self.metrics.get_all()
            
            # Get total message counts from database
            if self.config.mode == 'source':
                total_messages = self.messages_collection.count_documents({
                    "endpoint_uuid": self.config.uuid,
                })
                metrics['messages_received'] = total_messages
            else:
                # For destination endpoints
                total_messages = self.messages_collection.count_documents({
                    "endpoint_uuid": self.config.uuid,
                    "sent": True
                })
                metrics['messages_sent'] = total_messages

            # Get total error count
            error_count = self.logs_collection.count_documents({
                "endpoint_uuid": self.config.uuid,
                "level": "ERROR"
            })
            metrics['errors'] = error_count

            # Get endpoint status info
            endpoint = self.db.endpoints.find_one({"uuid": self.config.uuid})
            if endpoint:
                metrics.update({
                    "status": endpoint.get("status", EndpointStatus.INACTIVE),
                    "last_active": endpoint.get("last_active"),
                    "endpoint_type": self.config.endpoint_type,
                    "mode": self.config.mode
                })

            # Format datetime objects
            for key in ['start_time', 'last_active', 'last_message_time', 'last_upload_time', 
                    'last_file_time', 'last_fetch_time']:
                if metrics.get(key) and isinstance(metrics[key], datetime):
                    metrics[key] = metrics[key].isoformat()

            return metrics

        except Exception as e:
            self.log_error(f"Error getting metrics: {str(e)}")
            # Return safe defaults if something goes wrong
            return {
                "messages_received": 0,
                "messages_sent": 0,
                "errors": 0,
                "status": EndpointStatus.ERROR,
                "error": str(e)
            }

    def test_connection(self) -> Tuple[bool, str]:
        """
        Test endpoint connectivity
        
        Returns:
            Tuple of (success boolean, message string)
        """
        return True, "Base test successful"

    @abstractmethod
    def cleanup(self) -> None:
        """Perform cleanup operations"""
        pass

    def _set_error_status(self) -> None:
        """Update endpoint status to error"""
        try:
            self.db.endpoints.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": EndpointStatus.ERROR,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
        except Exception as e:
            self.log_error(f"Error updating error status: {str(e)}")

    # Logging methods
    def log_info(self, message: str) -> None:
        """Log info level message"""
        self.logger.info(message)
        self._store_log("INFO", message)

    def log_error(self, message: str) -> None:
        """Log error level message"""
        self.logger.error(message)
        self._store_log("ERROR", message)

    def log_warning(self, message: str) -> None:
        """Log warning level message"""
        self.logger.warning(message)
        self._store_log("WARNING", message)

    def log_debug(self, message: str) -> None:
        """Log debug level message"""
        self.logger.debug(message)
        self._store_log("DEBUG", message)

    def _store_log(self, level: str, message: str) -> None:
        """
        Store log message in database
        
        Args:
            level: Log level
            message: Log message
        """
        log_entry = {
            "endpoint_uuid": self.config.uuid,
            "organization_uuid": self.config.organization_uuid,
            "level": level,
            "message": message,
            "timestamp": datetime.utcnow()
        }
        
        try:
            self.logs_collection.insert_one(log_entry)
        except Exception as e:
            self.logger.error(f"Error storing log: {str(e)}")

    def log_message_cycle(self, message_uuid: str, event_type: str, 
                     details: Dict[str, Any], status: str) -> None:
        """Log message processing cycle event"""
        log_message_cycle(
            message_uuid=message_uuid,
            event_type=event_type,
            details=details,
            status=status,
            organization_uuid=self.config.organization_uuid  # Add this line
        )