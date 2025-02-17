# app/endpoints/base.py

import logging
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, Union, ClassVar, List
from flask import current_app
from abc import ABC, abstractmethod
import threading
import json
import uuid

from app.extensions import mongo
from app.utils.logging_utils import log_message_cycle
from app.endpoints.endpoint_types import EndpointConfig, EndpointMetrics, EndpointStatus

class EndpointError(Exception):
    """Base exception for endpoint operations"""
    pass

class ConfigurationError(EndpointError):
    """Configuration related errors"""
    pass

class DeliveryError(EndpointError):
    """Message delivery related errors"""
    pass

class BaseEndpoint(ABC):
    """
    Base class for all endpoints with common functionality.
    Supports both standalone operation and registry integration.
    """

    # Class-level configuration
    DEFAULT_TIMEOUT: ClassVar[int] = 30
    MAX_RETRIES: ClassVar[int] = 3
    RETRY_DELAY: ClassVar[int] = 5
    
    def __init__(self, config: Union[EndpointConfig, Dict[str, Any]], **kwargs):
        """
        Initialize base endpoint with configuration
        
        Args:
            config: EndpointConfig instance or configuration dictionary
            **kwargs: Additional endpoint-specific configuration parameters
        """
        try:
            # Handle both dictionary and EndpointConfig initialization
            if isinstance(config, dict):
                self.config = EndpointConfig.from_dict(config)
            else:
                self.config = config
                
            # Store additional configuration
            self.extra_config = kwargs
            
            # Honor existing status and determine if endpoint is already active
            self._active = (
                self.config.status == EndpointStatus.ACTIVE 
                if hasattr(self.config, 'status') 
                else False
            )

            # Database connections
            self.db = mongo.db
            self.messages_collection = self.db.messages
            self.logs_collection = self.db.logs
            
            if not self._active:
                # Full initialization for new endpoints
                self._init_endpoint()
            else:
                # Minimal initialization for already active endpoints
                self._init_active_endpoint()
                
            self.log_info(f"Initialized endpoint {self.config.uuid} (Active: {self._active})")
                
        except Exception as e:
            raise ConfigurationError(f"Error initializing endpoint: {str(e)}")
    
    def _init_endpoint(self):
        """Full initialization for new endpoints"""
        # Setup logging
        self.logger = logging.getLogger(f'{self.__class__.__name__}-{self.config.uuid}')
        self.logger.setLevel(logging.DEBUG)

        # Initialize metrics with thread safety
        self.metrics = EndpointMetrics()
        self._metrics_lock = threading.Lock()
        
        # State tracking
        self._error_count = 0
        self._last_error = None
        self._state_lock = threading.Lock()
        
        # Initialize registry integration if available
        self._init_registry()

    def _init_active_endpoint(self):
        """Minimal initialization for already active endpoints"""
        # Setup minimal logging
        self.logger = logging.getLogger(f'{self.__class__.__name__}-{self.config.uuid}')
        self.logger.setLevel(logging.DEBUG)

        # Initialize minimal metrics tracking
        self.metrics = EndpointMetrics()
        self._metrics_lock = threading.Lock()
        
        # Minimal state management
        self._error_count = 0
        self._last_error = None
        self._state_lock = threading.Lock()
        
        # Skip registry initialization as endpoint is already active
        self.log_info(f"Minimal initialization for active endpoint {self.config.uuid}")

    def _init_registry(self) -> None:
        """Initialize registry integration if available"""
        try:
            from app.endpoints.registry import endpoint_registry
            # Register with the registry if available
            endpoint_registry.register(self)
            self.log_info("Registered with endpoint registry")
        except ImportError:
            self.log_debug("Endpoint registry not available, running in standalone mode")
        except Exception as e:
            self.log_warning(f"Failed to register with endpoint registry: {str(e)}")

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
            # Support both new and legacy configuration formats
            if isinstance(config_data.get('config'), dict):
                # Legacy format
                endpoint_config = config_data.copy()
                extra_config = config_data['config']
            else:
                # Create EndpointConfig from dictionary
                endpoint_config = EndpointConfig.from_dict(config_data)
                
                # Extract non-config parameters
                extra_config = {k: v for k, v in config_data.items() 
                              if k not in EndpointConfig.__annotations__}
            
            # Validate configuration
            is_valid, error = (
                endpoint_config.validate() 
                if isinstance(endpoint_config, EndpointConfig)
                else cls._validate_legacy_config(endpoint_config)
            )
            
            if not is_valid:
                raise ValueError(f"Invalid endpoint configuration: {error}")

            # Create endpoint instance
            endpoint = cls(config=endpoint_config, **extra_config)
            
            # Store configuration
            endpoint._store_configuration()
            
            return endpoint

        except Exception as e:
            current_app.logger.error(f"Error creating endpoint: {str(e)}")
            raise ValueError(f"Failed to create endpoint: {str(e)}")

    @classmethod
    def _validate_legacy_config(cls, config: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate legacy configuration format"""
        required_fields = ['uuid', 'name', 'mode', 'endpoint_type', 'organization_uuid']
        
        # Check required fields
        for field in required_fields:
            if field not in config:
                return False, f"Missing required field: {field}"
                
        # Validate mode
        if config['mode'] not in ['source', 'destination']:
            return False, f"Invalid mode: {config['mode']}"
            
        return True, None

    def _store_configuration(self) -> None:
        """
        Store endpoint configuration in database
        
        Raises:
            Exception: If storage fails
        """
        try:
            # Prepare endpoint data
            endpoint_data = (
                self.config.to_dict() 
                if isinstance(self.config, EndpointConfig)
                else self.config
            )
            
            endpoint_data.update({
                "config": self.extra_config,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "status": EndpointStatus.INACTIVE,
                "metadata": getattr(self.config, 'metadata', {}) or {}
            })

            # Check if endpoint already exists
            existing = self.db.endpoints.find_one({"uuid": endpoint_data["uuid"]})
            if existing:
                # Update existing endpoint
                result = self.db.endpoints.update_one(
                    {"uuid": endpoint_data["uuid"]},
                    {"$set": endpoint_data}
                )
            else:
                # Insert new endpoint
                result = self.db.endpoints.insert_one(endpoint_data)

            if not (result.acknowledged if existing else result.inserted_id):
                raise Exception("Database operation not acknowledged")

            self.log_info(f"Endpoint configuration stored: {self.config.uuid}")
            
        except Exception as e:
            self.log_error(f"Error storing endpoint configuration: {str(e)}")
            raise

    def start(self) -> bool:
        """
        Start endpoint operations with state management
        
        Returns:
            Boolean indicating success
        """
        try:
            with self._state_lock:
                if self._active:
                    self.log_warning("Endpoint already active")
                    return True

                # Update database status first
                self.db.endpoints.update_one(
                    {"uuid": self.config.uuid},
                    {
                        "$set": {
                            "status": EndpointStatus.STARTING,
                            "updated_at": datetime.utcnow()
                        }
                    }
                )

                # Perform actual start operations
                success = self._start_impl()
                
                if success:
                    self._active = True
                    self.metrics.set('start_time', datetime.utcnow())
                    
                    # Update final status
                    self.db.endpoints.update_one(
                        {"uuid": self.config.uuid},
                        {
                            "$set": {
                                "status": EndpointStatus.ACTIVE,
                                "last_active": datetime.utcnow(),
                                "updated_at": datetime.utcnow(),
                                "error_count": 0,
                                "last_error": None
                            }
                        }
                    )
                    
                    self.log_info(f"Endpoint {self.config.uuid} started")
                else:
                    self._set_error_status("Failed to start endpoint")
                
                return success
                
        except Exception as e:
            self.log_error(f"Error starting endpoint: {str(e)}")
            self._set_error_status(str(e))
            return False

    def _start_impl(self) -> bool:
        """
        Implementation specific start operations.
        Override in subclasses.
        """
        return True
    
    # app/endpoints/base.py (continued)

    def stop(self) -> bool:
        """
        Stop endpoint operations with cleanup
        
        Returns:
            Boolean indicating success
        """
        try:
            with self._state_lock:
                if not self._active:
                    self.log_warning("Endpoint already inactive")
                    return True

                # Update database status first
                self.db.endpoints.update_one(
                    {"uuid": self.config.uuid},
                    {
                        "$set": {
                            "status": EndpointStatus.STOPPING,
                            "updated_at": datetime.utcnow()
                        }
                    }
                )

                # Perform actual stop operations
                success = self._stop_impl()
                
                if success:
                    self._active = False
                    # Update final status
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
                else:
                    self._set_error_status("Failed to stop endpoint")
                
                return success
                
        except Exception as e:
            self.log_error(f"Error stopping endpoint: {str(e)}")
            self._set_error_status(str(e))
            return False

    def _stop_impl(self) -> bool:
        """
        Implementation specific stop operations.
        Override in subclasses.
        """
        return True

    def get_status(self) -> Dict[str, Any]:
        """
        Get comprehensive endpoint status
        
        Returns:
            Dictionary containing status information
        """
        try:
            endpoint = self.db.endpoints.find_one({"uuid": self.config.uuid})
            if not endpoint:
                raise ValueError(f"Endpoint not found: {self.config.uuid}")

            with self._metrics_lock:
                status = {
                    "uuid": self.config.uuid,
                    "name": self.config.name,
                    "type": self.config.endpoint_type,
                    "mode": self.config.mode,
                    "status": endpoint.get("status", EndpointStatus.INACTIVE),
                    "active": self._active,
                    "last_active": endpoint.get("last_active"),
                    "messages_processed": self.metrics.get("messages_received", 0),
                    "errors": self.metrics.get("errors", 0),
                    "error_count": self._error_count,
                    "last_error": self._last_error,
                    "last_error_time": endpoint.get("last_error_time"),
                    "uptime": (
                        (datetime.utcnow() - self.metrics.get('start_time')).total_seconds()
                        if self.metrics.get('start_time')
                        else 0
                    ),
                    "organization_uuid": self.config.organization_uuid,
                    "configuration": {
                        k: v for k, v in self.extra_config.items()
                        if k not in ['credentials', 'secret', 'password', 'key']
                    }
                }
                
                # Add implementation-specific status
                impl_status = self._get_impl_status()
                if impl_status:
                    status.update(impl_status)
                    
                return status
                
        except Exception as e:
            self.log_error(f"Error getting endpoint status: {str(e)}")
            return {
                "uuid": self.config.uuid,
                "status": EndpointStatus.ERROR,
                "error": str(e)
            }

    def _get_impl_status(self) -> Optional[Dict[str, Any]]:
        """
        Get implementation specific status.
        Override in subclasses.
        """
        return None

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive endpoint metrics"""
        try:
            with self._metrics_lock:
                # Get base metrics
                metrics = self.metrics.get_all()
                
                # Get database metrics
                if self.config.mode == 'source':
                    total_messages = self.messages_collection.count_documents({
                        "endpoint_uuid": self.config.uuid,
                    })
                    metrics['messages_received'] = total_messages
                else:
                    total_messages = self.messages_collection.count_documents({
                        "endpoint_uuid": self.config.uuid,
                        "sent": True
                    })
                    metrics['messages_sent'] = total_messages

                # Get error metrics
                error_count = self.logs_collection.count_documents({
                    "endpoint_uuid": self.config.uuid,
                    "level": "ERROR"
                })
                metrics['errors'] = error_count

                # Get endpoint status
                endpoint = self.db.endpoints.find_one({"uuid": self.config.uuid})
                if endpoint:
                    metrics.update({
                        "status": endpoint.get("status", EndpointStatus.INACTIVE),
                        "last_active": endpoint.get("last_active"),
                        "endpoint_type": self.config.endpoint_type,
                        "mode": self.config.mode
                    })

                # Format datetime objects
                for key in ['start_time', 'last_active', 'last_message_time',
                        'last_upload_time', 'last_file_time', 'last_fetch_time']:
                    if metrics.get(key) and isinstance(metrics[key], datetime):
                        metrics[key] = metrics[key].isoformat()

                # Add implementation specific metrics
                impl_metrics = self._get_impl_metrics()
                if impl_metrics:
                    metrics.update(impl_metrics)

                return metrics

        except Exception as e:
            self.log_error(f"Error getting metrics: {str(e)}")
            return {
                "messages_received": 0,
                "messages_sent": 0,
                "errors": 0,
                "status": EndpointStatus.ERROR,
                "error": str(e)
            }

    def _get_impl_metrics(self) -> Optional[Dict[str, Any]]:
        """
        Get implementation specific metrics.
        Override in subclasses.
        """
        return None

    def handle_delivery(self, message_uuid: str, message_content: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Handle message delivery with comprehensive tracking
        
        Args:
            message_uuid: UUID of the message
            message_content: Message content to deliver
            
        Returns:
            Tuple[bool, str]: (success, error_message)
        """
        try:
            start_time = datetime.utcnow()
            
            # First validate message format
            is_valid, error = self.validate_message_format(message_content)
            if not is_valid:
                self.log_error(f"Invalid message format for {message_uuid}: {error}")
                return False, f"Invalid format: {error}"

            # Log delivery attempt
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="delivery_started",
                details={
                    "endpoint_uuid": self.config.uuid,
                    "endpoint_type": self.config.endpoint_type,
                    "message_size": len(json.dumps(message_content))
                },
                status="started"
            )

            # Check endpoint state
            if not self._active:
                error_msg = "Endpoint not active"
                self.log_error(f"Delivery failed - {error_msg}")
                return False, error_msg

            # Attempt delivery with retry
            for attempt in range(self.MAX_RETRIES):
                try:
                    success, error = self.deliver_message(
                        message_content=message_content,
                        metadata={
                            "message_uuid": message_uuid,
                            "attempt": attempt + 1,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                    
                    if success:
                        break
                    
                    if attempt < self.MAX_RETRIES - 1:
                        self.log_warning(
                            f"Delivery attempt {attempt + 1} failed, retrying: {error}"
                        )
                        time.sleep(self.RETRY_DELAY)
                    
                except Exception as attempt_error:
                    error = str(attempt_error)
                    if attempt < self.MAX_RETRIES - 1:
                        continue
                    success = False

            # Calculate duration
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            if success:
                # Log success
                self.log_message_cycle(
                    message_uuid=message_uuid,
                    event_type="delivery_completed",
                    details={
                        "endpoint_uuid": self.config.uuid,
                        "endpoint_type": self.config.endpoint_type,
                        "duration": duration
                    },
                    status="success"
                )
                
                # Update metrics
                with self._metrics_lock:
                    self.metrics.increment("messages_delivered")
                    self.metrics.set("last_delivery_time", datetime.utcnow())
                    
                return True, None
            else:
                # Log failure
                self.log_message_cycle(
                    message_uuid=message_uuid,
                    event_type="delivery_failed",
                    details={
                        "endpoint_uuid": self.config.uuid,
                        "endpoint_type": self.config.endpoint_type,
                        "error": error,
                        "attempts": self.MAX_RETRIES,
                        "duration": duration
                    },
                    status="error"
                )
                
                # Update metrics
                with self._metrics_lock:
                    self.metrics.increment("delivery_errors")
                    
                return False, error

        except Exception as e:
            error_msg = f"Delivery error: {str(e)}"
            self.log_error(error_msg)
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="delivery_error",
                details={
                    "endpoint_uuid": self.config.uuid,
                    "error": error_msg
                },
                status="error"
            )
            return False, error_msg

    def cleanup(self) -> None:
        """Perform comprehensive cleanup"""
        try:
            # Stop if running
            if self._active:
                self.stop()
            
            # Cleanup implementation specific resources
            self._cleanup_impl()
            
            # Clear metrics
            with self._metrics_lock:
                self.metrics.reset()
            
            # Reset state
            with self._state_lock:
                self._active = False
                self._error_count = 0
                self._last_error = None
            
            # Update database status
            self.db.endpoints.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": EndpointStatus.INACTIVE,
                        "updated_at": datetime.utcnow(),
                        "cleaned_up": True
                    }
                }
            )
            
            # Unregister from registry if available
            try:
                from app.endpoints.registry import endpoint_registry
                endpoint_registry.unregister(self.config.uuid)
            except ImportError:
                pass
            except Exception as e:
                self.log_warning(f"Error unregistering from registry: {str(e)}")
            
            self.log_info("Cleanup completed successfully")
            
        except Exception as e:
            self.log_error(f"Error during cleanup: {str(e)}")
            raise

    def _cleanup_impl(self) -> None:
        """
        Implementation specific cleanup.
        Override in subclasses.
        """
        pass

    def _set_error_status(self, error: str) -> None:
        """Update error status with tracking"""
        try:
            with self._state_lock:
                self._error_count += 1
                self._last_error = error
                
            self.db.endpoints.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": EndpointStatus.ERROR,
                        "updated_at": datetime.utcnow(),
                        "last_error": error,
                        "last_error_time": datetime.utcnow(),
                        "error_count": self._error_count
                    }
                }
            )
            
            # Update metrics
            with self._metrics_lock:
                self.metrics.increment("errors")
                
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
        """Store log message in database"""
        try:
            log_entry = {
                "endpoint_uuid": self.config.uuid,
                "organization_uuid": self.config.organization_uuid,
                "level": level,
                "message": message,
                "timestamp": datetime.utcnow(),
                "endpoint_type": self.config.endpoint_type,
                "endpoint_mode": self.config.mode
            }
            
            self.logs_collection.insert_one(log_entry)
        except Exception as e:
            self.logger.error(f"Error storing log: {str(e)}")

    def log_message_cycle(self, message_uuid: str, event_type: str, 
                     details: Dict[str, Any], status: str) -> None:
        """Log message processing cycle event"""
        try:
            log_message_cycle(
                message_uuid=message_uuid,
                event_type=event_type,
                details={
                    **details,
                    "endpoint_uuid": self.config.uuid,
                    "endpoint_type": self.config.endpoint_type,
                    "endpoint_mode": self.config.mode
                },
                status=status,
                organization_uuid=self.config.organization_uuid
            )
        except Exception as e:
            self.logger.error(f"Error logging message cycle: {str(e)}")

    @abstractmethod
    def deliver_message(self, message_content: Dict[str, Any], 
                       metadata: Optional[Dict] = None) -> Tuple[bool, Optional[str]]:
        """Deliver message - must be implemented by subclasses"""
        pass

    @abstractmethod
    def validate_message_format(self, message_content: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate message format - must be implemented by subclasses"""
        pass

    def test_connection(self) -> Tuple[bool, str]:
        """
        Test endpoint connectivity with timeout
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            # Check if endpoint is in valid state
            endpoint = self.db.endpoints.find_one({"uuid": self.config.uuid})
            if not endpoint:
                return False, "Endpoint configuration not found"

            # Check if endpoint is in error state
            if endpoint.get("status") == EndpointStatus.ERROR:
                return False, f"Endpoint in error state: {endpoint.get('last_error', 'Unknown error')}"

            # Perform implementation-specific connection test
            success, message = self._test_connection_impl()
            
            # Update status based on test result
            if success:
                self.db.endpoints.update_one(
                    {"uuid": self.config.uuid},
                    {
                        "$set": {
                            "last_connection_test": {
                                "success": True,
                                "timestamp": datetime.utcnow(),
                                "message": message
                            }
                        }
                    }
                )
            else:
                self.db.endpoints.update_one(
                    {"uuid": self.config.uuid},
                    {
                        "$set": {
                            "last_connection_test": {
                                "success": False,
                                "timestamp": datetime.utcnow(),
                                "error": message
                            }
                        }
                    }
                )

            return success, message

        except Exception as e:
            error_msg = f"Connection test failed: {str(e)}"
            self.log_error(error_msg)
            return False, error_msg

    def _test_connection_impl(self) -> Tuple[bool, str]:
        """
        Implementation specific connection test.
        Override in subclasses.
        """
        return True, "Base connection test successful"

    def process_destination_message(self, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Process a destination message for delivery
        
        Args:
            message: Destination message document
            
        Returns:
            Tuple[bool, str]: (success, error_message)
        """
        try:
            message_uuid = message.get('uuid', str(uuid.uuid4()))
            original_content = message.get('transformed_content')
            
            if not original_content:
                return False, "No transformed content found"

            # Add delivery metadata
            delivery_metadata = {
                "destination_message_uuid": message_uuid,
                "original_message_uuid": message.get('original_message_uuid'),
                "stream_uuid": message.get('stream_uuid'),
                "organization_uuid": message.get('organization_uuid'),
                "delivery_timestamp": datetime.utcnow().isoformat(),
                "endpoint_type": self.config.endpoint_type,
                "endpoint_mode": self.config.mode
            }

            # Attempt delivery
            success, error = self.handle_delivery(
                message_uuid=message_uuid,
                message_content=original_content
            )

            if success:
                # Update destination message status
                self.db.destination_messages.update_one(
                    {"uuid": message_uuid},
                    {
                        "$set": {
                            "destinations.$[dest].status": "COMPLETED",
                            "destinations.$[dest].completed_at": datetime.utcnow(),
                            "overall_status": "COMPLETED",
                            "updated_at": datetime.utcnow(),
                            "delivery_details": delivery_metadata
                        }
                    },
                    array_filters=[{"dest.endpoint_uuid": self.config.uuid}]
                )
                return True, None
            else:
                return False, error

        except Exception as e:
            error_msg = f"Error processing destination message: {str(e)}"
            self.log_error(error_msg)
            return False, error_msg

    def update_metrics(self, metric_name: str, value: Any) -> None:
        """
        Thread-safe metric update
        
        Args:
            metric_name: Name of metric to update
            value: New value or increment amount
        """
        try:
            with self._metrics_lock:
                if isinstance(value, (int, float)):
                    current = self.metrics.get(metric_name, 0)
                    self.metrics.set(metric_name, current + value)
                else:
                    self.metrics.set(metric_name, value)
        except Exception as e:
            self.log_error(f"Error updating metrics: {str(e)}")

    def reset_error_state(self) -> bool:
        """
        Reset endpoint error state and counters
        
        Returns:
            bool: Success status
        """
        try:
            with self._state_lock:
                self._error_count = 0
                self._last_error = None
            
            self.db.endpoints.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "error_count": 0,
                        "last_error": None,
                        "last_error_time": None,
                        "status": EndpointStatus.INACTIVE if not self._active else EndpointStatus.ACTIVE,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            self.log_info("Error state reset successfully")
            return True
            
        except Exception as e:
            self.log_error(f"Error resetting error state: {str(e)}")
            return False

    def validate_and_sanitize_config(self, config: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
        """
        Validate and sanitize endpoint configuration
        
        Args:
            config: Configuration to validate
            
        Returns:
            Tuple[bool, str, Dict]: (is_valid, error_message, sanitized_config)
        """
        try:
            # Create temporary EndpointConfig for validation
            temp_config = EndpointConfig.from_dict(config)
            
            # Validate configuration
            is_valid, error = temp_config.validate()
            if not is_valid:
                return False, error, None
            
            # Create sanitized config
            sanitized = temp_config.to_dict()
            
            # Remove sensitive fields
            sensitive_fields = {'password', 'secret', 'key', 'token', 'credentials'}
            for field in sensitive_fields:
                if field in sanitized:
                    sanitized[field] = '***'
            
            return True, None, sanitized
            
        except Exception as e:
            return False, str(e), None

    def get_supported_message_types(self) -> List[str]:
        """Get supported message types for this endpoint type"""
        try:
            endpoint_types = current_app.config.get('ENDPOINT_MAPPINGS', {})
            mode_mapping = endpoint_types.get(self.config.mode, {})
            endpoint_config = mode_mapping.get(self.config.endpoint_type, {})
            return endpoint_config.get('message_types', [])
        except Exception:
            # Fallback to EndpointConfig's valid message types
            return EndpointConfig.VALID_MESSAGE_TYPES.get(self.config.endpoint_type, [])

    def __str__(self) -> str:
        """String representation"""
        return (f"{self.__class__.__name__}("
                f"uuid={self.config.uuid}, "
                f"name={self.config.name}, "
                f"type={self.config.endpoint_type}, "
                f"mode={self.config.mode}, "
                f"active={self._active})")

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"{self.__class__.__name__}("
                f"uuid={self.config.uuid}, "
                f"name={self.config.name}, "
                f"type={self.config.endpoint_type}, "
                f"mode={self.config.mode}, "
                f"active={self._active}, "
                f"organization={self.config.organization_uuid}, "
                f"errors={self._error_count})")