# app/endpoints/registry.py

import threading
import logging
import json
from typing import Dict, Optional, List, Any, Tuple
from datetime import datetime
from flask import current_app

from app.endpoints.base import BaseEndpoint
from app.endpoints.endpoint_types import EndpointConfig, EndpointStatus
from app.extensions import mongo

logger = logging.getLogger(__name__)

class RegistryError(Exception):
    """Base exception for registry operations"""
    pass

class EndpointInitializationError(RegistryError):
    """Error during endpoint initialization"""
    pass

class EndpointNotFoundError(RegistryError):
    """Endpoint not found in registry"""
    pass

class EndpointStateError(RegistryError):
    """Invalid endpoint state transition"""
    pass

class EndpointRegistry:
    def __init__(self):
        # Keep existing initialization with legacy support
        self._endpoints: Dict[str, BaseEndpoint] = {}
        self._lock = threading.Lock()
        self._status_timestamps: Dict[str, datetime] = {}
        self._error_counts: Dict[str, int] = {}
        self._max_errors = 3
        self._legacy_mode = True  # Preserve legacy support
        self._recovery_attempts: Dict[str, int] = {}
        self._max_recovery_attempts = 5
        
        self._endpoint_types = {
            'mllp': 'app.endpoints.mllp_endpoint.MLLPEndpoint',
            'aws_s3': 'app.endpoints.cloud_storage_endpoint.CloudStorageEndpoint',
            'gcp_storage': 'app.endpoints.cloud_storage_endpoint.CloudStorageEndpoint',
            'sftp': 'app.endpoints.sftp_endpoint.SFTPEndpoint',
            'https': 'app.endpoints.https_endpoint.HTTPSEndpoint'
        }
    
    def register(self, endpoint: BaseEndpoint) -> bool:
        """
        Register endpoint instance with backward compatibility
        
        Args:
            endpoint: Endpoint instance to register
            
        Returns:
            bool: Success status
            
        Raises:
            RegistryError: If registration fails
        """
        try:
            endpoint_uuid = endpoint.config.uuid
            
            with self._lock:
                # Check if already registered
                if endpoint_uuid in self._endpoints:
                    # Support reregistration for legacy endpoints
                    if self._legacy_mode:
                        self._endpoints[endpoint_uuid] = endpoint
                        self._status_timestamps[endpoint_uuid] = datetime.utcnow()
                        self._error_counts[endpoint_uuid] = 0
                        self._recovery_attempts[endpoint_uuid] = 0
                        logger.info(f"Re-registered legacy endpoint: {endpoint_uuid}")
                        return True
                    raise RegistryError(f"Endpoint {endpoint_uuid} already registered")
                
                # Store endpoint with metadata
                self._endpoints[endpoint_uuid] = endpoint
                self._status_timestamps[endpoint_uuid] = datetime.utcnow()
                self._error_counts[endpoint_uuid] = 0
                self._recovery_attempts[endpoint_uuid] = 0
                
                # Update database status
                try:
                    mongo.db.endpoints.update_one(
                        {"uuid": endpoint_uuid},
                        {
                            "$set": {
                                "registered_at": datetime.utcnow(),
                                "last_status_update": datetime.utcnow(),
                                "registry_status": "registered"
                            }
                        }
                    )
                except Exception as db_error:
                    logger.error(f"Database update failed for {endpoint_uuid}: {str(db_error)}")
                
                logger.info(f"Registered endpoint: {endpoint_uuid}")
                return True
                
        except Exception as e:
            # Fallback to legacy behavior
            if self._legacy_mode:
                try:
                    self._endpoints[endpoint_uuid] = endpoint
                    logger.warning(f"Fallback registration for legacy endpoint {endpoint_uuid}")
                    return True
                except Exception as legacy_error:
                    logger.error(f"Legacy fallback failed: {str(legacy_error)}")
            
            logger.error(f"Error registering endpoint: {str(e)}")
            raise RegistryError(f"Registration failed: {str(e)}")
    
    def unregister(self, endpoint_uuid: str) -> bool:
        """
        Unregister endpoint instance with cleanup
        
        Args:
            endpoint_uuid: UUID of endpoint to unregister
            
        Returns:
            bool: Success status
        """
        try:
            with self._lock:
                if endpoint_uuid not in self._endpoints:
                    if self._legacy_mode:
                        logger.warning(f"Endpoint {endpoint_uuid} not found in registry")
                        return True
                    return False
                
                # Get endpoint
                endpoint = self._endpoints[endpoint_uuid]
                
                # Stop endpoint if active
                if endpoint.config.active:
                    try:
                        endpoint.stop()
                    except Exception as e:
                        logger.error(f"Error stopping endpoint {endpoint_uuid}: {str(e)}")
                
                # Cleanup endpoint
                try:
                    endpoint.cleanup()
                except Exception as e:
                    logger.error(f"Error cleaning up endpoint {endpoint_uuid}: {str(e)}")
                
                # Remove from registry
                del self._endpoints[endpoint_uuid]
                del self._status_timestamps[endpoint_uuid]
                del self._error_counts[endpoint_uuid]
                self._recovery_attempts.pop(endpoint_uuid, None)
                
                # Update database
                try:
                    mongo.db.endpoints.update_one(
                        {"uuid": endpoint_uuid},
                        {
                            "$set": {
                                "registry_status": "unregistered",
                                "unregistered_at": datetime.utcnow()
                            }
                        }
                    )
                except Exception as db_error:
                    logger.error(f"Database update failed: {str(db_error)}")
                
                logger.info(f"Unregistered endpoint: {endpoint_uuid}")
                return True
                
        except Exception as e:
            logger.error(f"Error unregistering endpoint: {str(e)}")
            return False
    
    def get_endpoint(self, endpoint_uuid: str) -> Optional[BaseEndpoint]:
        """Get endpoint instance with legacy support"""
        logger.info(f"[REGISTRY-GET-START] Getting endpoint {endpoint_uuid}")
        try:
            with self._lock:
                logger.info(f"[REGISTRY-GET-LOCK] Acquired lock for {endpoint_uuid}")
                endpoint = self._endpoints.get(endpoint_uuid)
                if not endpoint and self._legacy_mode:
                    # Try to load from database, maintaining legacy support
                    endpoint_data = mongo.db.endpoints.find_one({
                        "uuid": endpoint_uuid,
                        "deleted": {"$ne": True}
                    })
                    if endpoint_data:
                        endpoint = self._create_endpoint_from_data(endpoint_data)
                        if endpoint:
                            self._endpoints[endpoint_uuid] = endpoint
                            self._status_timestamps[endpoint_uuid] = datetime.utcnow()
                            logger.info(f"Loaded legacy endpoint: {endpoint_uuid}")
                return endpoint
        except Exception as e:
            logger.error(f"Error getting endpoint: {str(e)}")
            return None

    
    def get_all_endpoints(self) -> List[BaseEndpoint]:
        """Get all registered endpoints"""
        with self._lock:
            return list(self._endpoints.values())
    
    def get_endpoints_by_type(self, endpoint_type: str) -> List[BaseEndpoint]:
        """Get all endpoints of specified type"""
        with self._lock:
            return [
                endpoint for endpoint in self._endpoints.values()
                if endpoint.config.endpoint_type == endpoint_type
            ]
    
    def get_endpoints_by_organization(self, organization_uuid: str) -> List[BaseEndpoint]:
        """Get all endpoints for an organization"""
        with self._lock:
            return [
                endpoint for endpoint in self._endpoints.values()
                if endpoint.config.organization_uuid == organization_uuid
            ]
    
    def start_endpoint(self, endpoint_uuid: str) -> bool:
        """Start endpoint with improved error handling"""
        try:
            with self._lock:
                endpoint = self._endpoints.get(endpoint_uuid)
                if not endpoint:
                    if self._legacy_mode:
                        endpoint = self._load_legacy_endpoint(endpoint_uuid)
                        if not endpoint:
                            return False
                    else:
                        raise EndpointNotFoundError(f"Endpoint {endpoint_uuid} not found")

                # Don't restart if already active
                if endpoint.config.active:
                    return True

                # Update database status first
                mongo.db.endpoints.update_one(
                    {"uuid": endpoint_uuid},
                    {
                        "$set": {
                            "status": EndpointStatus.STARTING,
                            "updated_at": datetime.utcnow()
                        }
                    }
                )

                # Start endpoint
                success = endpoint.start()

                if success:
                    endpoint.config.active = True
                    self._status_timestamps[endpoint_uuid] = datetime.utcnow()
                    self._error_counts[endpoint_uuid] = 0
                    self._recovery_attempts[endpoint_uuid] = 0

                    # Update final status
                    mongo.db.endpoints.update_one(
                        {"uuid": endpoint_uuid},
                        {
                            "$set": {
                                "status": EndpointStatus.ACTIVE,
                                "updated_at": datetime.utcnow(),
                                "last_active": datetime.utcnow()
                            }
                        }
                    )
                    logger.info(f"Started endpoint: {endpoint_uuid}")
                else:
                    self._handle_endpoint_error(endpoint_uuid)

                return success

        except Exception as e:
            logger.error(f"Error starting endpoint: {str(e)}")
            self._handle_endpoint_error(endpoint_uuid)
            return False
    
    def stop_endpoint(self, endpoint_uuid: str) -> bool:
        """
        Stop registered endpoint with cleanup
        
        Args:
            endpoint_uuid: UUID of endpoint to stop
            
        Returns:
            bool: Success status
        """
        try:
            with self._lock:
                endpoint = self._endpoints.get(endpoint_uuid)
                if not endpoint:
                    if self._legacy_mode:
                        logger.warning(f"Endpoint {endpoint_uuid} not found for stopping")
                        return True
                    raise EndpointNotFoundError(f"Endpoint {endpoint_uuid} not found")
                
                # Update database status first
                mongo.db.endpoints.update_one(
                    {"uuid": endpoint_uuid},
                    {
                        "$set": {
                            "status": EndpointStatus.STOPPING,
                            "updated_at": datetime.utcnow()
                        }
                    }
                )
                
                # Stop endpoint
                success = endpoint.stop()
                
                if success:
                    self._status_timestamps[endpoint_uuid] = datetime.utcnow()
                    logger.info(f"Stopped endpoint: {endpoint_uuid}")
                else:
                    self._handle_endpoint_error(endpoint_uuid)
                    
                return success
                
        except Exception as e:
            logger.error(f"Error stopping endpoint: {str(e)}")
            self._handle_endpoint_error(endpoint_uuid)
            return False
    
    def endpoint_exists(self, endpoint_uuid: str) -> bool:
        """Check if endpoint exists in registry"""
        with self._lock:
            # Check both registry and database for legacy support
            if endpoint_uuid in self._endpoints:
                return True
            if self._legacy_mode:
                return bool(mongo.db.endpoints.find_one({
                    "uuid": endpoint_uuid,
                    "deleted": {"$ne": True}
                }))
            return False

    def restart_endpoint(self, endpoint_uuid: str) -> bool:
        """Restart registered endpoint"""
        try:
            if self.stop_endpoint(endpoint_uuid):
                return self.start_endpoint(endpoint_uuid)
            return False
        except Exception as e:
            logger.error(f"Error restarting endpoint: {str(e)}")
            return False
    
    def _handle_endpoint_error(self, endpoint_uuid: str) -> None:
        """Handle endpoint error with recovery tracking"""
        with self._lock:
            # Increment error count
            self._error_counts[endpoint_uuid] = self._error_counts.get(endpoint_uuid, 0) + 1
            
            # Increment recovery attempts if needed
            if self._error_counts[endpoint_uuid] >= self._max_errors:
                self._recovery_attempts[endpoint_uuid] = self._recovery_attempts.get(endpoint_uuid, 0) + 1
            
            # Check thresholds
            if self._recovery_attempts.get(endpoint_uuid, 0) >= self._max_recovery_attempts:
                logger.error(f"Endpoint {endpoint_uuid} exceeded recovery attempts")
                self._handle_unrecoverable_error(endpoint_uuid)
            elif self._error_counts[endpoint_uuid] >= self._max_errors:
                logger.error(f"Endpoint {endpoint_uuid} exceeded error threshold")
                self._attempt_recovery(endpoint_uuid)
    
    def _handle_unrecoverable_error(self, endpoint_uuid: str) -> None:
        """Handle unrecoverable endpoint error"""
        try:
            # Update database status
            mongo.db.endpoints.update_one(
                {"uuid": endpoint_uuid},
                {
                    "$set": {
                        "status": EndpointStatus.ERROR,
                        "updated_at": datetime.utcnow(),
                        "last_error": "Exceeded recovery attempts",
                        "last_error_time": datetime.utcnow(),
                        "needs_intervention": True
                    }
                }
            )
            
            # Try to cleanup endpoint
            endpoint = self._endpoints.get(endpoint_uuid)
            if endpoint:
                try:
                    endpoint.cleanup()
                except:
                    pass
                    
            # Remove from registry
            self._endpoints.pop(endpoint_uuid, None)
            self._status_timestamps.pop(endpoint_uuid, None)
            self._error_counts.pop(endpoint_uuid, None)
            self._recovery_attempts.pop(endpoint_uuid, None)
            
        except Exception as e:
            logger.error(f"Error handling unrecoverable error: {str(e)}")
    
    def _attempt_recovery(self, endpoint_uuid: str) -> None:
        """Attempt endpoint recovery"""
        try:
            endpoint = self._endpoints.get(endpoint_uuid)
            if not endpoint:
                return
                
            # Stop endpoint
            try:
                endpoint.stop()
            except:
                pass
                
            # Wait briefly
            time.sleep(1)
            
            # Attempt restart
            try:
                if endpoint.start():
                    self._error_counts[endpoint_uuid] = 0
                    logger.info(f"Successfully recovered endpoint {endpoint_uuid}")
                else:
                    logger.error(f"Recovery failed for endpoint {endpoint_uuid}")
            except Exception as e:
                logger.error(f"Error during recovery: {str(e)}")
                
        except Exception as e:
            logger.error(f"Recovery attempt failed: {str(e)}")
    
    def _create_endpoint_from_data(self, endpoint_data: Dict) -> Optional[BaseEndpoint]:
        """Create endpoint instance from database data"""
        try:
            endpoint_type = endpoint_data.get('endpoint_type')
            if not endpoint_type:
                raise ValueError("No endpoint type specified")
            
            # Get endpoint class
            endpoint_class = self._get_endpoint_class(endpoint_type)
            if not endpoint_class:
                raise ValueError(f"Invalid endpoint type: {endpoint_type}")
            
            # Create configuration
            config = EndpointConfig.from_dict(endpoint_data)
            
            # Prepare kwargs based on endpoint type
            kwargs = {}
            if endpoint_type == 'gcp_storage':
                kwargs['provider'] = endpoint_data.get('provider')
                kwargs['storage_config'] = endpoint_data.get('storage_config')
            else:
                kwargs = endpoint_data.get('config', {})
            
            # Create endpoint instance with proper configuration
            return endpoint_class(config=config, **kwargs)
                
        except Exception as e:
            logger.error(f"Error creating endpoint from data: {str(e)}")
            return None

    def _get_endpoint_class(self, endpoint_type: str) -> Optional[type]:
        """Get endpoint class from type"""
        try:
            if endpoint_type not in self._endpoint_types:
                return None
                
            # Import class dynamically
            module_path, class_name = self._endpoint_types[endpoint_type].rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            return getattr(module, class_name)
            
        except Exception as e:
            logger.error(f"Error getting endpoint class: {str(e)}")
            return None

    def _load_legacy_endpoint(self, endpoint_uuid: str) -> Optional[BaseEndpoint]:
        """Load legacy endpoint from database"""
        try:
            endpoint_data = mongo.db.endpoints.find_one({"uuid": endpoint_uuid})
            if not endpoint_data:
                return None
                
            endpoint = self._create_endpoint_from_data(endpoint_data)
            if endpoint:
                self._endpoints[endpoint_uuid] = endpoint
                self._status_timestamps[endpoint_uuid] = datetime.utcnow()
                self._error_counts[endpoint_uuid] = 0
                
            return endpoint
            
        except Exception as e:
            logger.error(f"Error loading legacy endpoint: {str(e)}")
            return None

    def get_endpoint_status(self, endpoint_uuid: str) -> Dict[str, Any]:
        """
        Get comprehensive endpoint status
        
        Args:
            endpoint_uuid: UUID of endpoint to check
            
        Returns:
            Dict containing status information
        """
        try:
            with self._lock:
                endpoint = self._endpoints.get(endpoint_uuid)
                if not endpoint:
                    return {
                        "uuid": endpoint_uuid,
                        "status": "NOT_FOUND",
                        "error": "Endpoint not registered"
                    }

                # Get status from endpoint
                status = endpoint.get_status()
                
                # Add registry metadata
                status.update({
                    "registered_at": self._status_timestamps[endpoint_uuid].isoformat(),
                    "error_count": self._error_counts[endpoint_uuid],
                    "recovery_attempts": self._recovery_attempts.get(endpoint_uuid, 0),
                    "registry_status": "active"
                })
                
                return status
                
        except Exception as e:
            logger.error(f"Error getting endpoint status: {str(e)}")
            return {
                "uuid": endpoint_uuid,
                "status": "ERROR",
                "error": str(e)
            }

    def validate_endpoint(self, endpoint_uuid: str) -> Tuple[bool, Optional[str]]:
        """
        Validate endpoint configuration and state
        
        Args:
            endpoint_uuid: UUID of endpoint to validate
            
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        try:
            with self._lock:
                endpoint = self._endpoints.get(endpoint_uuid)
                if not endpoint:
                    return False, f"Endpoint {endpoint_uuid} not found"
                
                # Check endpoint configuration
                is_valid, error = endpoint.config.validate()
                if not is_valid:
                    return False, error
                
                # Test connection if endpoint has the method
                if hasattr(endpoint, 'test_connection'):
                    success, message = endpoint.test_connection()
                    if not success:
                        return False, message
                
                return True, None
                
        except Exception as e:
            logger.error(f"Error validating endpoint: {str(e)}")
            return False, str(e)

    def cleanup(self) -> None:
        """Cleanup all registered endpoints"""
        logger.info("Starting registry cleanup")
        
        with self._lock:
            endpoint_uuids = list(self._endpoints.keys())
            
            for endpoint_uuid in endpoint_uuids:
                try:
                    self.unregister(endpoint_uuid)
                except Exception as e:
                    logger.error(f"Error cleaning up endpoint {endpoint_uuid}: {str(e)}")
        
        logger.info("Registry cleanup completed")

    def get_registry_stats(self) -> Dict[str, Any]:
        """Get registry statistics"""
        with self._lock:
            return {
                "total_endpoints": len(self._endpoints),
                "endpoints_by_type": self._get_endpoints_by_type_count(),
                "active_endpoints": len([e for e in self._endpoints.values() if e.config.active]),
                "error_endpoints": len([uuid for uuid, count in self._error_counts.items() if count > 0]),
                "endpoints_needing_recovery": len([uuid for uuid, count in self._recovery_attempts.items() 
                                                 if count > 0])
            }

    def _get_endpoints_by_type_count(self) -> Dict[str, int]:
        """Get count of endpoints by type"""
        counts = {}
        for endpoint in self._endpoints.values():
            endpoint_type = endpoint.config.endpoint_type
            counts[endpoint_type] = counts.get(endpoint_type, 0) + 1
        return counts

    def __str__(self) -> str:
        """String representation"""
        return f"EndpointRegistry(endpoints={len(self._endpoints)})"


# Global registry instance
endpoint_registry = EndpointRegistry()