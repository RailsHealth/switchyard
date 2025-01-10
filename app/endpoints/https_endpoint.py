# app/endpoints/https_endpoint.py

import threading
import time
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
import requests
import json
import uuid
import logging
from flask import current_app
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from app.endpoints.base import BaseEndpoint
from app.endpoints.endpoint_types import EndpointConfig, EndpointStatus
from app.services.fhir_validator import FHIRValidator
from app.utils.logging_utils import log_message_cycle

class HTTPSEndpointError(Exception):
    """Base exception for HTTPS endpoint operations"""
    pass

class ConnectionMetrics:
    """Connection-level metrics tracking"""
    def __init__(self):
        self.last_connection = None
        self.connection_attempts = 0
        self.successful_connections = 0
        self.failed_connections = 0
        self.last_error = None
        self.last_error_time = None
        self.resources_fetched = 0
        self.fetch_errors = 0
        self.last_fetch_time = None

class HTTPSEndpoint(BaseEndpoint):
    """HTTPS endpoint implementation for FHIR servers"""

    # Configuration constants
    DEFAULT_FETCH_INTERVAL = 600  # 10 minutes
    DEFAULT_REQUEST_RATE = 10     # requests per second
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    SOCKET_TIMEOUT = 30
    SHUTDOWN_TIMEOUT = 10
    MAX_RESOURCES_PER_REQUEST = 50
    MIN_FETCH_WINDOW = timedelta(minutes=5)
    
    VALID_SERVER_TYPES = ['hapi', 'firely']
    VALID_FHIR_VERSIONS = ['R4']

    def __init__(self, config: EndpointConfig, **kwargs):
        """Initialize HTTPS endpoint"""
        # Initialize base endpoint
        super().__init__(config=config)
        
        # Connection settings
        self.base_url = kwargs.get('base_url') or self._get_server_url()
        self.server_type = kwargs.get('server_type')
        self.session = None
        self.processing_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        
        # Retry settings
        self.max_retries = kwargs.get('max_retries', self.MAX_RETRIES)
        self.retry_delay = kwargs.get('retry_delay', self.RETRY_DELAY)
        
        # Fetch settings
        self.fetch_interval = kwargs.get('fetch_interval', self.DEFAULT_FETCH_INTERVAL)
        self.last_fetch_time = None
        self.fetch_thread = None
        self.polling = False

        # Rate limiting
        self.request_rate = kwargs.get('request_rate', self.DEFAULT_REQUEST_RATE)
        self.last_request_time = 0
        
        # Create logger
        self.logger = logging.getLogger(f'HTTPSEndpoint-{config.uuid}')
        self.logger.setLevel(logging.DEBUG)

        # Initialize metrics
        self.connection_metrics = ConnectionMetrics()

    def _get_server_url(self) -> str:
        """Get server URL from configuration"""
        server_config = current_app.config['FHIR_SERVERS'].get(self.server_type)
        if not server_config:
            raise HTTPSEndpointError(f"Unknown FHIR server type: {self.server_type}")
        return server_config['url']

    def start_polling(self):
        """Start polling the FHIR server"""
        if not self.polling:
            try:
                # Test connection first
                self._test_connection()
                
                self.polling = True
                self.shutdown_event.clear()
                self.last_fetch_time = self._get_last_fetch_time()
                
                self.fetch_thread = threading.Thread(
                    target=self._poll_loop, 
                    name=f"FHIRPoller-{self.config.uuid}"
                )
                self.fetch_thread.daemon = True
                self.fetch_thread.start()
                
                self.logger.info(f"Started polling FHIR server: {self.base_url}")
                return True
                
            except Exception as e:
                self.logger.error(f"Failed to start polling: {str(e)}")
                return False

    def stop_polling(self):
        """Stop polling the FHIR server"""
        if self.polling:
            self.polling = False
            self.shutdown_event.set()
            
            if self.fetch_thread:
                self.fetch_thread.join(timeout=self.SHUTDOWN_TIMEOUT)
                if self.fetch_thread.is_alive():
                    self.logger.warning("Fetch thread did not stop within timeout")
            
            if self.session:
                self.session.close()
                self.session = None
                
            self.logger.info(f"Stopped polling FHIR server: {self.base_url}")
            self.fetch_thread = None

    def _poll_loop(self):
        """Main polling loop"""
        while not self.shutdown_event.is_set():
            try:
                self._fetch_resources()
            except Exception as e:
                self.logger.error(f"Error in poll loop: {str(e)}")
            
            # Wait for next interval or shutdown
            self.shutdown_event.wait(self.fetch_interval)

    def _fetch_resources(self):
        """Fetch resources from FHIR server"""
        if self.shutdown_event.is_set():
            return

        try:
            # Different URL structure and params for HAPI vs Firely
            if self.server_type == 'hapi':
                search_url = f"{self.base_url}/"
                params = {
                    "_lastUpdated": f"gt{self.last_fetch_time.isoformat()}" if self.last_fetch_time else None,
                    "_count": "10",
                    "_format": "json"
                }
            else:
                search_url = f"{self.base_url}/Bundle"
                params = {
                    "_sort": "-_lastUpdated",
                    "_count": "10",
                    "_lastUpdated": f"gt{self.last_fetch_time.isoformat()}" if self.last_fetch_time else None
                }

            with self.processing_lock:
                response = self._make_request("GET", search_url, params=params)
                bundle = response.json()

                if bundle.get("resourceType") == "Bundle":
                    self._process_bundle(bundle)

                    if bundle.get("entry"):
                        latest_update = max(
                            entry["resource"].get("meta", {}).get("lastUpdated", "") 
                            for entry in bundle["entry"]
                        )
                        if latest_update:
                            self.last_fetch_time = datetime.fromisoformat(latest_update.rstrip('Z'))

                    # Handle pagination
                    next_link = next(
                        (link for link in bundle.get("link", []) if link["relation"] == "next"),
                        None
                    )
                    if next_link:
                        search_url = next_link["url"]
                        params = None
                else:
                    self.logger.warning(f"Unexpected response format: {bundle.get('resourceType', 'Unknown')}")

        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with retries"""
        for attempt in range(self.max_retries):
            try:
                if self.server_type == 'hapi':
                    # Add headers specifically for HAPI
                    headers = {
                        'Accept': 'application/fhir+json',
                        'Prefer': 'return=representation'
                    }
                    if 'headers' in kwargs:
                        kwargs['headers'].update(headers)
                    else:
                        kwargs['headers'] = headers

                response = requests.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff

    def _test_connection(self) -> None:
        """Test connection to FHIR server"""
        try:
            # Test with Patient resource for HAPI
            if self.server_type == 'hapi':
                test_url = f"{self.base_url}/Patient"
                params = {
                    "_count": "1",
                    "_format": "json"
                }
                headers = {
                    'Accept': 'application/fhir+json'
                }
                response = self._make_request("GET", test_url, params=params, headers=headers)
            else:
                metadata_url = f"{self.base_url}/metadata"
                response = self._make_request("GET", metadata_url)
            
            # Basic validation of response
            data = response.json()
            if 'resourceType' not in data:
                raise HTTPSEndpointError("Invalid FHIR response")
                
            self.logger.info("Successfully connected to FHIR server")
            
        except Exception as e:
            raise HTTPSEndpointError(f"Connection test failed: {str(e)}")

    def _process_bundle(self, bundle: Dict[str, Any]) -> None:
        """Process and store FHIR bundle"""
        if not bundle.get("entry"):
            return

        for entry in bundle["entry"]:
            message_uuid = str(uuid.uuid4())
            try:
                resource = entry.get("resource")
                if not resource:
                    continue

                message_dict = {
                    "uuid": message_uuid,
                    "endpoint_uuid": self.config.uuid,
                    "organization_uuid": self.config.organization_uuid,
                    "message": json.dumps(resource),
                    "resource_type": resource.get("resourceType"),
                    "timestamp": datetime.utcnow(),
                    "type": "FHIR",
                    "validation_status": "pending",
                    "metadata": {
                        "server_type": self.server_type,
                        "bundle_source": bundle.get("id"),
                        "resource_lastUpdated": resource.get("meta", {}).get("lastUpdated"),
                        "fetch_time": datetime.utcnow().isoformat()
                    }
                }

                result = self.messages_collection.insert_one(message_dict)
                if result.acknowledged:
                    self.logger.info(f"FHIR resource stored: {message_uuid}")
                    self.connection_metrics.resources_fetched += 1
                else:
                    self.logger.error(f"Failed to store FHIR resource: {message_uuid}")

            except Exception as e:
                self.logger.error(f"Error processing resource: {str(e)}")

    def _get_last_fetch_time(self) -> Optional[datetime]:
        """Get timestamp of last fetched message"""
        last_message = self.messages_collection.find_one(
            {
                "endpoint_uuid": self.config.uuid, 
                "type": "FHIR",
                "validation_status": {"$ne": "invalid"}
            },
            sort=[("timestamp", -1)]
        )
        if last_message:
            return last_message["timestamp"]
        return datetime.utcnow() - self.MIN_FETCH_WINDOW

    def start(self) -> bool:
        """Start endpoint operations"""
        return self.start_polling()

    def stop(self) -> bool:
        """Stop endpoint operations"""
        self.stop_polling()
        return True

    def cleanup(self) -> None:
        """Cleanup endpoint resources"""
        self.stop()

    def get_metrics(self) -> Dict[str, Any]:
        """Get endpoint metrics"""
        return {
            "server_type": self.server_type,
            "resources_fetched": self.connection_metrics.resources_fetched,
            "fetch_errors": self.connection_metrics.fetch_errors,
            "successful_connections": self.connection_metrics.successful_connections,
            "failed_connections": self.connection_metrics.failed_connections,
            "last_fetch_time": (
                self.connection_metrics.last_fetch_time.isoformat()
                if self.connection_metrics.last_fetch_time
                else None
            ),
            "last_error": self.connection_metrics.last_error,
            "last_error_time": (
                self.connection_metrics.last_error_time.isoformat()
                if self.connection_metrics.last_error_time
                else None
            ),
            "polling_active": self.polling
        }

    def __str__(self) -> str:
        """String representation"""
        return f"HTTPSEndpoint(url={self.base_url}, active={self.polling})"