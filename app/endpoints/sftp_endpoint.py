# app/endpoints/sftp_endpoint.py

import os
import threading
import time
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
import uuid
import re
import paramiko
from flask import current_app

from app.endpoints.base import BaseEndpoint
from app.endpoints.endpoint_types import EndpointConfig, EndpointStatus
from app.endpoints.validators.file_validator import FileValidator

class SFTPError(Exception):
    """Base exception for SFTP operations"""
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

class SFTPEndpoint(BaseEndpoint):
    """
    SFTP endpoint implementation for file-based data
    
    Supports:
    - Multiple file types (CCDA, X12, Clinical Notes)
    - File extension validation
    - Secure file transfer
    - File pattern matching
    - Automatic retry
    """

    # Configuration constants
    DEFAULT_FETCH_INTERVAL = 600  # 10 minutes
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
    CONNECTION_TIMEOUT = 30  # seconds
    
    # Valid message types
    VALID_MESSAGE_TYPES = ['CCDA', 'X12', 'Clinical Notes']

    def __init__(self, config: EndpointConfig, **kwargs):
        """Initialize SFTP endpoint"""
        # Validate SFTP specific config
        self._validate_sftp_config(kwargs)
        
        super().__init__(config=config)
        
        # SFTP configuration
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.username = kwargs['username']
        self.password = kwargs.get('password')
        self.private_key = kwargs.get('private_key')
        self.remote_path = kwargs.get('remote_path', '/')
        self.file_pattern = kwargs.get('file_pattern', '.*')
        self.fetch_interval = kwargs.get('fetch_interval', self.DEFAULT_FETCH_INTERVAL)

        # Validate file pattern against allowed extensions
        is_valid, error = FileValidator.validate_file_pattern(
            self.file_pattern, 
            self.config.message_type
        )
        if not is_valid:
            raise ValueError(f"Invalid file pattern: {error}")

        # Connection management
        self.fetching = False
        self.stop_event = threading.Event()
        self.fetch_thread = None
        self.sftp = None
        self.transport = None
        self.connection_lock = threading.Lock()
        self.connection_metrics = ConnectionMetrics()

        # Initialize SFTP-specific metrics
        self.metrics.update({
            'files_processed': 0,
            'files_failed': 0,
            'files_skipped': 0,  # New metric for skipped files
            'invalid_extensions': 0,  # New metric for file extension failures
            'bytes_transferred': 0,
            'last_file_time': None,
            'connection_errors': 0,
            'processing_errors': 0
        })

    @classmethod
    def create(cls, config: Dict[str, Any]) -> 'SFTPEndpoint':
        """
        Create new SFTP endpoint
        
        Args:
            config: Configuration dictionary
            
        Returns:
            New SFTPEndpoint instance
            
        Raises:
            ValueError: If configuration is invalid
        """
        try:
            # Create endpoint config
            endpoint_config = EndpointConfig.from_dict(config)
            
            # Validate configuration
            is_valid, error = endpoint_config.validate()
            if not is_valid:
                raise ValueError(f"Invalid endpoint configuration: {error}")
            
            # Validate message type
            if endpoint_config.message_type not in cls.VALID_MESSAGE_TYPES:
                raise ValueError(f"Invalid message type: {endpoint_config.message_type}")
            
            # Extract non-config parameters
            extra_config = {k: v for k, v in config.items() 
                          if k not in EndpointConfig.__annotations__}
            
            # Create endpoint instance
            return cls(config=endpoint_config, **extra_config)
            
        except Exception as e:
            current_app.logger.error(f"Error creating SFTP endpoint: {str(e)}")
            raise ValueError(f"Failed to create SFTP endpoint: {str(e)}")

    def _validate_sftp_config(self, config: Dict[str, Any]) -> None:
        """
        Validate SFTP specific configuration
        
        Args:
            config: Configuration dictionary
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Check required fields
        required = ['host', 'port', 'username']
        if not all(k in config for k in required):
            raise ValueError(f"Missing required SFTP config: {required}")
        
        # Validate port range
        if not isinstance(config['port'], int) or not (1 <= config['port'] <= 65535):
            raise ValueError(f"Invalid port number: {config['port']}")
        
        # Validate authentication
        if not (config.get('password') or config.get('private_key')):
            raise ValueError("Either password or private key must be provided")
        
        # Validate remote path
        if not config.get('remote_path'):
            raise ValueError("Remote path is required")
        
        # Validate file pattern if provided
        if pattern := config.get('file_pattern'):
            try:
                re.compile(pattern)
            except re.error:
                raise ValueError(f"Invalid file pattern: {pattern}")

    def connect(self) -> bool:
        """
        Establish SFTP connection
        
        Returns:
            Boolean indicating success
        """
        retry_count = 0
        while retry_count < self.MAX_RETRIES:
            try:
                with self.connection_lock:
                    # Update connection metrics
                    self.connection_metrics.connection_attempts += 1
                    self.connection_metrics.last_connection = datetime.utcnow()

                    # Create new transport and connect
                    self.transport = paramiko.Transport((self.host, self.port))
                    self.transport.banner_timeout = self.CONNECTION_TIMEOUT
                    
                    # Handle authentication
                    if self.private_key:
                        key = paramiko.RSAKey.from_private_key_file(self.private_key)
                        self.transport.connect(username=self.username, pkey=key)
                    else:
                        self.transport.connect(username=self.username, password=self.password)
                    
                    # Create SFTP client
                    self.sftp = paramiko.SFTPClient.from_transport(self.transport)
                    
                    # Test connection by listing directory
                    self.sftp.listdir(self.remote_path)
                    
                    # Update success metrics
                    self.connection_metrics.successful_connections += 1
                    
                    self.log_info(f"Connected to SFTP server: {self.host}")
                    self.log_message_cycle(
                        message_uuid=str(uuid.uuid4()),
                        event_type="sftp_connected",
                        details={
                            "host": self.host,
                            "remote_path": self.remote_path
                        },
                        status="success"
                    )
                    return True
                    
            except Exception as e:
                retry_count += 1
                # Update failure metrics
                self.connection_metrics.failed_connections += 1
                self.connection_metrics.last_error = str(e)
                self.connection_metrics.last_error_time = datetime.utcnow()
                
                self.log_error(f"SFTP connection error (attempt {retry_count}): {str(e)}")
                self.metrics.increment('connection_errors')
                
                if retry_count == self.MAX_RETRIES:
                    self.log_message_cycle(
                        message_uuid=str(uuid.uuid4()),
                        event_type="sftp_connection_failed",
                        details={
                            "host": self.host, 
                            "error": str(e),
                            "attempts": retry_count
                        },
                        status="error"
                    )
                    break
                    
                time.sleep(self.RETRY_DELAY)
                
        return False

    def disconnect(self) -> None:
        """Clean up SFTP connection"""
        try:
            with self.connection_lock:
                if self.sftp:
                    self.sftp.close()
                if self.transport:
                    self.transport.close()
                
                self.sftp = None
                self.transport = None
            
            self.log_info(f"Disconnected from SFTP server: {self.host}")
            self.log_message_cycle(
                message_uuid=str(uuid.uuid4()),
                event_type="sftp_disconnected",
                details={"host": self.host},
                status="success"
            )
            
        except Exception as e:
            self.log_error(f"Error during SFTP disconnect: {str(e)}")

    def start(self) -> bool:
        """Start endpoint operations"""
        if not super().start():
            return False

        try:
            if self.config.mode == 'source':
                return self._start_source()
            else:
                raise ValueError(f"Unsupported mode: {self.config.mode}")

        except Exception as e:
            self.log_error(f"Error starting endpoint: {str(e)}")
            return False
        
    def _start_source(self) -> bool:
        """Start source mode operations (file fetching)"""
        if not self.fetching:
            try:
                # Test connection first
                if not self.connect():
                    raise Exception("Failed to connect to SFTP server")
                self.disconnect()
                
                self.fetching = True
                self.stop_event.clear()
                
                # Start fetch thread
                self.fetch_thread = threading.Thread(
                    target=self._fetch_loop,
                    name=f"SFTPFetcher-{self.config.uuid}"
                )
                self.fetch_thread.daemon = True
                self.fetch_thread.start()
                
                self.log_info(f"Started SFTP file fetching from {self.host}")
                return True
                
            except Exception as e:
                self.fetching = False
                self.log_error(f"Failed to start SFTP fetching: {str(e)}")
                return False

    def _fetch_loop(self) -> None:
        """Main file fetching loop"""
        while not self.stop_event.is_set():
            try:
                self._fetch_files()
                
            except Exception as e:
                self.log_error(f"Error in fetch loop: {str(e)}")
                
            # Wait for next interval or stop event
            self.stop_event.wait(timeout=self.fetch_interval)

    def _fetch_files(self) -> None:
        """Fetch and process files from SFTP server"""
        if not self.connect():
            return

        try:
            # List remote files
            remote_files = self.sftp.listdir(self.remote_path)
            
            for filename in remote_files:
                if self.stop_event.is_set():
                    break

                try:
                    # Check file pattern
                    if not re.match(self.file_pattern, filename):
                        self.log_debug(f"File {filename} does not match pattern, skipping")
                        self.metrics.increment('files_skipped')
                        continue

                    # Validate file extension
                    if not FileValidator.is_valid_extension(filename, self.config.message_type):
                        self.log_warning(
                            f"Invalid file extension for {filename}, "
                            f"message type: {self.config.message_type}"
                        )
                        self.metrics.increment('invalid_extensions')
                        self.metrics.increment('files_skipped')
                        continue

                    # Check if file already processed
                    if self._is_file_processed(filename):
                        self.log_debug(f"File {filename} already processed, skipping")
                        self.metrics.increment('files_skipped')
                        continue

                    remote_path = os.path.join(self.remote_path, filename)
                    
                    # Check file size
                    file_stat = self.sftp.stat(remote_path)
                    if file_stat.st_size > self.MAX_FILE_SIZE:
                        self.log_warning(
                            f"File {filename} exceeds size limit "
                            f"({file_stat.st_size} bytes), skipping"
                        )
                        self.metrics.increment('files_skipped')
                        continue

                    # Create storage directory if needed
                    storage_path = self._get_storage_path()
                    os.makedirs(storage_path, exist_ok=True)

                    # Download file
                    local_path = os.path.join(storage_path, filename)
                    self.sftp.get(remote_path, local_path)
                    
                    # Store message entry
                    message_uuid = self._store_file_message(filename, local_path, file_stat.st_size)
                    if message_uuid:
                        self._queue_file_for_processing(message_uuid, filename)
                        self.metrics.increment('files_processed')
                        self.metrics.increment('bytes_transferred', file_stat.st_size)
                        self.metrics.set('last_file_time', datetime.utcnow())
                    else:
                        os.remove(local_path)  # Clean up on failure

                except Exception as e:
                    self.log_error(f"Error processing file {filename}: {str(e)}")
                    self.metrics.increment('files_failed')
                    self.metrics.increment('processing_errors')

        except Exception as e:
            self.log_error(f"Error fetching files: {str(e)}")
        finally:
            self.disconnect()

    def _store_file_message(self, filename: str, local_path: str, file_size: int) -> Optional[str]:
        """Store file message entry"""
        message_uuid = str(uuid.uuid4())
        message = {
            "uuid": message_uuid,
            "endpoint_uuid": self.config.uuid,
            "organization_uuid": self.config.organization_uuid,
            "filename": filename,
            "local_path": local_path,
            "file_size": file_size,
            "timestamp": datetime.utcnow(),
            "type": self.config.message_type,
            "parsing_status": "pending",
            "conversion_status": "pending_parsing",
            "retry_count": 0,
            "deidentification_requested": self.config.deidentification_requested,
            "purging_requested": self.config.purging_requested,
            "metadata": {
                "remote_path": os.path.join(self.remote_path, filename),
                "file_pattern": self.file_pattern,
                "file_extension": os.path.splitext(filename)[1].lower(),
                "created_at": datetime.utcnow().isoformat(),
                "storage_path": self._get_storage_path()
            }
        }

        try:
            result = self.messages_collection.insert_one(message)
            
            if result.acknowledged:
                self.log_info(f"File message stored: {filename}")
                self.log_message_cycle(
                    message_uuid=message_uuid,
                    event_type="file_stored",
                    details={
                        "filename": filename,
                        "type": self.config.message_type,
                        "size": file_size,
                        "extension": message["metadata"]["file_extension"]
                    },
                    status="success"
                )
                return message_uuid
                
        except Exception as e:
            self.log_error(f"Error storing file message {filename}: {str(e)}")
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="file_store_failed",
                details={"filename": filename, "error": str(e)},
                status="error"
            )
            
        return None

    def _get_storage_path(self) -> str:
        """Get local storage path for downloaded files"""
        base_path = current_app.config.get('STORAGE_PATH', '/tmp')
        return os.path.join(base_path, 'sftp', self.config.uuid)

    def _is_file_processed(self, filename: str) -> bool:
        """Check if file has already been processed"""
        return self.messages_collection.find_one({
            "endpoint_uuid": self.config.uuid,
            "filename": filename,
            "type": self.config.message_type
        }) is not None

    def _queue_file_for_processing(self, message_uuid: str, filename: str) -> None:
        """Queue file for parsing and processing"""
        try:
            from app.tasks import parse_sftp_file
            
            task = parse_sftp_file.apply_async(
                args=[message_uuid],
                queue=current_app.config['SFTP_FILE_PARSING_QUEUE']
            )
            
            self.log_info(f"File {filename} queued for processing (Task: {task.id})")
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="queued_for_processing",
                details={
                    "filename": filename,
                    "task_id": task.id,
                    "message_type": self.config.message_type
                },
                status="success"
            )
            
        except Exception as e:
            self.log_error(f"Failed to queue file {filename} for processing: {str(e)}")
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="queue_failed",
                details={"filename": filename, "error": str(e)},
                status="error"
            )

    def stop(self) -> bool:
        """Stop endpoint operations"""
        if not super().stop():
            return False

        try:
            if self.fetching:
                self.fetching = False
                self.stop_event.set()
                
                # Wait for fetch thread to complete
                if self.fetch_thread and self.fetch_thread.is_alive():
                    self.fetch_thread.join(timeout=5)
                
                self.disconnect()
                self.log_info("SFTP fetching stopped")
                return True
                
        except Exception as e:
            self.log_error(f"Error stopping SFTP fetching: {str(e)}")
            return False
        finally:
            self.fetch_thread = None

    def get_metrics(self) -> Dict[str, Any]:
        """Get endpoint metrics"""
        metrics = super().get_metrics()
        metrics.update({
            "files_processed": self.metrics.get('files_processed', 0),
            "files_failed": self.metrics.get('files_failed', 0),
            "files_skipped": self.metrics.get('files_skipped', 0),
            "invalid_extensions": self.metrics.get('invalid_extensions', 0),
            "bytes_transferred": self.metrics.get('bytes_transferred', 0),
            "connection_errors": self.metrics.get('connection_errors', 0),
            "processing_errors": self.metrics.get('processing_errors', 0),
            "last_file_time": self.metrics.get('last_file_time').isoformat() 
                            if self.metrics.get('last_file_time') else None,
            "fetching_active": self.fetching,
            "remote_path": self.remote_path,
            "file_pattern": self.file_pattern,
            "allowed_extensions": FileValidator.get_allowed_extensions(self.config.message_type),
            "connection_stats": {
                "attempts": self.connection_metrics.connection_attempts,
                "successful": self.connection_metrics.successful_connections,
                "failed": self.connection_metrics.failed_connections,
                "last_error": self.connection_metrics.last_error,
                "last_error_time": (
                    self.connection_metrics.last_error_time.isoformat()
                    if self.connection_metrics.last_error_time else None
                )
            }
        })
        return metrics

    def test_connection(self) -> Tuple[bool, str]:
        """Test connection to SFTP server"""
        try:
            if self.connect():
                # Try to list directory
                self.sftp.listdir(self.remote_path)
                self.disconnect()
                return True, "Connection successful"
            return False, "Failed to connect to SFTP server"
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"

    def cleanup(self) -> None:
        """Perform cleanup operations"""
        try:
            self.stop()
            
            # Clean up local storage
            storage_path = self._get_storage_path()
            if os.path.exists(storage_path):
                for filename in os.listdir(storage_path):
                    try:
                        os.remove(os.path.join(storage_path, filename))
                    except Exception as e:
                        self.log_error(f"Error removing file {filename}: {str(e)}")
                try:
                    os.rmdir(storage_path)
                except Exception as e:
                    self.log_error(f"Error removing directory {storage_path}: {str(e)}")
            
            self.log_info("Cleanup completed successfully")
        except Exception as e:
            self.log_error(f"Error during cleanup: {str(e)}")

    def __str__(self) -> str:
        """String representation"""
        return (f"SFTPEndpoint(uuid={self.config.uuid}, name={self.config.name}, "
                f"type={self.config.message_type}, host={self.host}, active={self.fetching})")

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"SFTPEndpoint(uuid={self.config.uuid}, name={self.config.name}, "
                f"mode={self.config.mode}, message_type={self.config.message_type}, "
                f"endpoint_type={self.config.endpoint_type}, "
                f"organization_uuid={self.config.organization_uuid}, "
                f"host={self.host}, port={self.port}, "
                f"username={self.username}, active={self.fetching})")