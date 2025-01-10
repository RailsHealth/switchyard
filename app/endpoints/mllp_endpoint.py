# app/endpoints/mllp_endpoint.py

import socket
import threading
import time
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
from contextlib import contextmanager
import uuid
from flask import current_app
from hl7apy.parser import parse_message
from hl7apy.exceptions import ParserError, UnsupportedVersion

from app.endpoints.base import BaseEndpoint
from app.endpoints.endpoint_types import EndpointConfig, EndpointStatus
from app.services.test_hl7_client import TestHL7Client

class MLLPProtocolError(Exception):
    """Exception for MLLP protocol errors"""
    pass

class ConnectionMetrics:
    """Connection-level metrics tracking"""
    def __init__(self):
        self.connected_at = datetime.utcnow()
        self.messages_received = 0
        self.messages_sent = 0
        self.errors = 0
        self.last_activity = None
        self.last_error = None

class MLLPEndpoint(BaseEndpoint):
    """
    MLLP endpoint implementation for HL7v2 messages
    
    Supports:
    - Standard MLLP connections
    - Test endpoint integration
    - Connection management and metrics
    - Message processing and acknowledgments
    """
    
    # Configuration constants
    MAX_CONNECTIONS = 10
    MAX_MESSAGE_SIZE = 1024 * 1024  # 1MB
    MESSAGE_QUEUE_SIZE = 1000
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1.0  # seconds
    SOCKET_TIMEOUT = 1.0  # seconds
    SHUTDOWN_TIMEOUT = 5.0  # seconds
    
    # MLLP protocol characters
    VT = b'\x0b'  # Start block
    FS = b'\x1c'  # End block
    CR = b'\x0d'  # Carriage return

    def __init__(self, config: EndpointConfig, **kwargs):
        """Initialize MLLP endpoint"""
        # Validate MLLP specific config
        self._validate_mllp_config(kwargs)
        
        # Initialize base endpoint
        super().__init__(config=config)
        
        # MLLP configuration
        self.host = kwargs['host']
        self.port = kwargs['port']
        self.timeout = kwargs.get('timeout', 60)
        
        # Connection management
        self.server_socket = None
        self.listening = False
        self.stop_event = threading.Event()
        self.active_connections: Dict[str, ConnectionMetrics] = {}
        self.connection_lock = threading.Lock()
        self.receive_buffers: Dict[str, bytearray] = {}
        
        # Test client for test endpoints
        self.test_client = TestHL7Client() if self.config.is_test else None
        
        # Update metrics with MLLP-specific counters
        self.metrics.update({
            'messages_processed': 0,
            'messages_failed': 0,
            'parse_errors': 0,
            'protocol_errors': 0,
            'connection_errors': 0,
            'last_message_time': None
        })

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> bool:
        """
        Validate MLLP endpoint configuration
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Boolean indicating if configuration is valid
        """
        try:
            # Validate base configuration
            endpoint_config = EndpointConfig.from_dict(config)
            is_valid, error = endpoint_config.validate()
            if not is_valid:
                return False

            # Validate MLLP specific parameters
            required = ['host', 'port']
            if not all(k in config for k in required):
                return False

            # Validate port range
            if not isinstance(config['port'], int) or not (1 <= config['port'] <= 65535):
                return False

            # Validate message type
            if config['message_type'] != 'HL7v2':
                return False

            # Validate mode (source only for MLLP)
            if config['mode'] != 'source':
                return False

            return True

        except Exception as e:
            current_app.logger.error(f"MLLP config validation error: {str(e)}")
            return False

    def _validate_mllp_config(self, config: Dict[str, Any]) -> None:
        """Validate MLLP specific configuration"""
        if not isinstance(config.get('port'), int) or not (1 <= config['port'] <= 65535):
            raise ValueError(f"Invalid port number: {config.get('port')}")
        
        if 'host' not in config:
            raise ValueError("Host is required")

    def start(self) -> bool:
        """Start endpoint operations"""
        try:
            if not super().start():
                return False

            # Start test service for test endpoints
            if self.config.is_test:
                result = self.test_client.start_stream(self.config.organization_uuid)
                if not result or result.get('status') != 'success':
                    raise Exception('Failed to start test stream service')

            # Start based on mode
            if self.config.mode == 'source':
                success = self._start_source()
                if not success:
                    self._set_error_status()
                    return False
                return True
            else:
                raise ValueError(f"Unsupported mode: {self.config.mode}")

        except Exception as e:
            self.log_error(f"Error starting endpoint: {str(e)}")
            if self.config.is_test:
                try:
                    self.test_client.stop_stream(self.config.organization_uuid)
                except Exception as stop_error:
                    self.log_error(f"Error stopping test stream: {str(stop_error)}")
            self._set_error_status()
            return False

    def _start_source(self) -> bool:
        """Start source mode operations"""
        if not self.listening:
            last_error = None
            
            # Try to bind socket with retries
            for attempt in range(self.RETRY_ATTEMPTS):  # Using existing RETRY_ATTEMPTS constant
                try:
                    # Create new socket for each attempt
                    if self.server_socket:
                        try:
                            self.server_socket.close()
                        except Exception:
                            pass

                    # Create and configure server socket
                    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    
                    # Set socket options
                    self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    if hasattr(socket, 'SO_REUSEPORT'):
                        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    
                    # Attempt to bind
                    self.server_socket.bind((self.host, self.port))
                    self.server_socket.listen(self.MAX_CONNECTIONS)
                    self.server_socket.settimeout(self.SOCKET_TIMEOUT)
                    
                    # Successfully bound and listening
                    self.listening = True
                    self.stop_event.clear()
                    
                    # Start listener thread
                    self.listener_thread = threading.Thread(
                        target=self._listen_loop,
                        name=f"MLLPListener-{self.config.uuid}"
                    )
                    self.listener_thread.daemon = True
                    self.listener_thread.start()
                    
                    self.log_info(f"Started MLLP listener on {self.host}:{self.port}")
                    return True

                except Exception as e:
                    last_error = e
                    self.log_warning(f"Bind attempt {attempt + 1} failed: {str(e)}")
                    if attempt < self.RETRY_ATTEMPTS - 1:
                        time.sleep(self.RETRY_DELAY * (attempt + 1))
                    
                    # Clean up failed socket
                    if self.server_socket:
                        try:
                            self.server_socket.close()
                        except Exception:
                            pass
                        self.server_socket = None

            # All retries failed
            self.listening = False
            error_msg = f"Failed to start MLLP listener after {self.RETRY_ATTEMPTS} attempts"
            if last_error:
                error_msg += f": {str(last_error)}"
            self.log_error(error_msg)
            return False

    def _listen_loop(self) -> None:
        """Main listener loop for incoming connections"""
        while not self.stop_event.is_set():
            try:
                client_socket, client_address = self.server_socket.accept()
                
                # Check connection limit
                with self.connection_lock:
                    if len(self.active_connections) >= self.MAX_CONNECTIONS:
                        client_socket.close()
                        self.log_warning(
                            f"Maximum connections reached, rejecting {client_address}"
                        )
                        continue
                
                # Configure client socket
                client_socket.settimeout(self.timeout)
                client_id = str(uuid.uuid4())
                
                # Track connection
                with self.connection_lock:
                    self.active_connections[client_id] = ConnectionMetrics()
                
                # Handle client in separate thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, client_id, client_address),
                    name=f"MLLPClient-{client_id}"
                )
                client_thread.daemon = True
                client_thread.start()
                
                self.log_info(f"New connection from {client_address} (ID: {client_id})")
                
            except socket.timeout:
                continue
            except Exception as e:
                self.log_error(f"Error in listener loop: {str(e)}")
                self.metrics.increment('connection_errors')
                time.sleep(1)

    def _handle_client(self, client_socket: socket.socket, client_id: str, 
                      address: tuple) -> None:
        """Handle individual client connection"""
        try:
            with client_socket:
                while not self.stop_event.is_set():
                    try:
                        with self._get_client_buffer(client_id) as buffer:
                            # Receive data
                            chunk = client_socket.recv(1024)
                            if not chunk:
                                break
                            
                            buffer.extend(chunk)
                            
                            # Check for complete message
                            if self._is_complete_message(buffer):
                                try:
                                    # Process message
                                    message = self._unwrap_message(buffer)
                                    self._process_message(message, client_socket)
                                    buffer.clear()
                                    
                                    # Update connection metrics
                                    with self.connection_lock:
                                        metrics = self.active_connections[client_id]
                                        metrics.messages_received += 1
                                        metrics.last_activity = datetime.utcnow()
                                    
                                except MLLPProtocolError as e:
                                    self.log_error(f"Protocol error from {address}: {str(e)}")
                                    self.metrics.increment('protocol_errors')
                                    buffer.clear()
                                    
                    except socket.timeout:
                        continue
                        
        except Exception as e:
            self.log_error(f"Error handling client {client_id}: {str(e)}")
            with self.connection_lock:
                if client_id in self.active_connections:
                    self.active_connections[client_id].errors += 1
                    self.active_connections[client_id].last_error = str(e)
            
        finally:
            # Cleanup
            with self.connection_lock:
                self.active_connections.pop(client_id, None)
            self.receive_buffers.pop(client_id, None)
            self.log_info(f"Connection closed for client {client_id}")

    def _is_complete_message(self, buffer: bytearray) -> bool:
        """Check if buffer contains complete MLLP message"""
        return (len(buffer) >= 3 and
                buffer.startswith(self.VT) and
                self.FS in buffer and
                buffer.endswith(self.CR))

    def _unwrap_message(self, buffer: bytes) -> str:
        """Unwrap MLLP message"""
        try:
            if self._is_complete_message(buffer):
                message = buffer[1:buffer.rfind(self.FS)].decode('utf-8')
                return message
            raise MLLPProtocolError("Incomplete MLLP message")
        except UnicodeDecodeError as e:
            raise MLLPProtocolError(f"Invalid message encoding: {str(e)}")

    def _process_message(self, message: str, client_socket: socket.socket) -> None:
        """Process received message and send ACK"""
        message_uuid = str(uuid.uuid4())
        try:
            # Parse and validate message
            parsed_message = parse_message(message, find_groups=False, validation_level=2)
            self.log_info(f"Message {message_uuid} parsed successfully")
            
            # Store message
            stored = self._store_message(message_uuid, message, parsed=True)
            
            # Create and send ACK
            ack = self._create_ack(parsed_message)
            client_socket.sendall(self._wrap_message(ack))
            
            self.log_info(f"ACK sent for message {message_uuid}")
            self.metrics.increment('messages_processed')
            self.metrics.set('last_message_time', datetime.utcnow())

            # Queue for conversion if stored successfully
            if stored:
                self._queue_message(message_uuid)
                
            # Log successful processing
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="message_processed",
                details={
                    "endpoint_uuid": self.config.uuid,
                    "message_type": "HL7v2",
                    "parsed": True
                },
                status="success"
            )

        except (ParserError, UnsupportedVersion) as e:
            self.log_error(f"Error parsing message {message_uuid}: {str(e)}")
            self.metrics.increment('parse_errors')
            
            # Store raw message
            self._store_message(message_uuid, message, parsed=False)
            
            # Send negative ACK
            nack = self._create_nack(message, str(e))
            client_socket.sendall(self._wrap_message(nack))

            # Log parsing failure
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="message_parse_failed",
                details={
                    "endpoint_uuid": self.config.uuid,
                    "message_type": "HL7v2",
                    "error": str(e)
                },
                status="error"
            )

        except Exception as e:
            self.log_error(f"Error processing message {message_uuid}: {str(e)}")
            
            # Log unexpected error
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="message_processing_error",
                details={
                    "endpoint_uuid": self.config.uuid,
                    "message_type": "HL7v2",
                    "error": str(e)
                },
                status="error"
            )

    def _store_message(self, message_uuid: str, message: str, parsed: bool = True) -> bool:
        """Store received message"""
        message_dict = {
            "uuid": message_uuid,
            "endpoint_uuid": self.config.uuid,
            "organization_uuid": self.config.organization_uuid,
            "message": message,
            "parsed": parsed,
            "timestamp": datetime.utcnow(),
            "type": "HL7v2",
            "conversion_status": "pending",
            "retry_count": 0,
            "is_test": self.config.is_test,
            "metadata": {
                "source_host": self.host,
                "source_port": self.port
            }
        }

        try:
            result = self.messages_collection.insert_one(message_dict)
            
            if result.acknowledged:
                self.log_info(f"Message {message_uuid} stored successfully")
                self.log_message_cycle(
                    message_uuid=message_uuid,
                    event_type="message_received",
                    details={"parsed": parsed},
                    status="success"
                )
                return True
                
        except Exception as e:
            self.log_error(f"Failed to store message {message_uuid}: {str(e)}")
            self.metrics.increment('messages_failed')
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="store_failed",
                details={"error": str(e)},
                status="error"
            )
            
        return False

    def _queue_message(self, message_uuid: str) -> None:
        """Queue message for conversion"""
        try:
            from app.tasks import generic_to_fhir_conversion
            
            task = generic_to_fhir_conversion.apply_async(
                args=[message_uuid, "HL7v2"],
                queue='hl7v2_conversion'
            )
            
            self.log_info(f"Message {message_uuid} queued for conversion (Task: {task.id})")
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="queued_for_conversion",
                details={"task_id": task.id},
                status="success"
            )
            
        except Exception as e:
            self.log_error(f"Failed to queue message {message_uuid}: {str(e)}")
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="queue_failed",
                details={"error": str(e)},
                status="error"
            )

    def _wrap_message(self, message: str) -> bytes:
        """Wrap message with MLLP characters"""
        return self.VT + message.encode('utf-8') + self.FS + self.CR

    def _create_ack(self, message) -> str:
        """Create acknowledgment message"""
        try:
            # Extract MSH segment for response
            msh = message.msh
            
            # Create ACK message
            ack = parse_message(
                "MSH|^~\\&|{0}|{1}|{2}|{3}|{4}||ACK^{5}|{6}|P|{7}".format(
                    msh.msh_5.value,  # Receiving application
                    msh.msh_6.value,  # Receiving facility
                    msh.msh_3.value,  # Sending application
                    msh.msh_4.value,  # Sending facility
                    datetime.now().strftime('%Y%m%d%H%M%S'),
                    msh.msh_9.value.split('^')[1] if '^' in msh.msh_9.value else 'ACK',
                    str(uuid.uuid4()),
                    msh.msh_12.value  # Version ID
                )
            )
            
            # Add MSA segment
            ack.add_segment("MSA")
            ack.msa.msa_1 = "AA"  # Application Accept
            ack.msa.msa_2 = msh.msh_10.value  # Original Message Control ID
            
            return ack.to_er7()
            
        except Exception as e:
            self.log_error(f"Error creating ACK: {str(e)}")
            return self._create_fallback_ack()

    def _create_nack(self, original_message: str, error: str) -> str:
        """Create negative acknowledgment"""
        try:
            # Try to extract basic info from original message
            segments = original_message.split('\r')
            msh = segments[0].split('|')
            
            if len(msh) < 12:
                return self._create_fallback_ack("AE", error)
            
            # Extract fields from original message
            sending_app = msh[2]
            sending_facility = msh[3]
            receiving_app = msh[4]
            receiving_facility = msh[5]
            message_control_id = msh[9] if len(msh) > 9 else str(uuid.uuid4())
            version = msh[11]
            
            # Create NACK message
            nack = (
                f"MSH|^~\\&|{receiving_app}|{receiving_facility}|{sending_app}|"
                f"{sending_facility}|{datetime.now().strftime('%Y%m%d%H%M%S')}||"
                f"ACK|{str(uuid.uuid4())}|P|{version}\r"
                f"MSA|AE|{message_control_id}\r"
                f"ERR|||207|E|||{error}"
            )
            
            return nack
            
        except Exception as e:
            self.log_error(f"Error creating NACK: {str(e)}")
            return self._create_fallback_ack("AE", error)

    def _create_fallback_ack(self, ack_code: str = "AA", error: str = "") -> str:
        """Create fallback acknowledgment when normal creation fails"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        message_id = str(uuid.uuid4())
        
        ack = (
            "MSH|^~\\&|RecApp|RecFac|SendApp|SendFac|"
            f"{timestamp}||ACK|{message_id}|P|2.5\r"
            f"MSA|{ack_code}|Unknown\r"
        )
        
        if ack_code == "AE" and error:
            ack += f"ERR|||207|E|||{error}"
        
        return ack

    @contextmanager
    def _get_client_buffer(self, client_id: str) -> bytearray:
        """Get or create message buffer for client"""
        if client_id not in self.receive_buffers:
            self.receive_buffers[client_id] = bytearray()
        try:
            yield self.receive_buffers[client_id]
        finally:
            if len(self.receive_buffers[client_id]) > self.MAX_MESSAGE_SIZE:
                self.log_warning(f"Buffer overflow for client {client_id}, clearing buffer")
                self.receive_buffers[client_id].clear()

    def stop(self) -> bool:
        """Stop endpoint operations"""
        if not super().stop():
            return False

        try:
            # Stop test service if applicable
            if self.config.is_test:
                try:
                    self.test_client.stop_stream(self.config.organization_uuid)
                except Exception as e:
                    self.log_error(f"Error stopping test stream: {str(e)}")

            if self.listening:
                self.listening = False
                self.stop_event.set()
                
                # Wait for ongoing operations to complete
                time.sleep(0.5)
                
                # Close all active connections
                with self.connection_lock:
                    for conn_id in list(self.active_connections.keys()):
                        self._close_connection(conn_id)
                    self.active_connections.clear()
                
                # Clear message buffers
                self.receive_buffers.clear()
                
                # Wait for listener thread to complete
                if hasattr(self, 'listener_thread') and self.listener_thread.is_alive():
                    self.listener_thread.join(timeout=self.SHUTDOWN_TIMEOUT)
                
                # Properly close server socket
                if self.server_socket:
                    try:
                        # Shutdown socket first
                        self.server_socket.shutdown(socket.SHUT_RDWR)
                    except Exception:
                        pass  # Socket might already be closed
                    finally:
                        try:
                            self.server_socket.close()
                        except Exception:
                            pass
                        self.server_socket = None
                    
                self.log_info("MLLP endpoint stopped successfully")
                return True
                
        except Exception as e:
            self.log_error(f"Error stopping MLLP endpoint: {str(e)}")
            return False
        finally:
            self.server_socket = None
            self.listener_thread = None
            self.receive_buffers.clear()
            with self.connection_lock:
                self.active_connections.clear()

    def _close_connection(self, conn_id: str) -> None:
        """Safely close a client connection"""
        try:
            if conn_id in self.active_connections:
                # Connection cleanup handled in _handle_client finally block
                self.log_info(f"Closing connection {conn_id}")
        except Exception as e:
            self.log_error(f"Error closing connection {conn_id}: {str(e)}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive endpoint metrics"""
        metrics = super().get_metrics()
        
        with self.connection_lock:
            active_connections = len(self.active_connections)
            total_received = sum(c.messages_received for c in self.active_connections.values())
            total_errors = sum(c.errors for c in self.active_connections.values())
        
        metrics.update({
            "active_connections": active_connections,
            "total_messages_received": total_received,
            "messages_processed": self.metrics.get('messages_processed', 0),
            "messages_failed": self.metrics.get('messages_failed', 0),
            "parse_errors": self.metrics.get('parse_errors', 0),
            "protocol_errors": self.metrics.get('protocol_errors', 0),
            "connection_errors": self.metrics.get('connection_errors', 0),
            "total_errors": total_errors,
            "last_message_time": self.metrics.get('last_message_time'),
            "listening": self.listening
        })
        
        if self.config.is_test:
            try:
                test_status = self.test_client.get_stream_status(
                    self.config.organization_uuid
                )
                if test_status and test_status.get('status') == 'success':
                    metrics['test_service'] = test_status.get('data', {})
            except Exception as e:
                self.log_error(f"Error fetching test metrics: {str(e)}")
        
        return metrics

    def test_connection(self) -> Tuple[bool, str]:
        """Test connection capability"""
        try:
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            test_socket.bind((self.host, self.port))
            test_socket.close()
            return True, "Port available and configuration valid"
            
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"

    def cleanup(self) -> None:
        """Perform cleanup operations"""
        try:
            self.stop()
            
            # Clean up resources
            self.receive_buffers.clear()
            
            with self.connection_lock:
                self.active_connections.clear()
            
            # Clean up test client if applicable
            if self.config.is_test and self.test_client:
                try:
                    self.test_client.cleanup(self.config.organization_uuid)
                except Exception as e:
                    self.log_error(f"Error cleaning up test client: {str(e)}")
            
            self.log_info("Cleanup completed successfully")
        except Exception as e:
            self.log_error(f"Error during cleanup: {str(e)}")

    def __str__(self) -> str:
        """String representation"""
        return (f"MLLPEndpoint(uuid={self.config.uuid}, name={self.config.name}, "
                f"host={self.host}, port={self.port}, active={self.listening})")

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"MLLPEndpoint(uuid={self.config.uuid}, name={self.config.name}, "
                f"mode={self.config.mode}, message_type={self.config.message_type}, "
                f"endpoint_type={self.config.endpoint_type}, "
                f"organization_uuid={self.config.organization_uuid}, "
                f"host={self.host}, port={self.port}, "
                f"is_test={self.config.is_test}, active={self.listening})")