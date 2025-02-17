"""
MLLP Endpoint Implementation with robust state management and connection handling.
Handles HL7v2 messages over MLLP protocol with proper cleanup and monitoring.
"""

import socket
import threading
import time
from typing import Optional, Dict, Any, Tuple, List, ClassVar
from datetime import datetime
import uuid
import logging
from flask import current_app
from hl7apy.parser import parse_message
from hl7apy.exceptions import ParserError, UnsupportedVersion
from app.endpoints.base import BaseEndpoint
from app.endpoints.endpoint_types import EndpointConfig, EndpointStatus
from app.extensions import mongo
import ctypes
import os

# Core Constants
SOCKET_TIMEOUT = 1.0        # Socket timeout in seconds
CLEANUP_TIMEOUT = 2.0       # Cleanup timeout in seconds
SHUTDOWN_TIMEOUT = 5.0      # Maximum shutdown wait time
MAX_MESSAGE_SIZE = 1048576  # 1MB maximum message size
MAX_BUFFER_SIZE = 4096      # Socket buffer size for receives
MAX_RETRIES = 3            # Maximum retry attempts
RETRY_DELAY = 1.0          # Delay between retries
HEALTH_CHECK_INTERVAL = 5.0 # Health check interval
KEEPALIVE_INTERVAL = 30.0   # Keepalive interval
CONNECTION_TIMEOUT = 60.0   # Connection timeout
MAX_CONNECTIONS = 5         # Maximum concurrent connections

class MLLPException(Exception):
    """Base exception for MLLP-related errors"""
    pass

class MLLPProtocolError(MLLPException):
    """MLLP protocol specific errors"""
    pass

class MLLPConnectionError(MLLPException):
    """Connection-related errors"""
    pass

class MLLPStateError(MLLPException):
    """State management errors"""
    pass

class MLLPProtocol:
    """MLLP protocol implementation with robust message handling"""
    
    # Protocol characters
    VT = b'\x0b'  # Start block
    FS = b'\x1c'  # End block
    CR = b'\x0d'  # Carriage return

    @classmethod
    def wrap_message(cls, message: str) -> bytes:
        """Wrap message with MLLP characters"""
        if not message:
            raise MLLPProtocolError("Cannot wrap empty message")
        try:
            return cls.VT + message.encode('utf-8') + cls.FS + cls.CR
        except Exception as e:
            raise MLLPProtocolError(f"Error wrapping message: {str(e)}")

    @classmethod
    def unwrap_message(cls, data: bytes) -> str:
        """Unwrap MLLP message with validation"""
        try:
            if not data:
                raise MLLPProtocolError("Empty message received")
            if len(data) > MAX_MESSAGE_SIZE:
                raise MLLPProtocolError(f"Message exceeds maximum size of {MAX_MESSAGE_SIZE} bytes")
            if not data.startswith(cls.VT):
                raise MLLPProtocolError("Message does not start with VT")
            if not data.endswith(cls.CR) or data[-2:-1] != cls.FS:
                raise MLLPProtocolError("Message does not end with FS CR")
            
            return data[1:-2].decode('utf-8')
        except UnicodeDecodeError:
            raise MLLPProtocolError("Invalid message encoding")
        except Exception as e:
            raise MLLPProtocolError(f"Error unwrapping message: {str(e)}")

    @classmethod
    def split_messages(cls, buffer: bytearray) -> List[bytes]:
        """Split buffer into complete MLLP messages"""
        messages = []
        start_index = 0
        
        while True:
            try:
                # Find start of message
                vt_index = buffer.find(cls.VT, start_index)
                if vt_index == -1:
                    break
                
                # Find end of message
                fs_index = buffer.find(cls.FS, vt_index)
                if fs_index == -1:
                    break
                
                # Check for CR after FS
                if len(buffer) <= fs_index + 1 or buffer[fs_index + 1:fs_index + 2] != cls.CR:
                    start_index = fs_index + 1
                    continue
                
                # Extract complete message
                message = buffer[vt_index:fs_index + 2]
                if len(message) <= MAX_MESSAGE_SIZE:
                    messages.append(buffer[vt_index + 1:fs_index])
                start_index = fs_index + 2
                
            except Exception as e:
                raise MLLPProtocolError(f"Error splitting messages: {str(e)}")
        
        # Remove processed messages from buffer
        if start_index > 0:
            buffer[:start_index] = []
            
        return messages

    @classmethod
    def validate_message(cls, message: bytes) -> bool:
        """Validate MLLP message format"""
        try:
            if not message:
                return False
            if len(message) > MAX_MESSAGE_SIZE:
                return False
            if not message.startswith(cls.VT):
                return False
            if not message.endswith(cls.CR) or message[-2:-1] != cls.FS:
                return False
            # Try decoding content
            message[1:-2].decode('utf-8')
            return True
        except Exception:
            return False

class MLLPConnection:
    """
    Manages a single MLLP connection with state monitoring and keepalive.
    Acts as the primary connection management interface.
    """
    def __init__(self, socket: socket.socket, address: tuple, endpoint_uuid: str):
        self.socket = socket
        self.address = address
        self.endpoint_uuid = endpoint_uuid
        self.buffer = bytearray()
        self.last_activity = datetime.utcnow()
        self.creation_time = datetime.utcnow()
        self.last_keepalive = datetime.utcnow()
        self.is_alive = True
        self.error_count = 0
        self.messages_processed = 0
        self.bytes_received = 0
        self._lock = threading.Lock()
        self.monitor_thread = None
        self.logger = logging.getLogger(f"MLLPConnection-{endpoint_uuid}-{address}")

    def start_monitoring(self) -> None:
        """Start connection monitoring thread"""
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name=f"MLLPMonitor-{self.address}"
        )
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def _monitor_loop(self) -> None:
        """Monitor connection health and database state"""
        while self.is_alive:
            try:
                # Check database state
                endpoint_active = mongo.db.endpoints.find_one(
                    {
                        "uuid": self.endpoint_uuid,
                        "status": "ACTIVE",
                        "deleted": {"$ne": True}
                    },
                    {"_id": 1}
                )

                if not endpoint_active:
                    self.logger.info("Endpoint no longer active, terminating connection")
                    self.terminate()
                    break

                # Send keepalive if needed
                if self._should_send_keepalive():
                    self._send_keepalive()

                # Check for timeout
                if self._is_timed_out():
                    self.logger.warning("Connection timed out, terminating")
                    self.terminate()
                    break

                time.sleep(1)

            except Exception as e:
                self.logger.error(f"Monitor error: {str(e)}")
                self.terminate()
                break

    def _should_send_keepalive(self) -> bool:
        """Check if keepalive should be sent"""
        with self._lock:
            return (datetime.utcnow() - self.last_keepalive).total_seconds() > KEEPALIVE_INTERVAL

    def _send_keepalive(self) -> None:
        """Send keepalive message"""
        try:
            with self._lock:
                keepalive = MLLPProtocol.wrap_message("PING")
                self.socket.sendall(keepalive)
                self.last_keepalive = datetime.utcnow()
                self.last_activity = datetime.utcnow()
        except Exception as e:
            self.logger.error(f"Keepalive failed: {str(e)}")
            self.terminate()

    def _is_timed_out(self) -> bool:
        """Check if connection has timed out"""
        with self._lock:
            return (datetime.utcnow() - self.last_activity).total_seconds() > CONNECTION_TIMEOUT

    def terminate(self) -> None:
        """Terminate connection with cleanup"""
        self.is_alive = False
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except:
            pass
        try:
            self.socket.close()
        except:
            pass

    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        with self._lock:
            return {
                'address': self.address,
                'messages_processed': self.messages_processed,
                'bytes_received': self.bytes_received,
                'error_count': self.error_count,
                'last_activity': self.last_activity.isoformat(),
                'uptime': (datetime.utcnow() - self.creation_time).total_seconds(),
                'is_alive': self.is_alive
            }
class MLLPEndpoint(BaseEndpoint):
    """
    MLLP endpoint implementation with comprehensive state management
    and connection handling.
    """
    # Class-level state tracking
    _active_endpoints: ClassVar[Dict[str, 'MLLPEndpoint']] = {}
    _state_lock = threading.Lock()

    def __init__(self, config: EndpointConfig, **kwargs):
        """Initialize MLLP endpoint"""
        super().__init__(config=config)
        
        # Validate and store configuration
        self._validate_config(kwargs)
        self.host = kwargs['host']
        self.port = int(kwargs['port'])
        self.timeout = float(kwargs.get('timeout', SOCKET_TIMEOUT))
        
        # Core components
        self.server_socket = None
        self.active_connections: Dict[str, MLLPConnection] = {}
        self.connection_lock = threading.Lock()
        
        # Thread management
        self.listener_thread = None
        self.monitor_thread = None
        self.cleanup_thread = None
        self.is_running = False
        self.shutdown_event = threading.Event()
        
        # State management
        self.status_lock = threading.Lock()
        self.recovery_mode = False
        self.last_health_check = datetime.utcnow()
        
        # Performance tracking
        self.stats = {
            'start_time': None,
            'total_connections': 0,
            'active_connections': 0,
            'messages_received': 0,
            'messages_processed': 0,
            'messages_failed': 0,
            'bytes_received': 0,
            'last_message_time': None,
            'errors': 0
        }
        
        self.log_info(f"[MLLPEndpoint.__init__] Initialized endpoint {self.config.uuid} "
                     f"for {self.host}:{self.port}")

    @classmethod
    def purge_endpoint(cls, uuid: str) -> None:
        """Purge endpoint instance from memory"""
        with cls._state_lock:
            if uuid in cls._active_endpoints:
                endpoint = cls._active_endpoints[uuid]
                endpoint.force_cleanup()
                cls._active_endpoints.pop(uuid)

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate endpoint configuration"""
        required_fields = ['host', 'port']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")

        # Validate port
        try:
            port = int(config['port'])
            if not 1 <= port <= 65535:
                raise ValueError(f"Invalid port number: {port}")
        except (ValueError, TypeError):
            raise ValueError(f"Invalid port value: {config.get('port')}")

        # Validate host
        host = config['host']
        if not isinstance(host, str) or not host:
            raise ValueError("Invalid host value")

        # Validate timeout
        timeout = config.get('timeout', SOCKET_TIMEOUT)
        try:
            timeout = float(timeout)
            if timeout <= 0:
                raise ValueError("Timeout must be positive")
        except (ValueError, TypeError):
            raise ValueError(f"Invalid timeout value: {timeout}")

    def _initialize_socket(self) -> bool:
        """Initialize server socket with proper error handling"""
        for attempt in range(MAX_RETRIES):
            try:
                if self.server_socket:
                    self.log_warning("Socket already initialized, closing existing socket")
                    self._cleanup_socket()

                # Create new socket
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                
                # Set timeout for accept() to prevent blocking
                self.server_socket.settimeout(self.timeout)

                # Set TCP keepalive
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                
                # Bind and listen
                self.server_socket.bind((self.host, self.port))
                self.server_socket.listen(MAX_CONNECTIONS)
                
                self.log_info(f"Socket initialized on {self.host}:{self.port}")
                return True

            except Exception as e:
                self.log_error(f"Socket initialization attempt {attempt + 1} failed: {str(e)}")
                self._cleanup_socket()
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    return False

        return False

    def _cleanup_socket(self) -> None:
        """Force cleanup server socket"""
        if self.server_socket:
            try:
                # Try to unblock accept() by connecting
                try:
                    temp_socket = socket.socket()
                    temp_socket.settimeout(1.0)
                    temp_socket.connect((self.host, self.port))
                    temp_socket.close()
                except:
                    pass

                # Shutdown socket
                try:
                    self.server_socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                
                # Close socket
                try:
                    self.server_socket.close()
                except:
                    pass
                
                # Force close if still open
                try:
                    if self.server_socket.fileno() != -1:
                        os.close(self.server_socket.fileno())
                except:
                    pass
            finally:
                self.server_socket = None

    def start(self) -> bool:
        """Start endpoint operations with state management"""
        try:
            self.log_info(f"Starting endpoint {self.config.uuid}")
            
            with self.status_lock:
                # Check database state first
                endpoint = self.db.endpoints.find_one({
                    "uuid": self.config.uuid,
                    "deleted": {"$ne": True}
                })
                
                if not endpoint:
                    raise ValueError("Endpoint does not exist in database")

                if self.is_running:
                    self.log_info("Endpoint already running")
                    return True

                # Clear shutdown event
                self.shutdown_event.clear()
                
                # Initialize socket
                if not self._initialize_socket():
                    self.log_error("Socket initialization failed")
                    return False

                # Update database status first
                self.db.endpoints.update_one(
                    {"uuid": self.config.uuid},
                    {
                        "$set": {
                            "status": "ACTIVE",
                            "last_active": datetime.utcnow(),
                            "updated_at": datetime.utcnow()
                        }
                    }
                )

                # Set running state and register endpoint
                self.is_running = True
                self.stats['start_time'] = datetime.utcnow()
                with self._state_lock:
                    self._active_endpoints[self.config.uuid] = self

                # Start all threads
                self._start_threads()

                self.log_info(f"Successfully started MLLP endpoint on {self.host}:{self.port}")
                return True

        except Exception as e:
            self.log_error(f"Error starting endpoint: {str(e)}")
            self._cleanup_resources()
            self._set_error_status()
            return False

    def _start_threads(self) -> None:
        """Start all required endpoint threads"""
        # Start listener thread
        self.listener_thread = threading.Thread(
            target=self._listen_loop,
            name=f"MLLPListener-{self.config.uuid}"
        )
        self.listener_thread.daemon = True
        self.listener_thread.start()

        # Start monitor thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name=f"MLLPMonitor-{self.config.uuid}"
        )
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

        # Start cleanup thread
        self.cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            name=f"MLLPCleanup-{self.config.uuid}"
        )
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()

    def _monitor_loop(self) -> None:
        """Monitor endpoint health and state"""
        while self.is_running and not self.shutdown_event.is_set():
            try:
                # Check database state
                endpoint = self.db.endpoints.find_one({
                    "uuid": self.config.uuid,
                    "status": "ACTIVE",
                    "deleted": {"$ne": True}
                })

                if not endpoint:
                    self.log_info("Endpoint no longer active in database, stopping")
                    self.force_cleanup()
                    break

                # Update health check timestamp
                self.last_health_check = datetime.utcnow()

                time.sleep(HEALTH_CHECK_INTERVAL)
            except Exception as e:
                self.log_error(f"Monitor error: {str(e)}")
                time.sleep(1)

    def _cleanup_loop(self) -> None:
        """Cleanup dead connections and resources"""
        while self.is_running and not self.shutdown_event.is_set():
            try:
                self._cleanup_idle_connections()
                time.sleep(CLEANUP_TIMEOUT)
            except Exception as e:
                self.log_error(f"Cleanup error: {str(e)}")
                time.sleep(1)
    
    def _listen_loop(self) -> None:
        """Main connection acceptance loop with state validation"""
        self.log_info(f"Starting listener loop for {self.config.uuid}")
        
        while self.is_running and not self.shutdown_event.is_set():
            try:
                # Check database state before accepting
                endpoint_state = self.db.endpoints.find_one(
                    {
                        "uuid": self.config.uuid,
                        "status": "ACTIVE",
                        "deleted": {"$ne": True}
                    },
                    {"_id": 1}
                )

                if not endpoint_state:
                    self.log_info("Endpoint not active in database, stopping listener")
                    break

                try:
                    client_socket, address = self.server_socket.accept()
                except socket.timeout:
                    continue
                except socket.error as e:
                    if self.is_running and not self.shutdown_event.is_set():
                        self.log_error(f"Socket error: {str(e)}")
                    continue

                # Double-check state after accept
                if not self.is_running or self.shutdown_event.is_set():
                    try:
                        client_socket.close()
                    except:
                        pass
                    break

                # Create MLLP connection
                conn_id = str(uuid.uuid4())
                mllp_conn = MLLPConnection(client_socket, address, self.config.uuid)
                
                with self.connection_lock:
                    # Check connection limit
                    if len(self.active_connections) >= MAX_CONNECTIONS:
                        self.log_warning(f"Connection limit reached, rejecting {address}")
                        client_socket.close()
                        continue
                        
                    self.active_connections[conn_id] = mllp_conn
                    self.stats['total_connections'] += 1
                    self.stats['active_connections'] = len(self.active_connections)

                # Start connection monitoring
                mllp_conn.start_monitoring()

                # Start client handler thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(conn_id,),
                    name=f"MLLPClient-{conn_id}"
                )
                client_thread.daemon = True
                client_thread.start()

                self.log_info(f"Started handler for {address}")

            except Exception as e:
                if self.is_running and not self.shutdown_event.is_set():
                    self.log_error(f"Error in listener loop: {str(e)}")
                    time.sleep(1)

        self.log_info("Listener loop exited")
        self.force_cleanup()

    def _handle_client(self, conn_id: str) -> None:
        """Handle client connection with state validation"""
        mllp_conn = self.active_connections.get(conn_id)
        if not mllp_conn:
            return

        try:
            while mllp_conn.is_alive and not self.shutdown_event.is_set():
                try:
                    # Check database state before processing
                    endpoint_state = self.db.endpoints.find_one(
                        {
                            "uuid": self.config.uuid,
                            "status": "ACTIVE",
                            "deleted": {"$ne": True}
                        },
                        {"_id": 1}
                    )

                    if not endpoint_state:
                        self.log_info(f"Endpoint not active, stopping connection {conn_id}")
                        break

                    # Receive data
                    data = self._receive_data(mllp_conn)
                    if not data:
                        continue

                    # Process complete messages
                    messages = MLLPProtocol.split_messages(data)
                    for message in messages:
                        if not mllp_conn.is_alive or self.shutdown_event.is_set():
                            return

                        try:
                            self._process_message(message, mllp_conn)
                            mllp_conn.messages_processed += 1
                            mllp_conn.last_activity = datetime.utcnow()
                            
                        except Exception as msg_error:
                            self.log_error(f"Error processing message: {str(msg_error)}")
                            mllp_conn.error_count += 1
                            continue

                except socket.timeout:
                    continue
                except socket.error as e:
                    self.log_error(f"Socket error for client {conn_id}: {str(e)}")
                    break
                except Exception as e:
                    self.log_error(f"Error handling client {conn_id}: {str(e)}")
                    break

        except Exception as e:
            self.log_error(f"Fatal error in client handler {conn_id}: {str(e)}")
        finally:
            self._close_connection(conn_id)

    def _receive_data(self, conn: MLLPConnection) -> Optional[bytearray]:
        """Receive data from connection with size validation"""
        try:
            chunk = conn.socket.recv(MAX_BUFFER_SIZE)
            if not chunk:
                return None

            conn.buffer.extend(chunk)
            conn.bytes_received += len(chunk)
            self.stats['bytes_received'] += len(chunk)

            # Check buffer size limit
            if len(conn.buffer) > MAX_MESSAGE_SIZE:
                conn.buffer.clear()
                raise MLLPProtocolError("Message size limit exceeded")

            return conn.buffer

        except socket.timeout:
            return None
        except Exception as e:
            self.log_error(f"Error receiving data: {str(e)}")
            return None

    def _process_message(self, message: bytes, conn: MLLPConnection) -> None:
        """Process received message with acknowledgment"""
        message_uuid = str(uuid.uuid4())
        self.log_info(f"Processing message {message_uuid}")

        try:
            # Check database state before processing
            endpoint_state = self.db.endpoints.find_one(
                {
                    "uuid": self.config.uuid,
                    "status": "ACTIVE",
                    "deleted": {"$ne": True}
                },
                {"_id": 1}
            )

            if not endpoint_state:
                return

            # Decode and validate message
            message_str = message.decode('utf-8')
            
            # Parse message
            try:
                parsed_message = parse_message(message_str, find_groups=False, validation_level=2)
                parsing_successful = True
                error_message = None
            except (ParserError, UnsupportedVersion) as e:
                parsing_successful = False
                error_message = str(e)

            # Store message
            stored = self._store_message(
                message_uuid=message_uuid,
                message=message_str,
                parsed=parsing_successful,
                error=error_message,
                conn=conn
            )

            if stored:
                if parsing_successful:
                    # Send acknowledgment
                    ack = self._create_ack(parsed_message)
                    self._send_response(conn.socket, ack)
                    self.stats['messages_received'] += 1
                else:
                    # Send negative acknowledgment
                    nack = self._create_nack(message_str, error_message)
                    self._send_response(conn.socket, nack)
                    self.stats['messages_failed'] += 1

                self.stats['last_message_time'] = datetime.utcnow()

        except Exception as e:
            self.log_error(f"Error processing message {message_uuid}: {str(e)}")
            try:
                # Send minimal error acknowledgment
                error_nack = self._create_minimal_ack("AE", str(e)[:80])
                self._send_response(conn.socket, error_nack)
            except Exception as nack_error:
                self.log_error(f"Failed to send error NACK: {str(nack_error)}")

    def _store_message(self, message_uuid: str, message: str, parsed: bool = False, 
                      error: Optional[str] = None, conn: Optional[MLLPConnection] = None) -> bool:
        """Store message in database with metadata"""
        try:
            message_dict = {
                "uuid": message_uuid,
                "endpoint_uuid": self.config.uuid,
                "organization_uuid": self.config.organization_uuid,
                "message": message,
                "parsed": parsed,
                "parsing_error": error,
                "timestamp": datetime.utcnow(),
                "type": "HL7v2",
                "status": "received",
                "metadata": {
                    "source_host": self.host,
                    "source_port": self.port,
                    "client_address": conn.address if conn else None,
                    "parsed_status": "success" if parsed else "failed",
                    "message_size": len(message)
                }
            }
            
            result = self.messages_collection.insert_one(message_dict)
            return result.acknowledged
            
        except Exception as e:
            self.log_error(f"Failed to store message {message_uuid}: {str(e)}")
            return False

    def _cleanup_idle_connections(self) -> None:
        """Clean up idle and dead connections"""
        current_time = datetime.utcnow()
        to_close = []

        with self.connection_lock:
            for conn_id, conn in self.active_connections.items():
                try:
                    # Check connection health
                    if not conn.is_alive or (current_time - conn.last_activity).total_seconds() > CONNECTION_TIMEOUT:
                        to_close.append(conn_id)
                except Exception as e:
                    self.log_error(f"Error checking connection {conn_id}: {str(e)}")
                    to_close.append(conn_id)

        # Close identified connections
        for conn_id in to_close:
            self._close_connection(conn_id)

    def _close_connection(self, conn_id: str) -> None:
        """Close a specific connection with cleanup"""
        with self.connection_lock:
            conn = self.active_connections.pop(conn_id, None)
            
        if conn:
            conn.terminate()  # This handles socket shutdown and close
            
            # Update stats
            self.stats['active_connections'] = len(self.active_connections)
            self.log_info(f"Connection {conn_id} closed")

    def _close_all_connections(self) -> None:
        """Close all active connections"""
        conn_ids = list(self.active_connections.keys())
        for conn_id in conn_ids:
            self._close_connection(conn_id)

    def _create_ack(self, message) -> str:
        """Create acknowledgment message"""
        try:
            # Extract message details
            control_id = message.msh.message_control_id.value
            sending_app = message.msh.sending_application.value
            sending_facility = message.msh.sending_facility.value
            message_type = message.msh.message_type.value

            # Build ACK
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            ack = (
                f"MSH|^~\\&|SWITCHYARD|SWITCHYARD|{sending_app}|{sending_facility}|"
                f"{timestamp}||ACK^{message_type}|{control_id}_ACK|P|2.5\r"
                f"MSA|AA|{control_id}|Message received successfully\r"
            )
            return ack
        except Exception as e:
            self.log_error(f"Error creating ACK: {str(e)}")
            return self._create_minimal_ack("AA")
    
    def _create_nack(self, message: str, error: str) -> str:
        """Create negative acknowledgment"""
        try:
            # Extract MSH segment
            msh_segment = next(line for line in message.split('\r') if line.startswith('MSH'))
            segments = msh_segment.split('|')
            
            # Extract required fields
            sending_app = segments[2] if len(segments) > 2 else 'UNKNOWN'
            sending_facility = segments[3] if len(segments) > 3 else 'UNKNOWN'
            try:
                message_type = segments[8].split('^')[0] if len(segments) > 8 else 'UNKNOWN'
            except:
                message_type = 'UNKNOWN'
            try:
                control_id = segments[9] if len(segments) > 9 else datetime.now().strftime('%Y%m%d%H%M%S')
            except:
                control_id = datetime.now().strftime('%Y%m%d%H%M%S')

            # Build NACK
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            nack = (
                f"MSH|^~\\&|SWITCHYARD|SWITCHYARD|{sending_app}|{sending_facility}|"
                f"{timestamp}||ACK^{message_type}|{control_id}_NACK|P|2.5\r"
                f"MSA|AE|{control_id}|{error[:80]}\r"  # Truncate error if too long
            )
            return nack

        except Exception as e:
            self.log_error(f"Error creating NACK: {str(e)}")
            return self._create_minimal_ack("AE", "Error processing message")

    def _create_minimal_ack(self, ack_code: str, message: str = "") -> str:
        """Create minimal acknowledgment for error cases"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        control_id = f"MIN_{timestamp}"
        
        minimal_ack = (
            f"MSH|^~\\&|SWITCHYARD|SWITCHYARD|UNKNOWN|UNKNOWN|{timestamp}||"
            f"ACK|{control_id}|P|2.5\r"
            f"MSA|{ack_code}|{control_id}|{message}\r"
        )
        return minimal_ack

    def _send_response(self, socket: socket.socket, message: str) -> None:
        """Send MLLP response with error handling"""
        try:
            wrapped_message = MLLPProtocol.wrap_message(message)
            socket.sendall(wrapped_message)
        except Exception as e:
            raise MLLPProtocolError(f"Error sending response: {str(e)}")

    def force_cleanup(self) -> None:
        """Force cleanup of all resources"""
        if getattr(self, '_cleaning_up', False):
            self.log_warning("Cleanup already in progress, skipping")
            return

        try:
            self._cleaning_up = True
            self.log_info("Starting force cleanup")
            
            # Set flags to stop operations
            self.is_running = False
            self.shutdown_event.set()
            
            # Close all connections first
            self._close_all_connections()

            # Force close server socket
            if self.server_socket:
                try:
                    # Create temporary connection to unblock accept()
                    temp_socket = socket.socket()
                    temp_socket.settimeout(1.0)
                    try:
                        temp_socket.connect((self.host, self.port))
                    except:
                        pass
                    finally:
                        temp_socket.close()
                except:
                    pass

                # Force socket cleanup
                self._cleanup_socket()

            # Force terminate threads
            self._cleanup_threads()

            # Remove from active endpoints
            with self._state_lock:
                self._active_endpoints.pop(self.config.uuid, None)

            self.log_info("Force cleanup completed")

        except Exception as e:
            self.log_error(f"Error during force cleanup: {str(e)}")
            self._set_error_status()
        finally:
            self._cleaning_up = False

    def _cleanup_threads(self) -> None:
        """Clean up all threads with force termination if needed"""
        current_thread_id = threading.get_ident()
        threads = [
            (self.listener_thread, "Listener"),
            (self.monitor_thread, "Monitor"),
            (self.cleanup_thread, "Cleanup")
        ]
        
        for thread, name in threads:
            if thread and thread.is_alive():
                # Don't try to join the current thread
                if thread.ident == current_thread_id:
                    self.log_warning(f"Skipping {name} thread - cannot join current thread")
                    continue

                self.log_info(f"Waiting for {name} thread to finish...")
                thread.join(timeout=CLEANUP_TIMEOUT)
                
                # Force terminate if still alive
                if thread.is_alive():
                    self.log_warning(f"{name} thread did not finish cleanly, force terminating")
                    try:
                        thread_id = thread.ident
                        if thread_id and thread_id != current_thread_id:
                            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                                ctypes.c_long(thread_id),
                                ctypes.py_object(SystemExit)
                            )
                    except:
                        pass

        # Clear thread references
        self.listener_thread = None
        self.monitor_thread = None
        self.cleanup_thread = None

    def _set_error_status(self) -> None:
        """Update endpoint status to error"""
        try:
            self.db.endpoints.update_one(
                {"uuid": self.config.uuid},
                {
                    "$set": {
                        "status": "ERROR",
                        "updated_at": datetime.utcnow(),
                        "last_error": datetime.utcnow().isoformat(),
                        "last_error_message": "Endpoint encountered an error"
                    }
                }
            )
        except Exception as e:
            self.log_error(f"Error updating error status: {str(e)}")

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive endpoint status"""
        try:
            with self.connection_lock:
                active_connections = len(self.active_connections)
                connection_stats = [
                    conn.get_stats() for conn in self.active_connections.values()
                ]

            status = {
                "uuid": self.config.uuid,
                "name": self.config.name,
                "status": "ACTIVE" if self.is_running else "INACTIVE",
                "host": self.host,
                "port": self.port,
                "uptime": (datetime.utcnow() - self.stats['start_time']).total_seconds() 
                         if self.stats['start_time'] else 0,
                "total_connections": self.stats['total_connections'],
                "active_connections": active_connections,
                "connection_stats": connection_stats,
                "messages_received": self.stats['messages_received'],
                "messages_failed": self.stats['messages_failed'],
                "bytes_received": self.stats['bytes_received'],
                "last_message_time": self.stats['last_message_time'].isoformat() 
                                   if self.stats['last_message_time'] else None,
                "errors": self.stats['errors'],
                "last_health_check": self.last_health_check.isoformat(),
                "endpoint_type": "MLLP",
                "mode": self.config.mode,
                "organization_uuid": self.config.organization_uuid
            }
            return status

        except Exception as e:
            self.log_error(f"Error getting status: {str(e)}")
            return {
                "uuid": self.config.uuid,
                "status": "ERROR",
                "error": str(e)
            }

    def test_connection(self) -> Tuple[bool, str]:
        """Test endpoint connectivity"""
        try:
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.settimeout(self.timeout)
            test_socket.connect((self.host, self.port))
            test_socket.close()
            return True, "Connection test successful"
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"

    def cleanup(self) -> None:
        """Implementation of abstract cleanup method"""
        try:
            self.log_info(f"Starting cleanup for endpoint {self.config.uuid}")
            
            # Stop if running
            if self.is_running:
                self.stop()
            
            # Force cleanup resources
            self.force_cleanup()
            
            # Unregister from manager
            with self._state_lock:
                self._active_endpoints.pop(self.config.uuid, None)
            
            self.log_info("Cleanup completed successfully")
            
        except Exception as e:
            self.log_error(f"Error during cleanup: {str(e)}")
            raise
    
    def deliver_message(self, message_content: Dict[str, Any], metadata: Optional[Dict] = None) -> Tuple[bool, Optional[str]]:
        """
        Deliver message over MLLP connection
        
        Args:
            message_content: Message content to deliver
            metadata: Optional metadata for message delivery
            
        Returns:
            Tuple[bool, str]: (success, error_message)
        """
        try:
            if not isinstance(message_content, dict):
                return False, "Message content must be a dictionary"
                
            # Extract HL7 message from content
            hl7_message = message_content.get('message')
            if not hl7_message:
                return False, "No HL7 message found in content"
                
            # Create connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            
            try:
                # Connect to destination
                sock.connect((self.host, self.port))
                
                # Wrap message in MLLP format
                wrapped_message = MLLPProtocol.wrap_message(hl7_message)
                
                # Send message
                sock.sendall(wrapped_message)
                
                # Wait for acknowledgment
                buffer = sock.recv(MAX_BUFFER_SIZE)
                if not buffer:
                    return False, "No acknowledgment received"
                    
                # Process acknowledgment
                try:
                    ack = MLLPProtocol.unwrap_message(buffer)
                    if "MSA|AA|" in ack:
                        self.log_info(f"Message delivered successfully with ACK: {ack}")
                        return True, None
                    else:
                        self.log_warning(f"Received NACK: {ack}")
                        return False, f"Negative acknowledgment received: {ack}"
                except MLLPProtocolError as e:
                    return False, f"Invalid acknowledgment format: {str(e)}"
                    
            finally:
                sock.close()
                
        except socket.error as e:
            return False, f"Socket error: {str(e)}"
        except Exception as e:
            return False, f"Delivery error: {str(e)}"

    def validate_message_format(self, message_content: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate HL7 message format
        
        Args:
            message_content: Message content to validate
            
        Returns:
            Tuple[bool, str]: (is_valid, error_message)
        """
        try:
            # Validate content type
            if not isinstance(message_content, dict):
                return False, "Message content must be a dictionary"
                
            # Check required fields
            required_fields = ['message', 'message_type']
            missing_fields = [field for field in required_fields if field not in message_content]
            if missing_fields:
                return False, f"Missing required fields: {', '.join(missing_fields)}"
                
            # Get message
            message = message_content['message']
            if not isinstance(message, str):
                return False, "Message must be a string"
                
            # Basic HL7 validation
            if not message.startswith('MSH|'):
                return False, "Invalid HL7 message format - must start with MSH segment"
                
            # Validate message type
            try:
                segments = message.split('\r')
                msh = segments[0].split('|')
                if len(msh) < 9:
                    return False, "Invalid MSH segment - missing required fields"
                    
                message_type = msh[8].split('^')[0]
                if not message_type:
                    return False, "Missing message type in MSH segment"
                    
            except Exception as e:
                return False, f"Error parsing message segments: {str(e)}"
                
            # Attempt full parsing if hl7apy is available
            try:
                from hl7apy.parser import parse_message
                parse_message(message, find_groups=False, validation_level=2)
            except ImportError:
                self.log_warning("hl7apy not available for detailed message validation")
            except Exception as e:
                return False, f"Message parsing failed: {str(e)}"
                
            return True, None
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def __str__(self) -> str:
        """String representation"""
        return (f"MLLPEndpoint(uuid={self.config.uuid}, "
                f"host={self.host}, port={self.port}, running={self.is_running})")

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"MLLPEndpoint(uuid={self.config.uuid}, name={self.config.name}, "
                f"host={self.host}, port={self.port}, running={self.is_running}, "
                f"connections={len(self.active_connections)})")