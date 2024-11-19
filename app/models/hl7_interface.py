import socket
import threading
import time
from typing import Optional, Dict, Any
from hl7apy import core
from hl7apy.exceptions import ParserError, UnsupportedVersion
from hl7apy.parser import parse_message
from app.extensions import mongo
from datetime import datetime
import logging
import uuid
from flask import current_app
from app.utils.logging_utils import log_message_cycle
import ssl
from queue import Queue, Full
from dataclasses import dataclass
from contextlib import contextmanager

@dataclass
class ConnectionMetrics:
    """Tracks connection-level metrics"""
    connected_at: datetime
    messages_received: int = 0
    messages_sent: int = 0
    errors: int = 0
    last_activity: datetime = None

class MLLPHandler:
    """Handles MLLP protocol wrapping/unwrapping"""
    
    VT = b'\x0b'  # Start block
    FS = b'\x1c'  # End block
    CR = b'\x0d'  # Carriage return
    
    @classmethod
    def wrap(cls, message: str) -> bytes:
        """Wrap message with MLLP characters"""
        return cls.VT + message.encode('utf-8') + cls.FS + cls.CR
    
    @classmethod
    def unwrap(cls, data: bytes) -> tuple[str, bool]:
        """Unwrap MLLP message and validate format"""
        is_valid = data.startswith(cls.VT) and cls.FS in data and data.endswith(cls.CR)
        if is_valid:
            return data[1:data.rfind(cls.FS)].decode('utf-8'), True
        return data.decode('utf-8'), False
    
    @classmethod
    def is_complete(cls, buffer: bytes) -> bool:
        """Check if buffer contains complete message"""
        return buffer.startswith(cls.VT) and cls.FS in buffer and buffer.endswith(cls.CR)

class HL7v2Interface:
    """HL7v2 interface with MLLP support"""
    
    # Configuration constants
    MAX_CONNECTIONS = 10
    MAX_MESSAGE_SIZE = 1024 * 1024  # 1MB
    MESSAGE_QUEUE_SIZE = 1000
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1.0  # seconds
    
    def __init__(self, 
                 stream_uuid: str, 
                 host: str, 
                 port: int, 
                 timeout: int = 60, 
                 organization_uuid: Optional[str] = None,
                 use_ssl: bool = False,
                 ssl_cert: Optional[str] = None,
                 ssl_key: Optional[str] = None):
        """Initialize HL7v2 interface"""
        
        # Basic configuration
        self.stream_uuid = stream_uuid
        self.organization_uuid = organization_uuid
        self.host = host
        self.port = port
        self.timeout = timeout
        
        # Database connections
        self.db = mongo.db
        self.messages_collection = self.db.messages
        self.logs_collection = self.db.logs
        
        # Setup logging
        self.logger = logging.getLogger(f'HL7v2Interface-{stream_uuid}')
        self.logger.setLevel(logging.DEBUG)
        
        # Connection management
        self.listening = False
        self.server_socket = None
        self.stop_event = threading.Event()
        self.active_connections: Dict[str, ConnectionMetrics] = {}
        self.connection_lock = threading.Lock()
        
        # Message handling
        self.mllp_handler = MLLPHandler()
        self.message_queue = Queue(maxsize=self.MESSAGE_QUEUE_SIZE)
        self.receive_buffers: Dict[str, bytearray] = {}
        
        # SSL configuration
        self.use_ssl = use_ssl
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        
        # Performance metrics
        self.metrics = {
            'messages_received': 0,
            'messages_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
        self.logger.info(
            f"HL7v2Interface {stream_uuid} initialized with "
            f"host={host}, port={port}, timeout={timeout}"
        )

    @contextmanager
    def _get_client_buffer(self, client_id: str) -> bytearray:
        """Get or create message buffer for client"""
        if client_id not in self.receive_buffers:
            self.receive_buffers[client_id] = bytearray()
        try:
            yield self.receive_buffers[client_id]
        finally:
            if len(self.receive_buffers[client_id]) > self.MAX_MESSAGE_SIZE:
                self.receive_buffers[client_id].clear()

    def start_listening(self):
        """Start listening for incoming connections"""
        if not self.listening:
            try:
                self.listening = True
                self.stop_event.clear()
                self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                
                # Configure SSL if enabled
                if self.use_ssl:
                    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                    context.load_cert_chain(self.ssl_cert, self.ssl_key)
                    self.server_socket = context.wrap_socket(
                        self.server_socket, 
                        server_side=True
                    )
                
                self.server_socket.bind((self.host, self.port))
                self.server_socket.listen(self.MAX_CONNECTIONS)
                self.server_socket.settimeout(1)
                
                self.metrics['start_time'] = datetime.utcnow()
                
                # Start listener thread
                self.listener_thread = threading.Thread(
                    target=self._listen,
                    name=f"HL7Listener-{self.stream_uuid}"
                )
                self.listener_thread.daemon = True
                self.listener_thread.start()
                
                self.log_info(f"Listening for HL7 messages on {self.host}:{self.port}")
                
            except Exception as e:
                self.listening = False
                self.log_error(f"Failed to start listening: {str(e)}")
                raise

    def _listen(self):
        """Main listener loop"""
        while not self.stop_event.is_set():
            try:
                client_socket, client_address = self.server_socket.accept()
                
                # Check connection limit
                with self.connection_lock:
                    if len(self.active_connections) >= self.MAX_CONNECTIONS:
                        client_socket.close()
                        self.log_warning("Maximum connections reached, rejecting new connection")
                        continue
                
                # Configure client socket
                client_socket.settimeout(self.timeout)
                client_id = str(uuid.uuid4())
                
                # Track connection
                self.active_connections[client_id] = ConnectionMetrics(
                    connected_at=datetime.utcnow()
                )
                
                # Handle client in separate thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, client_id, client_address),
                    name=f"HL7Client-{client_id}"
                )
                client_thread.daemon = True
                client_thread.start()
                
            except socket.timeout:
                continue
            except Exception as e:
                self.log_error(f"Error in listener loop: {str(e)}")
                time.sleep(1)

    def _handle_client(self, client_socket: socket.socket, client_id: str, address: tuple):
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
                            
                            # Check message completion
                            if self.mllp_handler.is_complete(buffer):
                                # Process message
                                message_data, is_valid_mllp = self.mllp_handler.unwrap(buffer)
                                if not is_valid_mllp:
                                    self.log_warning(f"Received non-MLLP message from {address}")
                                
                                self._process_message(message_data, client_socket)
                                buffer.clear()
                                
                                # Update metrics
                                with self.connection_lock:
                                    metrics = self.active_connections[client_id]
                                    metrics.messages_received += 1
                                    metrics.last_activity = datetime.utcnow()
                                
                    except socket.timeout:
                        continue
                        
        except Exception as e:
            self.log_error(f"Error handling client {client_id}: {str(e)}")
            with self.connection_lock:
                if client_id in self.active_connections:
                    self.active_connections[client_id].errors += 1
        finally:
            # Cleanup
            with self.connection_lock:
                self.active_connections.pop(client_id, None)
            self.receive_buffers.pop(client_id, None)

    def _process_message(self, message_data: str, client_socket: socket.socket):
        """Process received message and send ACK"""
        try:
            # Parse and validate message
            message = parse_message(message_data, find_groups=False, validation_level=2)
            self.log_info("Message parsed successfully")
            
            # Store message
            self._store_message(message, parsed=True)
            
            # Create and send ACK
            ack = self._create_ack_from_parsed(message)
            wrapped_ack = self.mllp_handler.wrap(ack)
            client_socket.sendall(wrapped_ack)
            self.log_info("ACK sent successfully")
            
        except (ParserError, UnsupportedVersion) as e:
            self.log_error(f"Error parsing message: {e}")
            self._store_message(message_data, parsed=False)
            
            # Send ACK for raw message
            ack = self._create_ack_from_raw(message_data)
            wrapped_ack = self.mllp_handler.wrap(ack)
            client_socket.sendall(wrapped_ack)
            self.log_info("ACK sent for raw message")

    def _store_message(self, message, parsed=True):
        """Store message with retry mechanism"""
        message_dict = {
            "uuid": str(uuid.uuid4()),
            "stream_uuid": self.stream_uuid,
            "organization_uuid": self.organization_uuid,
            "message": message.to_er7() if parsed and hasattr(message, 'to_er7') else message,
            "parsed": parsed,
            "timestamp": datetime.utcnow(),
            "type": "HL7v2",
            "conversion_status": "pending",
            "retry_count": 0
        }
        
        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                self.logger.debug(f"Attempting to store message: {message_dict['uuid']}")
                result = self.messages_collection.insert_one(message_dict)
                
                if result.acknowledged:
                    message_id = str(result.inserted_id)
                    self.logger.info(f"Message stored successfully: {message_id}")
                    log_message_cycle(
                        message_dict['uuid'], 
                        self.organization_uuid,
                        "message_received",
                        "HL7v2",
                        {},
                        "success"
                    )
                    
                    self._queue_message_for_conversion(message_id)
                    return True
                    
            except Exception as e:
                if attempt == self.RETRY_ATTEMPTS - 1:
                    self.log_error(f"Failed to store message after {self.RETRY_ATTEMPTS} attempts: {str(e)}")
                    log_message_cycle(
                        message_dict['uuid'],
                        self.organization_uuid,
                        "store_failed",
                        "HL7v2",
                        {"error": str(e)},
                        "error"
                    )
                else:
                    time.sleep(self.RETRY_DELAY)
        
        return False

    def _queue_message_for_conversion(self, message_id: str):
        """Queue message for conversion"""
        try:
            from app.tasks import generic_to_fhir_conversion
            
            task = generic_to_fhir_conversion.apply_async(
                args=[str(message_id), "HL7v2"],
                queue='hl7v2_conversion'
            )
            
            self.logger.info(f"Message {message_id} queued for conversion. Task ID: {task.id}")
            log_message_cycle(
                str(message_id),
                self.organization_uuid,
                "queued_for_conversion",
                "HL7v2",
                {"task_id": task.id},
                "success"
            )
            
        except Exception as e:
            self.log_error(f"Failed to queue message {message_id} for conversion: {str(e)}")
            log_message_cycle(
                str(message_id),
                self.organization_uuid,
                "queue_failed",
                "HL7v2",
                {"error": str(e)},
                "error"
            )
            raise

    def _create_ack_from_parsed(self, message) -> str:
        """Create ACK message from parsed HL7 message"""
        msh = message.msh
        ack = parse_message(
            "MSH|^~\\&|{0}|{1}|{2}|{3}|{4}||ACK^{5}|{6}|P|{7}".format(
                msh.msh_5.value, msh.msh_6.value, msh.msh_3.value, msh.msh_4.value,
                datetime.now().strftime('%Y%m%d%H%M%S'),
                msh.msh_9.value.split('^')[1],
                str(uuid.uuid4()),
                msh.msh_12.value
            )
        )
        ack.add_segment("MSA")
        ack.msa.msa_1 = "AA"  # Application Accept
        ack.msa.msa_2 = msh.msh_10.value  # Message Control ID
        return ack.to_er7()

    def _create_ack_from_raw(self, raw_message: str) -> str:
        """Create ACK message from raw HL7 message"""
        try:
            segments = raw_message.split('\r')
            msh = segments[0].split('|')
            
            if len(msh) < 12:
                self.log_warning("Invalid MSH segment length in raw message")
                # Provide minimal valid response
                return (
                    "MSH|^~\\&|RecApp|RecFac|SendApp|SendFac|"
                    f"{datetime.now().strftime('%Y%m%d%H%M%S')}||"
                    "ACK|{str(uuid.uuid4())}|P|2.5\r"
                    "MSA|AR|Unknown\r"
                )
            
            sending_app = msh[2]
            sending_facility = msh[3]
            receiving_app = msh[4]
            receiving_facility = msh[5]
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            try:
                message_type = msh[8].split('^')[0]
            except (IndexError, AttributeError):
                message_type = "Unknown"
            message_control_id = msh[9] if len(msh) > 9 else str(uuid.uuid4())
            version = msh[11] if len(msh) > 11 else '2.5'
            
            ack = (
                f"MSH|^~\\&|{receiving_app}|{receiving_facility}|{sending_app}|"
                f"{sending_facility}|{timestamp}||ACK^{message_type}|"
                f"{message_control_id}|P|{version}\r"
                f"MSA|AA|{message_control_id}\r"
            )
            return ack
            
        except Exception as e:
            self.log_error(f"Error creating ACK from raw message: {str(e)}")
            # Return safe fallback ACK
            return (
                "MSH|^~\\&|RecApp|RecFac|SendApp|SendFac|"
                f"{datetime.now().strftime('%Y%m%d%H%M%S')}||"
                "ACK|{str(uuid.uuid4())}|P|2.5\r"
                "MSA|AE|Unknown\r"
            )

    def stop_listening(self):
        """Stop listening and cleanup connections"""
        if self.listening:
            try:
                self.listening = False
                self.stop_event.set()
                
                # Close all active connections
                with self.connection_lock:
                    self.active_connections.clear()
                
                # Clear message buffers
                self.receive_buffers.clear()
                
                # Wait for listener thread to complete
                if self.listener_thread and self.listener_thread.is_alive():
                    self.listener_thread.join(timeout=5)
                
                # Close server socket
                if self.server_socket:
                    self.server_socket.close()
                    
                self.log_info("Stopped listening for HL7 messages")
                
            except Exception as e:
                self.log_error(f"Error stopping listener: {str(e)}")
            finally:
                self.server_socket = None
                self.listener_thread = None

    def get_messages(self, limit: int = 100, skip: int = 0) -> list:
        """Retrieve messages with pagination"""
        try:
            messages = list(
                self.messages_collection.find({
                    "stream_uuid": self.stream_uuid,
                    "organization_uuid": self.organization_uuid
                })
                .sort('timestamp', -1)
                .skip(skip)
                .limit(limit)
            )
            self.log_info(f"Retrieved {len(messages)} messages from MongoDB")
            return messages
        except Exception as e:
            self.log_error(f"Failed to retrieve messages from MongoDB: {e}")
            return []

    def get_logs(self, limit: int = 100, skip: int = 0) -> list:
        """Retrieve logs with pagination"""
        try:
            logs = list(
                self.logs_collection.find({
                    "stream_uuid": self.stream_uuid,
                    "organization_uuid": self.organization_uuid
                })
                .sort('timestamp', -1)
                .skip(skip)
                .limit(limit)
            )
            self.log_info(f"Retrieved {len(logs)} logs from MongoDB")
            return logs
        except Exception as e:
            self.log_error(f"Failed to retrieve logs from MongoDB: {e}")
            return []

    def get_metrics(self) -> Dict[str, Any]:
        """Get interface metrics"""
        with self.connection_lock:
            active_connections = len(self.active_connections)
            total_messages = sum(
                conn.messages_received 
                for conn in self.active_connections.values()
            )
            total_errors = sum(
                conn.errors 
                for conn in self.active_connections.values()
            )
        
        return {
            "active_connections": active_connections,
            "total_messages_received": total_messages,
            "total_errors": total_errors,
            "uptime": (
                datetime.utcnow() - self.metrics['start_time']
                if self.metrics['start_time']
                else None
            ),
            "listening": self.listening,
            "buffer_usage": len(self.receive_buffers)
        }

    def log_info(self, message: str):
        """Log info message"""
        self.logger.info(message)
        self._store_log(logging.INFO, message)

    def log_debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)
        self._store_log(logging.DEBUG, message)

    def log_error(self, message: str):
        """Log error message"""
        self.logger.error(message)
        self._store_log(logging.ERROR, message)

    def log_warning(self, message: str):
        """Log warning message"""
        self.logger.warning(message)
        self._store_log(logging.WARNING, message)

    def _store_log(self, level: int, message: str):
        """Store log entry in database"""
        log_entry = {
            "stream_uuid": self.stream_uuid,
            "organization_uuid": self.organization_uuid,
            "level": logging.getLevelName(level),
            "message": message,
            "timestamp": datetime.utcnow()
        }
        
        try:
            result = self.logs_collection.insert_one(log_entry)
            if not result.acknowledged:
                self.logger.error(f"Failed to store log in MongoDB: {message}")
        except Exception as e:
            self.logger.error(f"Error storing log in MongoDB: {e}")

    def health_check(self) -> Dict[str, Any]:
        """Perform health check"""
        status = {
            "status": "healthy",
            "listening": self.listening,
            "active_connections": len(self.active_connections),
            "last_message_time": None,
            "buffer_status": "ok"
        }

        # Check buffer sizes
        large_buffers = sum(
            1 for buf in self.receive_buffers.values()
            if len(buf) > self.MAX_MESSAGE_SIZE / 2
        )
        if large_buffers:
            status["buffer_status"] = f"warning: {large_buffers} large buffers"

        # Check last activity
        with self.connection_lock:
            last_activities = [
                m.last_activity for m in self.active_connections.values()
                if m.last_activity
            ]
            if last_activities:
                status["last_message_time"] = max(last_activities)

        return status