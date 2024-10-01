import socket
import threading
import time
from hl7apy import core
from hl7apy.exceptions import ParserError, UnsupportedVersion
from hl7apy.parser import parse_message
from app.extensions import mongo
from datetime import datetime
import logging
import uuid
import json

class HL7v2Interface:
    def __init__(self, stream_uuid, host, port, timeout=60, organization_uuid=None):
        self.stream_uuid = stream_uuid
        self.organization_uuid = organization_uuid
        self.host = host
        self.port = port
        self.timeout = timeout
        self.db = mongo.db
        self.messages_collection = self.db.messages
        self.logs_collection = self.db.logs
        
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(f'HL7v2Interface-{stream_uuid}')

        self.listening = False
        self.listener_thread = None
        self.server_socket = None
        self.stop_event = threading.Event()
        self.logger.info(f"HL7v2Interface {stream_uuid} initialized with host={host}, port={port}, timeout={timeout}")

    def _store_message(self, message, parsed=True):
        message_dict = {
            "uuid": str(uuid.uuid4()),
            "stream_uuid": self.stream_uuid,
            "organization_uuid": self.organization_uuid,
            "message": message.to_er7() if parsed and hasattr(message, 'to_er7') else message,
            "parsed": parsed,
            "timestamp": datetime.utcnow(),
            "type": "HL7v2",
            "conversion_status": "pending"
        }
        try:
            self.logger.debug(f"Attempting to store message: {message_dict['uuid']}")
            result = self.messages_collection.insert_one(message_dict)
            if result.acknowledged:
                self.logger.info(f"Message stored successfully: {message_dict['uuid']} (Parsed: {parsed}, Type: HL7v2)")
                self.logger.debug(f"Stored message details: {message_dict}")
            else:
                self.logger.error(f"Failed to store message in MongoDB: {message_dict['uuid']}")
        except Exception as e:
            self.logger.error(f"Error storing message in MongoDB: {e}")
            self.logger.debug(f"Message that failed to store: {message_dict}")

    def _store_log(self, level, message):
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
                self.logger.error(f"Failed to log message to MongoDB: {message}")
        except Exception as e:
            self.logger.error(f"Error logging to MongoDB: {e}")

    def log_info(self, message):
        self.logger.info(message)
        self._store_log(logging.INFO, message)

    def log_debug(self, message):
        self.logger.debug(message)
        self._store_log(logging.DEBUG, message)

    def log_error(self, message):
        self.logger.error(message)
        self._store_log(logging.ERROR, message)

    def start_listening(self):
        if not self.listening:
            self.listening = True
            self.stop_event.clear()
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.server_socket.settimeout(1)  # Set a short timeout for more responsive stopping
            self.listener_thread = threading.Thread(target=self._listen)
            self.listener_thread.start()
            self.log_info(f"Listening for HL7 messages on {self.host}:{self.port}")

    def _listen(self):
        while not self.stop_event.is_set():
            try:
                client_socket, client_address = self.server_socket.accept()
                with client_socket:
                    client_socket.settimeout(self.timeout)
                    data = client_socket.recv(1024).decode('utf-8')
                    self.log_info(f"Received data: {data}")
                    try:
                        message = parse_message(data, find_groups=False, validation_level=2)
                        self.log_info("Message parsed successfully.")
                        self._store_message(message, parsed=True)
                        ack = self._create_ack_from_parsed(message)
                        client_socket.sendall(ack.encode('utf-8'))
                        self.log_info("ACK sent successfully.")
                    except (ParserError, UnsupportedVersion) as e:
                        self.log_error(f"Error parsing message: {e}")
                        self._store_message(data, parsed=False)
                        ack = self._create_ack_from_raw(data)
                        client_socket.sendall(ack.encode('utf-8'))
                        self.log_info("ACK sent for raw message.")
            except socket.timeout:
                continue
            except Exception as e:
                self.log_error(f"Error handling client: {e}")

    def stop_listening(self):
        if self.listening:
            self.listening = False
            self.stop_event.set()
            self.listener_thread.join()
            self.server_socket.close()
            self.log_info("Stopped listening for HL7 messages")

    def _create_ack_from_parsed(self, message):
        msh = message.msh
        ack = parse_message("MSH|^~\\&|{0}|{1}|{2}|{3}|{4}||ACK^{5}|{6}|P|{7}".format(
            msh.msh_5.value, msh.msh_6.value, msh.msh_3.value, msh.msh_4.value,
            datetime.now().strftime('%Y%m%d%H%M%S'), msh.msh_9.value.split('^')[1],
            str(uuid.uuid4()), msh.msh_12.value
        ))
        ack.add_segment("MSA")
        ack.msa.msa_1 = "AA"
        ack.msa.msa_2 = msh.msh_10.value
        return ack.to_er7()

    def _create_ack_from_raw(self, raw_message):
        segments = raw_message.split('\r')
        msh = segments[0].split('|')
        
        sending_app = msh[2]
        sending_facility = msh[3]
        receiving_app = msh[4]
        receiving_facility = msh[5]
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        message_type = msh[8].split('^')[0]
        message_control_id = msh[9]
        version = msh[11] if len(msh) > 11 else '2.3'
        
        ack = (f"MSH|^~\\&|{receiving_app}|{receiving_facility}|{sending_app}|{sending_facility}|{timestamp}||ACK^{message_type}|{message_control_id}|P|{version}\r"
               f"MSA|AA|{message_control_id}\r")
        return ack

    def get_messages(self):
        try:
            messages = list(self.messages_collection.find({"stream_uuid": self.stream_uuid, "organization_uuid": self.organization_uuid}).sort('timestamp', -1))
            self.log_info(f"Retrieved {len(messages)} messages from MongoDB.")
            return messages
        except Exception as e:
            self.log_error(f"Failed to retrieve messages from MongoDB: {e}")
            return []

    def get_logs(self):
        try:
            logs = list(self.logs_collection.find({"stream_uuid": self.stream_uuid, "organization_uuid": self.organization_uuid}).sort('timestamp', -1))
            self.log_info(f"Retrieved {len(logs)} logs from MongoDB.")
            return logs
        except Exception as e:
            self.log_error(f"Failed to retrieve logs from MongoDB: {e}")
            return []