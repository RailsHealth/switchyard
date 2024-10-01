import paramiko
import os
import uuid
from datetime import datetime
import logging
from app.extensions import mongo
from config import Config
import time
import re
from app.models.base_interface import BaseInterface
import threading

class SFTPInterface(BaseInterface):
    def __init__(self, stream_uuid, host, port, username, password=None, private_key=None, remote_path=None, file_pattern=None, fetch_interval=None):
        super().__init__(stream_uuid)
        elf.organization_uuid = organization_uuid
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.private_key = private_key
        self.remote_path = remote_path
        self.file_pattern = file_pattern if file_pattern else ".*"
        self.fetch_interval = fetch_interval or Config.DEFAULT_SFTP_FETCH_INTERVAL

        self.sftp = None
        self.transport = None
        self.is_fetching = False
        self.fetch_thread = None
        self.stop_event = threading.Event()

    def connect(self):
        retry_count = 0
        while retry_count < Config.SFTP_RETRY_ATTEMPTS:
            try:
                self.transport = paramiko.Transport((self.host, self.port))
                if self.private_key:
                    key = paramiko.RSAKey.from_private_key_file(self.private_key)
                    self.transport.connect(username=self.username, pkey=key)
                else:
                    self.transport.connect(username=self.username, password=self.password)
                self.sftp = paramiko.SFTPClient.from_transport(self.transport)
                self.logger.info(f"Connected to SFTP server: {self.host}")
                return True
            except Exception as e:
                self.logger.error(f"SFTP connection error (attempt {retry_count + 1}): {str(e)}")
                retry_count += 1
                time.sleep(Config.SFTP_RETRY_DELAY)
        return False

    def disconnect(self):
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()
        self.logger.info(f"Disconnected from SFTP server: {self.host}")

    def start_fetching(self):
        if not self.is_fetching:
            self.is_fetching = True
            self.stop_event.clear()
            self.fetch_thread = threading.Thread(target=self._fetch_loop)
            self.fetch_thread.start()
            self.logger.info(f"Started fetching files from SFTP server: {self.host}")
            self._store_log(logging.INFO, f"Started fetching files from SFTP server: {self.host}")

    def stop_fetching(self):
        if self.is_fetching:
            self.is_fetching = False
            self.stop_event.set()
            if self.fetch_thread:
                self.fetch_thread.join(timeout=30)  # Wait for up to 30 seconds for the thread to finish
            self.logger.info(f"Stopped fetching files from SFTP server: {self.host}")
            self._store_log(logging.INFO, f"Stopped fetching files from SFTP server: {self.host}")

    def _fetch_loop(self):
        while not self.stop_event.is_set():
            self.fetch_files()
            # Wait for the configured interval or until the stop event is set
            self.stop_event.wait(timeout=self.fetch_interval)

    def fetch_files(self):
        if not self.connect():
            self.logger.error("Failed to connect to SFTP server after multiple attempts")
            return

        try:
            remote_files = self.sftp.listdir(self.remote_path)
            for filename in remote_files:
                if self.stop_event.is_set():
                    break

                if not re.match(self.file_pattern, filename):
                    continue

                remote_file_path = os.path.join(self.remote_path, filename)
                local_file_path = os.path.join(Config.STORAGE_PATH, filename)

                # Check if file already processed
                if mongo.db.messages.find_one({"filename": filename, "stream_uuid": self.stream_uuid}):
                    self.logger.info(f"File {filename} already processed, skipping")
                    continue

                # Download file
                self.sftp.get(remote_file_path, local_file_path)

                # Create message entry
                message_uuid = str(uuid.uuid4())
                message = {
                    "uuid": message_uuid,
                    "stream_uuid": self.stream_uuid,
                    "organization_uuid": self.organization_uuid,
                    "filename": filename,
                    "local_path": local_file_path,
                    "timestamp": datetime.utcnow(),
                    "parsed": False,
                    "type": self._get_message_type(),
                    "parsing_status": "pending",
                    "conversion_status": "pending"
                }
                mongo.db.messages.insert_one(message)

                self.logger.info(f"File {filename} downloaded and message created")
                self._store_log(logging.INFO, f"File {filename} downloaded and message created")

        except Exception as e:
            self.logger.error(f"Error fetching files: {str(e)}")
            self._store_log(logging.ERROR, f"Error fetching files: {str(e)}")
        finally:
            self.disconnect()

    def _get_message_type(self):
        stream = mongo.db.streams.find_one({"uuid": self.stream_uuid, "organization_uuid": self.organization_uuid})
        return stream.get('message_type', 'Unknown')

    def _store_log(self, level, message):
        log_entry = {
            "stream_uuid": self.stream_uuid,
            "organization_uuid": self.organization_uuid,
            "level": logging.getLevelName(level),
            "message": message,
            "timestamp": datetime.utcnow()
        }
        try:
            mongo.db.logs.insert_one(log_entry)
        except Exception as e:
            self.logger.error(f"Error while logging to MongoDB: {e}")

    def update_fetch_interval(self, new_interval):
        self.fetch_interval = new_interval
        self.logger.info(f"Updated fetch interval to {new_interval} seconds")
        self._store_log(logging.INFO, f"Updated fetch interval to {new_interval} seconds")

    def test_connection(self):
        try:
            if self.connect():
                self.disconnect()
                return True, "Connection successful"
            else:
                return False, "Failed to connect to SFTP server"
        except Exception as e:
            return False, f"Error testing connection: {str(e)}"