import threading
import time
from datetime import datetime
import logging
import requests
from flask import current_app
from app.extensions import mongo
import json
from app.services.fhir_validator import FHIRValidator

class FHIRInterface:
    def __init__(self, stream_uuid, url, fhir_version, mongo_uri, organization_uuid):
        self.stream_uuid = stream_uuid
        self.organization_uuid = organization_uuid
        self.url = url
        self.fhir_version = fhir_version
        self.mongo_uri = mongo_uri
        self.listening = False
        self.last_fetch_time = None
        self.fetch_interval = 600  # 10 minutes in seconds
        self.max_retries = 3
        self.retry_delay = 5  # seconds

        self.shutdown_event = threading.Event()
        self.processing_lock = threading.Lock()
        self.fetch_thread = None

        self.logger = logging.getLogger(f'FHIRInterface-{stream_uuid}')
        self.logger.setLevel(logging.DEBUG)

        self.db=mongo.db
        self.messages_collection = self.db["messages"]
        self.logs_collection = self.db["logs"]

    def start_listening(self):
        if not self.listening:
            self.listening = True
            self.shutdown_event.clear()
            self.last_fetch_time = self._get_last_fetch_time()
            self.logger.info(f"Started listening to FHIR server: {self.url}")
            self._store_log(logging.INFO, f"Started listening to FHIR server: {self.url}")
            self.fetch_thread = threading.Thread(target=self._fetch_loop)
            self.fetch_thread.start()

    def stop_listening(self):
        if self.listening:
            self.listening = False
            self.shutdown_event.set()
            self.logger.info(f"Stopping FHIR interface for: {self.url}")
            self._store_log(logging.INFO, f"Stopping FHIR interface for: {self.url}")
            
            if self.fetch_thread:
                self.fetch_thread.join(timeout=2)  # Reduced timeout to 2 seconds
                if self.fetch_thread.is_alive():
                    self.logger.warning(f"Fetch thread for {self.url} did not stop within the timeout period")
            
            self.logger.info(f"Stopped listening to FHIR server: {self.url}")
            self._store_log(logging.INFO, f"Stopped listening to FHIR server: {self.url}")

    def _fetch_loop(self):
        while not self.shutdown_event.is_set():
            try:
                self.fetch_data()
            except Exception as e:
                self.logger.error(f"Error in fetch loop: {str(e)}")
            
            # Check shutdown_event every second for more responsiveness
            for _ in range(self.fetch_interval):
                if self.shutdown_event.wait(timeout=1):
                    break

    def fetch_data(self):
        if self.shutdown_event.is_set():
            return

        try:
            search_url = f"{self.url}/Bundle"
            params = {
                "_sort": "-_lastUpdated",
                "_count": 10,
                "_lastUpdated": f"gt{self.last_fetch_time.isoformat()}" if self.last_fetch_time else None
            }

            while not self.shutdown_event.is_set():
                with self.processing_lock:
                    response = self._make_request("GET", search_url, params=params)
                    bundle = response.json()

                    if bundle.get("resourceType") == "Bundle":
                        self._process_bundle(bundle)

                        if bundle.get("entry"):
                            latest_update = max(entry["resource"].get("meta", {}).get("lastUpdated", "") 
                                                for entry in bundle["entry"])
                            if latest_update:
                                self.last_fetch_time = datetime.fromisoformat(latest_update.rstrip('Z'))

                        next_link = next((link for link in bundle.get("link", []) if link["relation"] == "next"), None)
                        if next_link:
                            search_url = next_link["url"]
                            params = None
                        else:
                            break
                    else:
                        self.logger.warning(f"Unexpected response format: {bundle}")
                        break

        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            self._store_log(logging.ERROR, f"Error fetching data: {str(e)}")

    def _make_request(self, method, url, **kwargs):
        for attempt in range(self.max_retries):
            try:
                response = requests.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay)

    def _process_bundle(self, bundle):
        self._store_message(bundle)

    def _store_message(self, message):
        message_dict = {
            "stream_uuid": self.stream_uuid,
            "organization_uuid": self.organization_uuid,
            "message": json.dumps(message),
            "parsed": True,
            "timestamp": datetime.utcnow(),
            "type": "FHIR",
            "validation_status": "pending"
        }
        try:
            self.logger.debug(f"Attempting to store FHIR message: {message_dict}")
            result = self.messages_collection.insert_one(message_dict)
            if result.acknowledged:
                self.logger.info(f"FHIR message stored: {message_dict['message'][:100]}...")
                is_valid, validation_message = FHIRValidator.validate_fhir_message(message)
            
                # Update the message with validation results
                self.messages_collection.update_one(
                    {"_id": result.inserted_id},
                    {
                        "$set": {
                            "validation_status": "valid" if is_valid else "invalid",
                            "validation_message": validation_message
                        }
                    }
                )
                self._store_validation_log(result.inserted_id, is_valid, validation_message)
            else:
                self.logger.error(f"Failed to store FHIR message in MongoDB: {message_dict['message'][:100]}...")
        except Exception as e:
            self.logger.error(f"Failed to store FHIR message in MongoDB: {e}")

    def _store_validation_log(self, message_id, is_valid, validation_message):
        log_entry = {
            "message_id": message_id,
            "stream_uuid": self.stream_uuid,
            "organization_uuid": self.organization_uuid,
            "is_valid": is_valid,
            "validation_message": validation_message,
            "timestamp": datetime.utcnow()
        }
        try:
            self.db.validation_logs.insert_one(log_entry)
        except Exception as e:
            self.logger.error(f"Failed to store validation log: {e}")            

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
            if result.acknowledged:
                self.logger.debug(f"Log entry stored successfully: {log_entry}")
            else:
                self.logger.error(f"Failed to log message to MongoDB: {message}")
        except Exception as e:
            self.logger.error(f"Error while logging to MongoDB: {e}")

    def _get_last_fetch_time(self):
        # Retrieve the timestamp of the last fetched message from the database
        last_message = self.messages_collection.find_one(
            {"stream_uuid": self.stream_uuid, "type": "FHIR", "organization_uuid": self.organization_uuid},
            sort=[("timestamp", -1)]
        )
        return last_message["timestamp"] if last_message else None