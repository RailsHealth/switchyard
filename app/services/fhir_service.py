import requests
from flask import current_app
from datetime import datetime
import logging
from app.extensions import mongo

logger = logging.getLogger(__name__)

class FHIRInterface:
    def __init__(self, stream_uuid, url, fhir_version, organization_uuid):
        self.stream_uuid = stream_uuid
        self.organization_uuid = organization_uuid
        self.url = url
        self.fhir_version = fhir_version
        self.listening = False
        self.last_fetch_time = None

    def start_listening(self):
        self.listening = True
        self.last_fetch_time = self._get_last_fetch_time()
        logger.info(f"Started listening to FHIR server: {self.url}")

    def stop_listening(self):
        self.listening = False
        logger.info(f"Stopped listening to FHIR server: {self.url}")

    def fetch_data(self):
        if not self.listening:
            return

        try:
            search_url = f"{self.url}/Bundle"
            params = {
                "_sort": "-_lastUpdated",
                "_count": 10,
                "_lastUpdated": f"gt{self.last_fetch_time.isoformat()}" if self.last_fetch_time else None
            }

            response = requests.get(search_url, params=params)
            response.raise_for_status()
            bundle = response.json()

            if bundle.get("resourceType") == "Bundle":
                self._process_bundle(bundle)

                if bundle.get("entry"):
                    latest_update = max(entry["resource"].get("meta", {}).get("lastUpdated", "") 
                                        for entry in bundle["entry"])
                    if latest_update:
                        self.last_fetch_time = datetime.fromisoformat(latest_update.rstrip('Z'))
            else:
                logger.warning(f"Unexpected response format: {bundle}")

        except requests.RequestException as e:
            logger.error(f"Error fetching data: {str(e)}")

    def _process_bundle(self, bundle):
        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if resource:
                message = {
                    "stream_uuid": self.stream_uuid,
                    "organization_uuid": self.organization_uuid,
                    "message": resource,
                    "type": "FHIR",
                    "timestamp": datetime.utcnow(),
                    "parsed": True
                }
                mongo.db.messages.insert_one(message)
        logger.info(f"Processed {len(bundle.get('entry', []))} FHIR resources")

    def _get_last_fetch_time(self):
        last_message = mongo.db.messages.find_one(
            {"stream_uuid": self.stream_uuid, "type": "FHIR"},
            sort=[("timestamp", -1)]
        )
        return last_message["timestamp"] if last_message else None

fhir_interfaces = {}

def initialize_fhir_interfaces():
    streams = mongo.db.streams.find({"message_type": "FHIR", "deleted": {"$ne": True}})
    
    for stream in streams:
        fhir_interface = FHIRInterface(
            stream_uuid=stream['uuid'],
            url=stream['url'],
            organization_uuid=stream['organization_uuid'],
            fhir_version=stream['fhir_version']
        )
        fhir_interfaces[stream['uuid']] = fhir_interface
        
        if stream.get('active', False):
            fhir_interface.start_listening()

    logger.info(f"Initialized {len(fhir_interfaces)} FHIR interfaces")

def get_fhir_interface(stream_uuid):
    return fhir_interfaces.get(stream_uuid)

def add_fhir_interface(stream_uuid, url, fhir_version, organization_uuid):
    fhir_interface = FHIRInterface(stream_uuid, url, fhir_version, organization_uuid)
    fhir_interfaces[stream_uuid] = fhir_interface
    return fhir_interface

def remove_fhir_interface(stream_uuid):
    if stream_uuid in fhir_interfaces:
        fhir_interfaces[stream_uuid].stop_listening()
        del fhir_interfaces[stream_uuid]
        logger.info(f"Removed FHIR interface for stream {stream_uuid}")

def fetch_all_fhir_data():
    for interface in fhir_interfaces.values():
        if interface.listening:
            interface.fetch_data()

# This function can be called periodically by a Celery task
def scheduled_fhir_fetch():
    logger.info("Starting scheduled FHIR data fetch")
    fetch_all_fhir_data()
    logger.info("Completed scheduled FHIR data fetch")