# app/utils/logging_utils.py

from app.extensions import mongo
from datetime import datetime

def log_message_cycle(message_uuid, organization_id, event_type, message_type, details, status, error_message=None, duration=None):
    log_entry = {
        "message_uuid": message_uuid,
        "organization_id": organization_id,
        "timestamp": datetime.utcnow(),
        "event_type": event_type,
        "message_type": message_type,
        "details": details,
        "status": status,
        "error_message": error_message,
        "duration": duration
    }
    mongo.db.message_cycle_logs.insert_one(log_entry)