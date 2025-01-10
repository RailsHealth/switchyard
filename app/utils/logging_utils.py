# app/utils/logging_utils.py

from app.extensions import mongo
from datetime import datetime
from typing import Optional, Dict, Any
from flask import current_app

def log_message_cycle(
    message_uuid: str,
    event_type: str, 
    details: Dict[str, Any],
    status: str,
    organization_uuid: Optional[str] = None
) -> None:
    """
    Log message processing cycle event
    
    Args:
        message_uuid: Message identifier
        event_type: Type of event
        details: Additional event details
        status: Event status
        organization_uuid: Optional organization identifier. If not provided,
                         will look for it in details dictionary
    """
    try:
        # Create base log entry
        log_entry = {
            "message_uuid": message_uuid,
            "event_type": event_type,
            "status": status,
            "timestamp": datetime.utcnow(),
            "details": details.copy()  # Make a copy to avoid modifying the original
        }

        # Handle organization_uuid (support both patterns)
        if organization_uuid:
            log_entry["organization_uuid"] = organization_uuid
        elif "organization_uuid" in details:
            log_entry["organization_uuid"] = details["organization_uuid"]
            
        # Log to database
        result = mongo.db.message_cycle_logs.insert_one(log_entry)
        
        if not result.acknowledged:
            current_app.logger.error(
                f"Failed to insert message cycle log for message {message_uuid}"
            )
            
    except Exception as e:
        current_app.logger.error(f"Error logging message cycle: {str(e)}")