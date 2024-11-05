from flask import current_app
from app.extensions import mongo
from datetime import datetime, timedelta
import json
from bson import ObjectId
import traceback
from app.utils.logging_utils import log_message_cycle
from app.services.external_api_service import convert_message, validate_api_response

# Constants
PROCESSING_TIMEOUT = 300  # 5 minutes
BATCH_SIZE = 5
CHECK_INTERVAL = 10  # seconds between checks

def convert_to_fhir(app, message):
    """
    Convert a message to FHIR format with comprehensive error handling and logging
    """
    start_time = datetime.utcnow()
    app.logger.info(f"Starting conversion for message {message['uuid']}")

    # Get stream information
    stream = mongo.db.streams.find_one({"uuid": message.get('stream_uuid')})
    is_mllp_source = (
        stream and 
        stream.get('message_type') == 'HL7v2' and 
        stream.get('connection_type') == 'mllp'
    )
    
    log_message_cycle(
        message['uuid'], 
        message['organization_uuid'], 
        "conversion_started", 
        message['type'], 
        {
            "retry_count": message.get('retry_count', 0),
            "max_retries": app.config['MAX_CONVERSION_RETRIES'],
            "is_mllp": is_mllp_source
        }, 
        "in_progress"
    )

    # Check if parsing is needed (non-MLLP sources)
    if not (message['type'] == 'HL7v2' and is_mllp_source):
        if message.get('parsing_status') != 'completed':
            app.logger.info(f"Message {message['uuid']} requires parsing before conversion")
            return None

    try:
        # Prepare conversion payload
        payload = {
            "UUID": message['uuid'],
            "CreationTimestamp": message['timestamp'].timestamp(),
            "OriginalDataType": "HL7" if message['type'] == "HL7v2" else message['type'],
            "MessageBody": message['message']
        }

        # Attempt conversion
        try:
            outgoing_message = convert_message(payload, message['type'])

            # Validate API response structure
            if not validate_api_response(outgoing_message):
                retry_count = message.get('retry_count', 0) + 1
                error_msg = "Invalid API response structure"
                
                log_message_cycle(
                    message['uuid'],
                    message['organization_uuid'],
                    "api_validation_failed",
                    message['type'],
                    {
                        "retry_count": retry_count,
                        "max_retries": app.config['MAX_CONVERSION_RETRIES'],
                        "error": error_msg
                    },
                    "error"
                )

                if retry_count >= app.config['MAX_CONVERSION_RETRIES']:
                    # Update message as permanently failed
                    mongo.db.messages.update_one(
                        {"_id": message['_id']},
                        {
                            "$set": {
                                "conversion_status": "error",
                                "retry_count": retry_count,
                                "error_message": f"{error_msg} (Max retry attempts reached)",
                                "last_failed_at": datetime.utcnow()
                            }
                        }
                    )
                else:
                    # Queue for retry
                    mongo.db.messages.update_one(
                        {"_id": message['_id']},
                        {
                            "$set": {
                                "conversion_status": "failed",
                                "retry_count": retry_count,
                                "error_message": error_msg,
                                "last_failed_at": datetime.utcnow(),
                                "next_retry_time": datetime.utcnow() + timedelta(seconds=app.config['CONVERSION_RETRY_DELAY'])
                            }
                        }
                    )
                return None

        except Exception as conversion_error:
            # Handle conversion errors
            log_message_cycle(
                message['uuid'],
                message['organization_uuid'],
                "conversion_failed",
                message['type'],
                {"error": str(conversion_error)},
                "error"
            )
            raise

        # Validate JSON structure
        try:
            if isinstance(outgoing_message['MessageBody'], str):
                fhir_content = json.loads(outgoing_message['MessageBody'])
            else:
                fhir_content = outgoing_message['MessageBody']
        except json.JSONDecodeError as json_error:
            log_message_cycle(
                message['uuid'],
                message['organization_uuid'],
                "json_validation_failed",
                message['type'],
                {"error": str(json_error)},
                "error"
            )
            raise ValueError(f"Invalid JSON in conversion result: {str(json_error)}")

        # Calculate conversion time
        conversion_time = (datetime.utcnow() - start_time).total_seconds()
        
        # Prepare FHIR message data
        fhir_message = {
            "original_message_uuid": message['uuid'],
            "organization_uuid": message['organization_uuid'],
            "fhir_content": fhir_content,
            "conversion_metadata": {
                "original_creation_timestamp": outgoing_message['CreationTimestamp'],
                "conversion_timestamp": outgoing_message['ConversionTimestamp'],
                "is_validated": outgoing_message['Isvalidated'],
                "transient_uuid": outgoing_message['UUID'],
                "original_data_type": outgoing_message['OriginalDataType'],
                "conversion_time": conversion_time,
                "converted_at": datetime.utcnow()
            },
            "fhir_validation_status": "pending",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }

        # Store converted message
        mongo.db.fhir_messages.update_one(
            {
                "original_message_uuid": message['uuid'],
                "organization_uuid": message['organization_uuid']
            },
            {"$set": fhir_message},
            upsert=True
        )

        # Update original message status
        mongo.db.messages.update_one(
            {"_id": message['_id']},
            {
                "$set": {
                    "conversion_status": "completed",
                    "retry_count": 0,
                    "last_converted_at": datetime.utcnow(),
                    "error_message": None
                }
            }
        )

        # Log successful conversion
        log_message_cycle(
            message['uuid'],
            message['organization_uuid'],
            "conversion_completed",
            message['type'],
            {
                "conversion_time": conversion_time,
                "fhir_validation_status": "pending"
            },
            "success"
        )

        # Trigger background FHIR validation
        app.logger.info(f"Triggering FHIR validation for message {message['uuid']}")
        from app.tasks import process_fhir_validation
        process_fhir_validation.delay(message['uuid'], fhir_content)

        return fhir_message

    except Exception as e:
        app.logger.error(f"Error converting {message['type']} message {message['uuid']} to FHIR: {str(e)}")
        app.logger.error(f"Traceback: {traceback.format_exc()}")

        # Determine if message should be retried
        retry_count = message.get('retry_count', 0) + 1
        
        log_message_cycle(
            message['uuid'],
            message['organization_uuid'],
            "conversion_error",
            message['type'],
            {
                "error": str(e),
                "error_type": type(e).__name__,
                "retry_count": retry_count,
                "max_retries": app.config['MAX_CONVERSION_RETRIES']
            },
            "error"
        )

        # Update message status based on retry count
        update_data = {
            "last_failed_at": datetime.utcnow(),
            "error_message": str(e),
            "retry_count": retry_count
        }

        if retry_count >= app.config['MAX_CONVERSION_RETRIES']:
            update_data["conversion_status"] = "error"
            update_data["error_message"] += " (Max retry attempts reached)"
        else:
            update_data["conversion_status"] = "failed"
            update_data["next_retry_time"] = datetime.utcnow() + timedelta(seconds=app.config['CONVERSION_RETRY_DELAY'])

        mongo.db.messages.update_one(
            {"_id": message['_id']},
            {"$set": update_data}
        )

        return None

def process_message_type(app, mongo, message_type):
    """Process all pending messages of a specific type"""
    app.logger.info(f"Starting process_{message_type}_messages function")
    query = {
        "type": message_type,
        "$or": [
            {"conversion_status": "pending"},
            {"conversion_status": "failed", "retry_count": {"$lt": app.config['MAX_CONVERSION_RETRIES']}}
        ],
        "organization_uuid": {"$exists": True}
    }
    
    pending_messages = list(mongo.db.messages.find(query).limit(BATCH_SIZE))
    app.logger.info(f"Found {len(pending_messages)} {message_type} messages to process")
    
    processed_count = 0
    for message in pending_messages:
        result = convert_to_fhir(app, message)
        if result:
            # Store converted message
            mongo.db.fhir_messages.update_one(
                {"original_message_uuid": message['uuid'], "organization_uuid": message['organization_uuid']},
                {"$set": result},
                upsert=True
            )
            # Update original message status
            mongo.db.messages.update_one(
                {"_id": message['_id']},
                {
                    "$set": {
                        "conversion_status": "completed",
                        "retry_count": 0,
                        "last_converted_at": datetime.utcnow(),
                        "error_message": None
                    }
                }
            )
            processed_count += 1
        else:
            retry_count = message.get('retry_count', 0) + 1
            update_data = {
                "conversion_status": "failed",
                "retry_count": retry_count,
                "last_failed_at": datetime.utcnow()
            }
            
            if retry_count >= app.config['MAX_CONVERSION_RETRIES']:
                update_data.update({
                    "conversion_status": "error",
                    "error_message": "Max retry attempts reached"
                })
                
            mongo.db.messages.update_one(
                {"_id": message['_id']},
                {"$set": update_data}
            )
    
    app.logger.info(f"Processed {processed_count} {message_type} messages")
    return processed_count

def process_pending_conversions(app, mongo):
    """Process all pending messages across message types"""
    message_types = ["HL7v2", "Clinical Notes", "CCDA", "X12"]
    total_processed = 0
    
    for message_type in message_types:
        processed_count = process_message_type(app, mongo, message_type)
        total_processed += processed_count
    
    return total_processed

def get_conversion_statistics(app, mongo):
    """Get conversion statistics for all message types"""
    stats = {}
    message_types = ["HL7v2", "Clinical Notes", "CCDA", "X12"]
    
    for message_type in message_types:
        total_count = mongo.db.messages.count_documents({"type": message_type})
        pending_count = mongo.db.messages.count_documents({"type": message_type, "conversion_status": "pending"})
        completed_count = mongo.db.messages.count_documents({"type": message_type, "conversion_status": "completed"})
        failed_count = mongo.db.messages.count_documents({"type": message_type, "conversion_status": "failed"})
        error_count = mongo.db.messages.count_documents({"type": message_type, "conversion_status": "error"})
        processing_count = mongo.db.messages.count_documents({"type": message_type, "conversion_status": "processing"})
        
        stats[message_type] = {
            "total": total_count,
            "pending": pending_count,
            "completed": completed_count,
            "failed": failed_count,
            "error": error_count,
            "processing": processing_count
        }
    
    return stats

def get_conversion_queue_status(app, mongo):
    """Get overall conversion queue status"""
    return {
        status: mongo.db.messages.count_documents({"conversion_status": status})
        for status in ["pending", "failed", "completed", "error", "processing"]
    }

def reset_failed_conversions(app, mongo):
    """Reset failed conversions to pending status"""
    result = mongo.db.messages.update_many(
        {"conversion_status": {"$in": ["failed", "error"]}},
        {"$set": {"conversion_status": "pending", "retry_count": 0}}
    )
    return {
        "reset_count": result.modified_count,
        "status": "success",
        "message": f"Reset {result.modified_count} failed and error conversions to pending status."
    }

def get_conversion_errors(app, mongo, limit=100):
    """Get recent conversion errors"""
    return list(mongo.db.messages.find(
        {"conversion_status": {"$in": ["failed", "error"]}},
        {"uuid": 1, "type": 1, "retry_count": 1, "error_message": 1, "last_processing_start": 1}
    ).sort("last_processing_start", -1).limit(limit))

def cleanup_stuck_messages(app, mongo):
    """Reset messages stuck in processing state"""
    timeout = datetime.utcnow() - timedelta(seconds=PROCESSING_TIMEOUT)
    result = mongo.db.messages.update_many(
        {
            "conversion_status": "processing",
            "last_processing_start": {"$lt": timeout}
        },
        {
            "$set": {
                "conversion_status": "pending",
                "error_message": "Reset due to processing timeout"
            }
        }
    )
    return {
        "reset_count": result.modified_count,
        "status": "success",
        "message": f"Reset {result.modified_count} stuck messages to pending status."
    }

def initialize_database_indexes(app, mongo):
    """Initialize required database indexes"""
    with app.app_context():
        # Index for messages collection
        mongo.db.messages.create_index([("uuid", 1)], unique=True)
        mongo.db.messages.create_index([("conversion_status", 1), ("type", 1)])
        mongo.db.messages.create_index([("last_processing_start", 1)])
        mongo.db.messages.create_index([("organization_uuid", 1)])

        # Index for fhir_messages collection
        mongo.db.fhir_messages.create_index(
            [("original_message_uuid", 1), ("organization_uuid", 1)],
            unique=True
        )

        app.logger.info("Database indexes initialized successfully")

def init_health_data_converter(app, mongo):
    """Initialize the health data converter service"""
    with app.app_context():
        app.logger.info("Initializing health data converter")
        initialize_database_indexes(app, mongo)
        
        # Verify endpoint configurations
        message_types = ["HL7v2", "Clinical Notes", "CCDA", "X12"]
        for message_type in message_types:
            endpoint = app.config.get(f"{message_type.upper()}_API_URL")
            app.logger.info(f"{message_type} API endpoint: {endpoint}")
        
        app.logger.info("Health Data Converter initialized successfully")

def log_conversion_metrics(app, mongo):
    """Log current conversion metrics"""
    stats = get_conversion_statistics(app, mongo)
    queue_status = get_conversion_queue_status(app, mongo)
    
    app.logger.info(f"Conversion Queue Status: {json.dumps(queue_status)}")
    app.logger.info(f"Conversion Statistics: {json.dumps(stats)}")

if __name__ == "__main__":
    from flask import Flask
    from config import Config
    
    app = Flask(__name__)
    app.config.from_object(Config)
    
    with app.app_context():
        init_health_data_converter(app, mongo)