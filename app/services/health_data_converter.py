import requests
from flask import current_app
from app.extensions import mongo
from datetime import datetime, timedelta
import time
import json
from bson import ObjectId
import traceback
from concurrent.futures import ThreadPoolExecutor
import os
from tenacity import retry, stop_after_attempt, wait_fixed
from app.utils.logging_utils import log_message_cycle
from app.services.external_api_service import get_endpoint_config, convert_message, validate_api_response

# Constants
MAX_RETRY_ATTEMPTS_NEW = 1
MAX_RETRY_ATTEMPTS_CURRENT = 3
RETRY_DELAY = 600  # 10 minutes in seconds
BATCH_SIZE = 5
CHECK_INTERVAL = 10  # 10 seconds between checks for new messages
MAX_INSTANCES = 3  # Use the number of CPU cores available
PROCESSING_TIMEOUT = 300  # 5 minutes

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def send_conversion_request(url, payload, attempt, endpoint_type):
    start_time = time.time()
    response = requests.post(url, json=payload, timeout=30)
    response_time = time.time() - start_time
    
    log_message_cycle(
        payload['UUID'],
        payload.get('organization_uuid'),
        "api_call",
        payload['OriginalDataType'],
        {
            "endpoint_type": endpoint_type,
            "attempt": attempt,
            "response_time": response_time,
            "status_code": response.status_code
        },
        "success" if response.status_code == 200 else "error"
    )
    
    response.raise_for_status()
    return response

def convert_message(message, endpoint_url, max_retries, endpoint_type):
    for attempt in range(max_retries):
        try:
            response = send_conversion_request(endpoint_url, message, attempt + 1, endpoint_type)
            return json.loads(response.text)
        except Exception as e:
            log_message_cycle(
                message['UUID'],
                message.get('organization_uuid'),
                "conversion_error",
                message['OriginalDataType'],
                {
                    "endpoint_type": endpoint_type,
                    "attempt": attempt + 1,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                "error"
            )
            if attempt == max_retries - 1:
                raise

def convert_to_fhir(app, message):
    start_time = time.time()
    app.logger.info(f"Starting conversion for message {message['uuid']}")
    log_message_cycle(message['uuid'], message['organization_uuid'], "conversion_started", message['type'], {}, "in_progress")

    new_endpoint, current_endpoint = get_endpoint_config(message['type'])

    incoming_message = {
        "UUID": message['uuid'],
        "CreationTimestamp": message['timestamp'].timestamp(),
        "OriginalDataType": message['type'],
        "MessageBody": message['message'],
        "organization_uuid": message['organization_uuid']
    }

    try:
        if new_endpoint:
            try:
                outgoing_message = convert_message(incoming_message, new_endpoint, MAX_RETRY_ATTEMPTS_NEW, "new")
            except Exception:
                if not current_endpoint:
                    raise
                outgoing_message = convert_message(incoming_message, current_endpoint, MAX_RETRY_ATTEMPTS_CURRENT, "current")
        elif current_endpoint:
            outgoing_message = convert_message(incoming_message, current_endpoint, MAX_RETRY_ATTEMPTS_CURRENT, "current")
        else:
            raise ValueError(f"No valid endpoint configuration for message type: {message['type']}")

        if not validate_api_response(outgoing_message):
            raise ValueError("Invalid API response structure")

        conversion_time = time.time() - start_time

        app.logger.info(f"Conversion successful for message {message['uuid']}")
        app.logger.info(f"Total conversion time: {conversion_time:.2f} seconds")

        log_message_cycle(message['uuid'], message['organization_uuid'], "conversion_completed", message['type'], 
                          {"conversion_time": conversion_time}, "success")

        return {
            'fhir_content': json.loads(outgoing_message['MessageBody']),
            'conversion_metadata': {
                'original_creation_timestamp': outgoing_message['CreationTimestamp'],
                'conversion_timestamp': outgoing_message['ConversionTimestamp'],
                'is_validated': outgoing_message['Isvalidated'],
                'transient_uuid': outgoing_message['UUID'],
                'original_data_type': outgoing_message['OriginalDataType'],
                'conversion_time': conversion_time
            }
        }
    except Exception as e:
        app.logger.error(f"Error converting {message['type']} message {message['uuid']} to FHIR: {str(e)}")
        app.logger.error(f"Traceback: {traceback.format_exc()}")
        log_message_cycle(message['uuid'], message['organization_uuid'], "conversion_error", message['type'], 
                          {"error": str(e), "error_type": type(e).__name__}, "error")
        return None

def process_message_type(app, mongo, message_type):
    app.logger.info(f"Starting process_{message_type}_messages function")
    query = {
        "type": message_type,
        "$or": [
            {"conversion_status": "pending"},
            {"conversion_status": "failed", "retry_count": {"$lt": MAX_RETRY_ATTEMPTS_CURRENT}}
        ],
        "organization_uuid": {"$exists": True}
    }
    
    pending_messages = list(mongo.db.messages.find(query).limit(BATCH_SIZE))
    app.logger.info(f"Found {len(pending_messages)} {message_type} messages to process")
    
    processed_count = 0
    for message in pending_messages:
        result = convert_to_fhir(app, message)
        if result:
            mongo.db.fhir_messages.update_one(
                {"original_message_uuid": message['uuid'], "organization_uuid": message['organization_uuid']},
                {"$set": result},
                upsert=True
            )
            mongo.db.messages.update_one(
                {"_id": message['_id']},
                {"$set": {"conversion_status": "completed", "retry_count": 0}}
            )
            processed_count += 1
        else:
            retry_count = message.get('retry_count', 0) + 1
            mongo.db.messages.update_one(
                {"_id": message['_id']},
                {"$set": {"conversion_status": "failed", "retry_count": retry_count}}
            )
    
    app.logger.info(f"Processed {processed_count} {message_type} messages")
    return processed_count

def process_pending_conversions(app, mongo):
    message_types = ["HL7v2", "Clinical Notes", "CCDA", "X12"]
    total_processed = 0
    
    for message_type in message_types:
        processed_count = process_message_type(app, mongo, message_type)
        total_processed += processed_count
    
    return total_processed

def get_conversion_statistics(app, mongo):
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
    pending_count = mongo.db.messages.count_documents({"conversion_status": "pending"})
    failed_count = mongo.db.messages.count_documents({"conversion_status": "failed"})
    completed_count = mongo.db.messages.count_documents({"conversion_status": "completed"})
    error_count = mongo.db.messages.count_documents({"conversion_status": "error"})
    processing_count = mongo.db.messages.count_documents({"conversion_status": "processing"})
    
    return {
        "pending": pending_count,
        "failed": failed_count,
        "completed": completed_count,
        "error": error_count,
        "processing": processing_count,
        "total": pending_count + failed_count + completed_count + error_count + processing_count
    }
def reset_failed_conversions(app, mongo):
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
    errors = list(mongo.db.messages.find(
        {"conversion_status": {"$in": ["failed", "error"]}},
        {"uuid": 1, "type": 1, "retry_count": 1, "error_message": 1, "last_processing_start": 1}
    ).sort("last_processing_start", -1).limit(limit))
    
    return errors

def cleanup_stuck_messages(app, mongo):
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
    with app.app_context():
        # Index for messages collection
        mongo.db.messages.create_index([("uuid", 1)], unique=True)
        mongo.db.messages.create_index([("conversion_status", 1), ("type", 1)])
        mongo.db.messages.create_index([("last_processing_start", 1)])

        # Index for fhir_messages collection
        mongo.db.fhir_messages.create_index([("original_message_uuid", 1), ("organization_uuid", 1)], unique=True)

        app.logger.info("Database indexes initialized successfully.")

def log_conversion_metrics(app, mongo):
    stats = get_conversion_statistics(app, mongo)
    queue_status = get_conversion_queue_status(app, mongo)
    
    app.logger.info(f"Conversion Queue Status: {json.dumps(queue_status)}")
    app.logger.info(f"Conversion Statistics: {json.dumps(stats)}")

def init_health_data_converter(app, mongo):
    with app.app_context():
        app.logger.info("Initializing health data converter")
        initialize_database_indexes(app, mongo)
        
        # Verify and log endpoint configurations
        message_types = ["HL7v2", "Clinical Notes", "CCDA", "X12"]
        for message_type in message_types:
            new_endpoint, current_endpoint = get_endpoint_config(message_type)
            app.logger.info(f"{message_type} configuration:")
            app.logger.info(f"  New endpoint: {new_endpoint}")
            app.logger.info(f"  Current endpoint: {current_endpoint}")
        
        app.logger.info("Health Data Converter initialized successfully")

def parse_pasted_message(message_id):
    from app.services.file_parser_service import FileParserService
    
    message = mongo.db.messages.find_one({"_id": ObjectId(message_id)})
    if not message:
        return

    try:
        extracted_text = message['message']
        detected_type, confidence = FileParserService.analyze_content(extracted_text)

        if detected_type != message['type']:
            log_message_cycle(message['uuid'], message['organization_uuid'], "type_mismatch", message['type'], 
                              {"detected_type": detected_type, "confidence": confidence}, "info")

        parsed_content = FileParserService.parse_file(extracted_text)

        mongo.db.messages.update_one(
            {"_id": ObjectId(message_id)},
            {
                "$set": {
                    "type": detected_type,
                    "parsed": True,
                    "parsing_status": "completed",
                    "message": parsed_content,
                    "content_analysis": {
                        "detected_type": detected_type,
                        "confidence": confidence
                    },
                    "conversion_status": "pending"  # Change status to pending after parsing
                }
            }
        )

        log_message_cycle(message['uuid'], message['organization_uuid'], "parsing_completed", detected_type, 
                          {"confidence": confidence}, "success")

    except Exception as e:
        log_message_cycle(message['uuid'], message['organization_uuid'], "parsing_failed", message['type'], 
                          {"error": str(e)}, "error")
        mongo.db.messages.update_one(
            {"_id": ObjectId(message_id)},
            {"$set": {"parsing_status": "failed", "error_message": str(e), "conversion_status": "failed"}}
        )

if __name__ == "__main__":
    from flask import Flask
    from config import Config
    
    app = Flask(__name__)
    app.config.from_object(Config)
    
    with app.app_context():
        init_health_data_converter(app, mongo)

    # Run the Flask app (for development purposes only)
    app.run(debug=True)