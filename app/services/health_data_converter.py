import requests
from flask import current_app
from app.extensions import mongo, celery
from datetime import datetime, timedelta
import time
import json
from bson import ObjectId
import traceback
from concurrent.futures import ThreadPoolExecutor
import os
from tenacity import retry, stop_after_attempt, wait_fixed
from app.celery_app import celery

# Constants
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 600  # 10 minutes in seconds
BATCH_SIZE = 5
CHECK_INTERVAL = 10  # 10 seconds between checks for new messages
MAX_INSTANCES = 3  # Use the number of CPU cores available
PROCESSING_TIMEOUT = 300  # 5 minutes

def parse_json_safely(json_string):
    try:
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        current_app.logger.warning(f"JSONDecodeError at position {e.pos}: {str(e)}")
        valid_json = json_string[:e.pos]
        return json.loads(valid_json + "}")  # Add closing brace if needed

def validate_api_response(app, response_data):
    required_keys = ['UUID', 'CreationTimestamp', 'ConversionTimestamp', 'OriginalDataType', 'MessageBody', 'Isvalidated']
    missing_keys = [key for key in required_keys if key not in response_data]
    if missing_keys:
        app.logger.error(f"API response is missing required fields: {', '.join(missing_keys)}")
        return False
    return True

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def send_conversion_request(url, payload):
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response

@celery.task(name="health_data_converter.convert_to_fhir")
def convert_to_fhir(app, message):
    start_time = time.time()
    app.logger.info(f"Starting conversion for message {message['uuid']}")
    creation_timestamp = message['timestamp'].timestamp()

    api_urls = {
        'HL7v2': app.config['HL7_TO_FHIR_API_URL'],
        'Clinical Notes': app.config['NOTES_TO_FHIR_API_URL'],
        'CCDA': app.config['CCDA_TO_FHIR_API_URL'],
        'X12': app.config['X12_TO_FHIR_API_URL']
    }

    api_url = api_urls.get(message['type'])
    if not api_url:
        app.logger.error(f"Unsupported message type for conversion: {message['type']}")
        return None

    original_data_type = "HL7" if message['type'] == 'HL7v2' else message['type']

    incoming_message = {
        "UUID": message['uuid'],
        "CreationTimestamp": creation_timestamp,
        "OriginalDataType": original_data_type,
        "MessageBody": message['message']
    }

    app.logger.info(f"Sending request to {api_url}")
    app.logger.debug(f"Request payload: {json.dumps(incoming_message)}")

    try:
        conversion_start_time = time.time()
        response = send_conversion_request(api_url, incoming_message)
        api_response_time = time.time() - conversion_start_time
        app.logger.info(f"API response time: {api_response_time:.2f} seconds")
        
        response_content = response.text
        app.logger.debug(f"Raw API response: {response_content}")

        outgoing_message = parse_json_safely(response_content)
        app.logger.debug(f"Parsed API response: {json.dumps(outgoing_message)}")

        if not validate_api_response(app, outgoing_message):
            raise ValueError("Invalid API response structure")

        conversion_time = time.time() - start_time

        app.logger.info(f"Conversion successful for message {message['uuid']}")
        app.logger.info(f"Total conversion time: {conversion_time:.2f} seconds")

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
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Error converting {message['type']} message {message['uuid']} to FHIR: {str(e)}")
        app.logger.error(f"Request payload: {json.dumps(incoming_message)}")
        if hasattr(e, 'response') and e.response is not None:
            app.logger.error(f"Response status code: {e.response.status_code}")
            app.logger.error(f"Response content: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        app.logger.error(f"Error decoding JSON for {message['type']} message {message['uuid']}: {str(e)}")
        app.logger.error(f"Response content: {response_content}")
        app.logger.error(f"Error at line {e.lineno}, column {e.colno}")
        app.logger.error(f"Error context: {e.doc[max(0, e.pos-50):e.pos+50]}")
        return None
    except Exception as e:
        app.logger.error(f"Unexpected error processing {message['type']} message {message['uuid']}: {str(e)}")
        app.logger.error(f"Traceback: {traceback.format_exc()}")
        return None

@celery.task(name="health_data_converter.process_message_type")
def process_message_type(app, mongo, message_type):
    if message_type == "HL7v2":
        return process_hl7v2_messages(app, mongo)
    else:
        return process_other_message_type(app, mongo, message_type)

@celery.task(name="health_data_converter.process_hl7v2_messages")
def process_hl7v2_messages(app, mongo):
    app.logger.info("Starting process_hl7v2_messages function")
    query = {
        "type": "HL7v2",
        "$or": [
            {"conversion_status": "pending"},
            {"conversion_status": "failed", "retry_count": {"$lt": MAX_RETRY_ATTEMPTS}}
        ],
        "organization_uuid": {"$exists": True}
    }
    
    pending_messages = list(mongo.db.messages.find(query).limit(BATCH_SIZE))
    app.logger.info(f"Found {len(pending_messages)} HL7v2 messages to process")
    
    for message in pending_messages:
        message_id = str(message['_id'])
        if not celery.AsyncResult(message_id).state:
            app.logger.info(f"Queueing HL7v2 message {message_id} for conversion")
            convert_to_fhir.apply_async(args=[app, message], task_id=message_id, queue='hl7v2_conversion')
            mongo.db.messages.update_one(
                {"_id": message['_id']},
                {"$set": {"conversion_status": "queued"}}
            )
        else:
            app.logger.info(f"HL7v2 message {message_id} already queued for conversion")
    
    app.logger.info(f"Processed {len(pending_messages)} HL7v2 messages")
    return len(pending_messages)
@celery.task(name="health_data_converter.process_other_message_type")
def process_other_message_type(app, mongo, message_type):
    query = {
        "type": message_type,
        "$or": [
            {"conversion_status": "pending"},
            {"conversion_status": "failed", "retry_count": {"$lt": MAX_RETRY_ATTEMPTS}},
            {"conversion_status": "processing", "last_processing_start": {"$lt": datetime.utcnow() - timedelta(seconds=PROCESSING_TIMEOUT)}}
        ]
    }
    
    sort_order = [("conversion_status", 1), ("timestamp", 1)]
    
    pending_messages = list(mongo.db.messages.find(query).sort(sort_order).limit(BATCH_SIZE))

    processed_count = 0
    
    for message in pending_messages:
        mongo.db.messages.update_one(
            {"_id": message['_id']},
            {"$set": {"conversion_status": "processing", "last_processing_start": datetime.utcnow()}}
        )

        app.logger.info(f"Processing message {message['uuid']} (current status: processing)")
        result = convert_to_fhir.delay(app, message)
        if result:
            fhir_message = {
                "original_message_uuid": message['uuid'],
                "organization_uuid": message['organization_uuid'],
                "fhir_content": result['fhir_content'],
                "conversion_metadata": result['conversion_metadata']
            }
            
            mongo.db.fhir_messages.update_one(
                {
                    "original_message_uuid": message['uuid'],
                    "organization_uuid": message['organization_uuid']
                },
                {"$set": fhir_message},
                upsert=True
            )
            
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
            app.logger.info(f"Message {message['uuid']} converted successfully and marked as completed")
            processed_count += 1
        else:
            error_message = f"Conversion failed for message {message['uuid']}"
            app.logger.info(error_message)
            update_failed_message(mongo, message, error_message)
        
        time.sleep(1)  # Add a 1-second delay between messages
    
    return processed_count

def update_failed_message(mongo, message, error_message):
    retry_count = message.get('retry_count', 0) + 1
    next_retry_time = datetime.utcnow() + timedelta(seconds=RETRY_DELAY)
    
    update_data = {
        "conversion_status": "failed",
        "retry_count": retry_count,
        "next_retry_time": next_retry_time,
        "error_message": error_message,
        "last_failed_at": datetime.utcnow()
    }
    
    if retry_count >= MAX_RETRY_ATTEMPTS:
        update_data["conversion_status"] = "error"
        update_data["error_message"] += " (Max retry attempts reached)"
    
    mongo.db.messages.update_one(
        {"_id": message['_id']},
        {"$set": update_data}
    )

@celery.task(name="health_data_converter.process_pending_conversions")
def process_pending_conversions(app, mongo):
    message_types = ["Clinical Notes", "CCDA", "X12"]
    total_processed = 0
    
    for message_type in message_types:
        processed_count = process_message_type.delay(app, mongo, message_type)
        total_processed += processed_count
    
    return total_processed

@celery.task(name="health_data_converter.scheduled_process_pending_conversions")
def scheduled_process_pending_conversions(app, mongo):
    app.logger.info("Starting FHIR conversion process for non-HL7v2 message types")
    
    message_types = ["Clinical Notes", "CCDA", "X12"]
    total_processed = 0
    
    for message_type in message_types:
        processed_count = process_message_type.delay(app, mongo, message_type)
        total_processed += processed_count
    
    app.logger.info(f"Completed FHIR conversion process. Processed {total_processed} non-HL7v2 messages.")
    
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

@celery.task(name="health_data_converter.reset_failed_conversions")
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

@celery.task(name="health_data_converter.get_conversion_errors")
def get_conversion_errors(app, mongo, limit=100):
    errors = list(mongo.db.messages.find(
        {"conversion_status": {"$in": ["failed", "error"]}},
        {"uuid": 1, "type": 1, "retry_count": 1, "error_message": 1, "last_processing_start": 1}
    ).sort("last_processing_start", -1).limit(limit))
    
    return errors

@celery.task(name="health_data_converter.cleanup_stuck_messages")
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

@celery.task(name="health_data_converter.log_conversion_metrics")
def log_conversion_metrics(app, mongo):
    stats = get_conversion_statistics(app, mongo)
    queue_status = get_conversion_queue_status(app, mongo)
    
    app.logger.info(f"Conversion Queue Status: {json.dumps(queue_status)}")
    app.logger.info(f"Conversion Statistics: {json.dumps(stats)}")

def init_health_data_converter(app, mongo):
    app.logger.info("Initializing health data converter")
    initialize_database_indexes(app, mongo)
    
    app.logger.info("Health Data Converter initialized successfully")

if __name__ == "__main__":
    from flask import Flask
    from config import Config
    
    app = Flask(__name__)
    app.config.from_object(Config)
    
    with app.app_context():
        init_health_data_converter(app, mongo)

    # Run the Flask app (for development purposes only)
    app.run(debug=True)