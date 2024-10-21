from app.celery_app import celery
from app.extensions import mongo
from flask import current_app
import requests
import json
from datetime import datetime, timedelta
from bson import ObjectId
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from requests.exceptions import RequestException
from app.services.file_parser_service import FileParserService
from app.services.fhir_validator import FHIRValidator
from app.services.health_data_converter import process_pending_conversions as hdc_process_pending_conversions
from app.services.fhir_service import scheduled_fhir_fetch
from app.utils.logging_utils import log_message_cycle

# Constants
MAX_RETRY_ATTEMPTS_NEW = 1
MAX_RETRY_ATTEMPTS_CURRENT = 3
PROCESSING_TIMEOUT = 300  # 5 minutes

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry=retry_if_exception_type(RequestException))
def send_conversion_request(url, payload, attempt, endpoint_type):
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response

def convert_message(message, endpoint_url, max_retries, endpoint_type):
    for attempt in range(max_retries):
        try:
            response = send_conversion_request(endpoint_url, message, attempt + 1, endpoint_type)
            return response.json()
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

@celery.task(bind=True, max_retries=MAX_RETRY_ATTEMPTS_CURRENT)
def generic_to_fhir_conversion(self, message_id, message_type):
    try:
        current_app.logger.info(f"Starting {message_type} to FHIR conversion for message {message_id}")
        
        message = mongo.db.messages.find_one_and_update(
            {"_id": ObjectId(message_id), "conversion_status": {"$in": ["pending", "queued", "failed"]}},
            {"$set": {"conversion_status": "processing", "last_processing_start": datetime.utcnow()}},
            return_document=True
        )
        
        if not message:
            current_app.logger.info(f"Message {message_id} is already being processed, completed, or still pending parsing")
            return

        # Check if parsing is complete
        if message.get('parsing_status') != 'completed':
            current_app.logger.error(f"Cannot convert message {message_id} - parsing not completed")
            mongo.db.messages.update_one(
                {"_id": ObjectId(message_id)},
                {"$set": {"conversion_status": "pending_parsing"}}
            )
            return

        new_endpoint, current_endpoint = get_endpoint_config(message_type)

        payload = {
            "UUID": str(message['_id']),
            "CreationTimestamp": message['timestamp'].timestamp(),
            "OriginalDataType": message_type,
            "MessageBody": message['message'],
            "organization_uuid": message['organization_uuid']
        }

        try:
            if new_endpoint:
                outgoing_message = convert_message(payload, new_endpoint, MAX_RETRY_ATTEMPTS_NEW, "new")
            elif current_endpoint:
                outgoing_message = convert_message(payload, current_endpoint, MAX_RETRY_ATTEMPTS_CURRENT, "current")
            else:
                raise ValueError(f"No valid endpoint configuration for message type: {message_type}")
        except Exception as e:
            if new_endpoint and current_endpoint:
                outgoing_message = convert_message(payload, current_endpoint, MAX_RETRY_ATTEMPTS_CURRENT, "current")
            else:
                raise

        if not all(key in outgoing_message for key in ['UUID', 'CreationTimestamp', 'ConversionTimestamp', 'OriginalDataType', 'MessageBody', 'Isvalidated']):
            raise ValueError("Invalid API response structure")

        fhir_content = json.loads(outgoing_message['MessageBody'])
        
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
                "conversion_time": (datetime.utcnow() - message['timestamp']).total_seconds()
            }
        }

        # Use upsert to avoid duplicates
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
            {"_id": ObjectId(message_id)},
            {
                "$set": {
                    "conversion_status": "completed",
                    "retry_count": 0,
                    "last_converted_at": datetime.utcnow(),
                    "error_message": None
                }
            }
        )

        current_app.logger.info(f"{message_type} conversion completed successfully for message {message['uuid']}")

    except Exception as e:
        current_app.logger.error(f"Error in {message_type} conversion for message {message_id}: {str(e)}")
        retry_count = message.get('retry_count', 0) + 1 if message else 0
        next_retry_time = datetime.utcnow() + timedelta(seconds=self.default_retry_delay)
        
        update_data = {
            "conversion_status": "failed",
            "retry_count": retry_count,
            "next_retry_time": next_retry_time,
            "error_message": str(e),
            "last_failed_at": datetime.utcnow()
        }
        
        if retry_count >= self.max_retries:
            update_data["conversion_status"] = "error"
            update_data["error_message"] += " (Max retry attempts reached)"
        
        mongo.db.messages.update_one(
            {"_id": ObjectId(message_id)},
            {"$set": update_data}
        )
        
        if retry_count < self.max_retries:
            raise self.retry(exc=e)

@celery.task
def hl7v2_to_fhir_conversion(message_id):
    return generic_to_fhir_conversion(message_id, "HL7v2")

@celery.task
def clinical_notes_to_fhir_conversion(message_id):
    return generic_to_fhir_conversion(message_id, "Clinical Notes")

@celery.task
def ccda_to_fhir_conversion(message_id):
    return generic_to_fhir_conversion(message_id, "CCDA")

@celery.task
def x12_to_fhir_conversion(message_id):
    return generic_to_fhir_conversion(message_id, "X12")

@celery.task
def cleanup_old_messages(days=30):
    current_app.logger.info(f"Starting cleanup of messages older than {days} days")
    cutoff_date = datetime.utcnow() - timedelta(days=days)
    result = mongo.db.messages.delete_many({"timestamp": {"$lt": cutoff_date}})
    current_app.logger.info(f"Deleted {result.deleted_count} old messages")
    return result.deleted_count

@celery.task
def check_stuck_messages():
    current_app.logger.info("Checking for stuck messages")
    timeout = datetime.utcnow() - timedelta(seconds=PROCESSING_TIMEOUT)
    stuck_messages = mongo.db.messages.find({
        "conversion_status": "processing",
        "last_processing_start": {"$lt": timeout}
    })
    
    count = 0
    for message in stuck_messages:
        mongo.db.messages.update_one(
            {"_id": message['_id']},
            {"$set": {"conversion_status": "failed", "error_message": "Conversion timed out"}}
        )
        generic_to_fhir_conversion.apply_async(args=[str(message['_id']), message['type']], queue=f"{message['type'].lower()}_conversion")
        count += 1
    
    current_app.logger.info(f"Reset {count} stuck messages")
    return count

@celery.task
def fetch_fhir_data():
    current_app.logger.info("Starting scheduled FHIR data fetch")
    scheduled_fhir_fetch()
    current_app.logger.info("Completed scheduled FHIR data fetch")

@celery.task
def process_pending_conversions():
    with current_app.app_context():
        return hdc_process_pending_conversions(current_app, mongo)

@celery.task
def parse_files():
    current_app.logger.info("Starting file parsing process")
    FileParserService.process_pending_files()
    current_app.logger.info("Completed file parsing process")

@celery.task
def validate_fhir_messages():
    current_app.logger.info("Starting FHIR message validation process")
    FHIRValidator.validate_fhir_messages()
    current_app.logger.info("Completed FHIR message validation process")

@celery.task
def log_conversion_metrics():
    from app.services.health_data_converter import get_conversion_statistics, get_conversion_queue_status
    
    stats = get_conversion_statistics(current_app, mongo)
    queue_status = get_conversion_queue_status(current_app, mongo)
    
    current_app.logger.info(f"Conversion Queue Status: {json.dumps(queue_status)}")
    current_app.logger.info(f"Conversion Statistics: {json.dumps(stats)}")

@celery.task
def scheduled_maintenance():
    current_app.logger.info("Starting scheduled maintenance tasks")
    cleanup_old_messages.delay()
    check_stuck_messages.delay()
    current_app.logger.info("Completed scheduled maintenance tasks")

@celery.task
def refresh_fhir_interfaces():
    from app.services.fhir_service import initialize_fhir_interfaces
    current_app.logger.info("Refreshing FHIR interfaces")
    initialize_fhir_interfaces()
    current_app.logger.info("FHIR interfaces refreshed")

def get_endpoint_config(message_type):
    flag_key = f"{message_type.upper()}_DUAL_ENDPOINTSWITCH"
    flag_value = current_app.config.get(flag_key, "DUAL").upper()
    
    new_endpoint_key = f"{message_type.upper()}_NEW_API_URL"
    current_endpoint_key = f"{message_type.upper()}_CURRENT_API_URL"
    
    new_endpoint = current_app.config.get(new_endpoint_key)
    current_endpoint = current_app.config.get(current_endpoint_key)
    
    if flag_value in ["1", "DUAL"]:
        return new_endpoint, current_endpoint
    elif flag_value in ["2", "DET"]:
        return new_endpoint, None
    elif flag_value in ["3", "PROB"]:
        return None, current_endpoint
    else:
        raise ValueError(f"Invalid flag value for {message_type}: {flag_value}")

@celery.task
def process_message_type(message_type):
    current_app.logger.info(f"Starting process_{message_type}_messages function")
    query = {
        "type": message_type,
        "$or": [
            {"conversion_status": "pending"},
            {"conversion_status": "failed", "retry_count": {"$lt": MAX_RETRY_ATTEMPTS_CURRENT}},
            {"conversion_status": "pending_parsing", "parsing_status": "completed"}
        ],
        "organization_uuid": {"$exists": True}
    }
    
    pending_messages = list(mongo.db.messages.find(query).limit(current_app.config['BATCH_SIZE']))
    current_app.logger.info(f"Found {len(pending_messages)} {message_type} messages to process")
    
    for message in pending_messages:
        message_id = str(message['_id'])
        if not celery.AsyncResult(message_id).state:
            current_app.logger.info(f"Queueing {message_type} message {message_id} for conversion")
            task_mapping = {
                "HL7v2": hl7v2_to_fhir_conversion,
                "Clinical Notes": clinical_notes_to_fhir_conversion,
                "CCDA": ccda_to_fhir_conversion,
                "X12": x12_to_fhir_conversion
            }
            conversion_task = task_mapping.get(message_type)
            if conversion_task:
                conversion_task.apply_async(args=[message_id], task_id=message_id, queue=f'{message_type.lower()}_conversion')
                mongo.db.messages.update_one(
                    {"_id": message['_id']},
                    {"$set": {"conversion_status": "queued"}}
                )
            else:
                current_app.logger.error(f"No conversion task found for message type: {message_type}")
        else:
            current_app.logger.info(f"{message_type} message {message_id} already queued for conversion")
    
    current_app.logger.info(f"Processed {len(pending_messages)} {message_type} messages")
    return len(pending_messages)

@celery.task
def process_all_message_types():
    message_types = ["HL7v2", "Clinical Notes", "CCDA", "X12"]
    total_processed = 0
    
    for message_type in message_types:
        processed_count = process_message_type.delay(message_type)
        total_processed += processed_count
    
    return total_processed

@celery.task(name="tasks.parse_pasted_message")
def parse_pasted_message(message_id):
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

        # Queue for conversion if appropriate
        if detected_type in ["HL7v2", "Clinical Notes", "CCDA", "X12"]:
            task_mapping = {
                "HL7v2": hl7v2_to_fhir_conversion,
                "Clinical Notes": clinical_notes_to_fhir_conversion,
                "CCDA": ccda_to_fhir_conversion,
                "X12": x12_to_fhir_conversion
            }
            conversion_task = task_mapping.get(detected_type)
            if conversion_task:
                conversion_task.apply_async(args=[str(message_id)], queue=f'{detected_type.lower()}_conversion')
                log_message_cycle(message['uuid'], message['organization_uuid'], "queued_for_conversion", detected_type, 
                                  {"queue": f'{detected_type.lower()}_conversion'}, "success")
            else:
                log_message_cycle(message['uuid'], message['organization_uuid'], "conversion_not_applicable", detected_type, 
                                  {}, "info")
        else:
            mongo.db.messages.update_one(
                {"_id": ObjectId(message_id)},
                {"$set": {"conversion_status": "not_applicable"}}
            )
            log_message_cycle(message['uuid'], message['organization_uuid'], "conversion_not_applicable", detected_type, 
                              {}, "info")

    except Exception as e:
        log_message_cycle(message['uuid'], message['organization_uuid'], "parsing_failed", message['type'], 
                          {"error": str(e)}, "error")
        mongo.db.messages.update_one(
            {"_id": ObjectId(message_id)},
            {"$set": {"parsing_status": "failed", "error_message": str(e), "conversion_status": "failed"}}
        )
