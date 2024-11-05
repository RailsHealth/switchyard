from app.extensions.celery_ext import celery
from app.extensions import mongo
from flask import current_app
import json
from datetime import datetime, timedelta
from bson import ObjectId
from tenacity import retry, stop_after_attempt, wait_fixed
from app.services.file_parser_service import FileParserService
from app.services.fhir_validator import FHIRValidator
from app.services.health_data_converter import process_pending_conversions as hdc_process_pending_conversions
from app.services.fhir_service import scheduled_fhir_fetch
from app.utils.logging_utils import log_message_cycle
from app.services.external_api_service import convert_message, validate_api_response

# Constants
PROCESSING_TIMEOUT = 300  # 5 minutes
MAX_RETRIES = 3
RETRY_DELAY = 5

# Helper Functions
def should_queue_for_conversion(message_id, parsing_status, detected_type, original_type, error_message=None):
    """Determines if a message should be queued for conversion"""
    if parsing_status != 'completed':
        return False, f"Parsing status is {parsing_status}"
    
    if error_message:
        return False, f"Parsing failed with error: {error_message}"
    
    if detected_type == 'FHIR' or original_type == 'FHIR':
        return False, "FHIR messages do not require conversion"

    if detected_type == 'RAW':
        return False, "Raw messages cannot be converted"
        
    if detected_type not in ["HL7v2", "Clinical Notes", "CCDA", "X12"]:
        return False, f"Message type {detected_type} not supported for conversion"
        
    return True, "OK"

def update_message_status(message_id, status, error=None, retry_count=None, metadata=None):
    """Centralized function to update message status"""
    update_data = {
        "conversion_status": status,
        "last_updated": datetime.utcnow()
    }
    
    if error:
        update_data["error_message"] = str(error)
        update_data["last_failed_at"] = datetime.utcnow()
    
    if retry_count is not None:
        update_data["retry_count"] = retry_count
    
    if metadata:
        update_data.update(metadata)
    
    mongo.db.messages.update_one(
        {"_id": ObjectId(message_id)},
        {"$set": update_data}
    )

# Core Conversion Tasks
@celery.task(bind=True, max_retries=MAX_RETRIES)
def generic_to_fhir_conversion(self, message_id, message_type):
    """Generic task for converting messages to FHIR format"""
    try:
        # Initial logging
        current_app.logger.info(f"Starting {message_type} to FHIR conversion for message {message_id}")
        log_message_cycle(message_id, None, "conversion_started", message_type, {
            "task_id": self.request.id,
            "retry": self.request.retries
        }, "in_progress")
        
        # Get and lock message
        message = mongo.db.messages.find_one_and_update(
            {"_id": ObjectId(message_id), "conversion_status": {"$in": ["pending", "queued", "failed"]}},
            {"$set": {"conversion_status": "processing", "last_processing_start": datetime.utcnow()}},
            return_document=True
        )
        
        if not message:
            current_app.logger.info(f"Message {message_id} not found or not in valid state for conversion")
            return

        # Get stream information to determine message source
        stream = mongo.db.streams.find_one({"uuid": message.get('stream_uuid')})
        current_app.logger.debug(f"Stream info for message {message_id}: {stream}")
        
        # Check if this is an MLLP source
        is_mllp_source = (
            stream and 
            stream.get('message_type') == 'HL7v2' and 
            stream.get('connection_type') == 'mllp'
        )
        
        current_app.logger.debug(f"Message {message_id} is MLLP source: {is_mllp_source}")

        # Only check parsing status for non-MLLP HL7v2 messages or other message types
        if not (message_type == 'HL7v2' and is_mllp_source):
            if message.get('parsing_status') != 'completed':
                current_app.logger.info(f"Setting message {message_id} to pending_parsing status")
                update_message_status(message_id, "pending_parsing")
                return

        try:
            # Prepare payload
            payload = {
                "UUID": str(message['uuid']),  # Using message uuid instead of _id
                "CreationTimestamp": message['timestamp'].timestamp(),
                "OriginalDataType": "HL7" if message_type == "HL7v2" else message_type,  # Fix type name
                "MessageBody": message['message']
            }

            # Convert message
            current_app.logger.info(f"Converting message {message_id}")
            outgoing_message = convert_message(payload, message_type)
            
            if not validate_api_response(outgoing_message):
                raise ValueError("Invalid API response structure")

            # Process successful conversion
            fhir_message = {
                "original_message_uuid": message['uuid'],
                "organization_uuid": message.get('organization_uuid'),
                "fhir_content": json.loads(outgoing_message['MessageBody']),
                "conversion_metadata": {
                    "original_creation_timestamp": outgoing_message['CreationTimestamp'],
                    "conversion_timestamp": outgoing_message['ConversionTimestamp'],
                    "is_validated": outgoing_message['Isvalidated'],
                    "transient_uuid": outgoing_message['UUID'],
                    "original_data_type": outgoing_message['OriginalDataType'],
                    "conversion_time": (datetime.utcnow() - message['timestamp']).total_seconds(),
                    "celery_task_id": self.request.id,
                    "conversion_retries": self.request.retries
                }
            }

            # Store converted message
            current_app.logger.info(f"Storing converted FHIR message for {message_id}")
            mongo.db.fhir_messages.update_one(
                {
                    "original_message_uuid": message['uuid'],
                    "organization_uuid": message.get('organization_uuid')
                },
                {"$set": fhir_message},
                upsert=True
            )

            # Update status
            update_message_status(
                message_id, 
                "completed",
                metadata={
                    "last_converted_at": datetime.utcnow(),
                    "retry_count": 0
                }
            )

            log_message_cycle(
                message['uuid'],
                message.get('organization_uuid'),
                "conversion_completed",
                message_type,
                {
                    "conversion_time": fhir_message['conversion_metadata']['conversion_time'],
                    "is_mllp": is_mllp_source
                },
                "success"
            )

        except Exception as e:
            current_app.logger.error(f"Error converting message {message_id}: {str(e)}")
            retry_count = message.get('retry_count', 0) + 1
            
            if retry_count >= MAX_RETRIES:
                status = "error"
                error_msg = f"{str(e)} (Max retry attempts reached)"
            else:
                status = "failed"
                error_msg = str(e)
                raise self.retry(exc=e)
            
            update_message_status(
                message_id,
                status,
                error=error_msg,
                retry_count=retry_count,
                metadata={"next_retry_time": datetime.utcnow() + timedelta(seconds=RETRY_DELAY)}
            )

    except Exception as e:
        current_app.logger.error(f"Unexpected error in conversion task for message {message_id}: {str(e)}")
        log_message_cycle(
            message['uuid'] if message else message_id,
            message.get('organization_uuid') if message else None,
            "unexpected_error",
            message_type,
            {"error": str(e), "is_mllp": is_mllp_source if 'is_mllp_source' in locals() else None},
            "error"
        )
        raise

# Message Type Specific Conversion Tasks
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

# Parsing Tasks
@celery.task(name="tasks.parse_pasted_message")
def parse_pasted_message(message_id):
    """Task for parsing pasted messages"""
    try:
        message = mongo.db.messages.find_one({"_id": ObjectId(message_id)})
        if not message:
            return

        log_message_cycle(
            message['uuid'],
            message['organization_uuid'],
            "parsing_started",
            message.get('type', 'unknown'),
            {"source": "pasted_message"},
            "in_progress"
        )

        original_type = message.get('type', 'unknown')
        extracted_text = message['message']
        detected_type, confidence = FileParserService.analyze_content(extracted_text)

        # Type detection and parsing
        should_update_type = confidence > 0.8 and detected_type != original_type
        final_type = detected_type if should_update_type else original_type

        try:
            # Parse based on type
            parsed_content, parsing_status, error_message = parse_content(final_type, extracted_text)
            
            # Prepare update data
            update_data = prepare_parsing_update(
                parsed_content, 
                final_type, 
                parsing_status,
                detected_type,
                confidence,
                error_message
            )

            # Update message
            mongo.db.messages.update_one(
                {"_id": ObjectId(message_id)},
                {"$set": update_data}
            )

            # Queue for conversion if appropriate
            if should_queue_conversion(parsing_status, final_type, error_message):
                queue_message_conversion(message_id, final_type, message)

            log_parsing_completion(message, final_type, parsing_status, confidence, original_type)

        except Exception as e:
            handle_parsing_error(message_id, message, final_type, str(e), original_type)
            raise

    except Exception as e:
        handle_parsing_failure(message_id, message, str(e))
        raise

@celery.task(name="tasks.parse_sftp_file")
def parse_sftp_file(message_id):
    """Task for parsing SFTP files"""
    try:
        message = mongo.db.messages.find_one({"_id": ObjectId(message_id)})
        if not message:
            return

        log_message_cycle(
            message['uuid'],
            message['organization_uuid'],
            "sftp_parsing_started",
            message.get('type', 'unknown'),
            {"filename": message.get('filename', 'unknown')},
            "in_progress"
        )

        original_type = message.get('type', 'unknown')
        extracted_text = FileParserService.extract_text_from_file(message['local_path'])
        detected_type, confidence = FileParserService.analyze_content(extracted_text)

        try:
            parsed_content, parsing_status, error_message = parse_content(detected_type, extracted_text)
            
            update_data = prepare_parsing_update(
                parsed_content,
                detected_type,
                parsing_status,
                detected_type,
                confidence,
                error_message
            )

            mongo.db.messages.update_one(
                {"_id": ObjectId(message_id)},
                {"$set": update_data}
            )

            if should_queue_conversion(parsing_status, detected_type, error_message):
                queue_message_conversion(message_id, detected_type, message)

            log_parsing_completion(
                message, 
                detected_type, 
                parsing_status, 
                confidence, 
                original_type,
                is_sftp=True
            )

        except Exception as e:
            handle_parsing_error(
                message_id, 
                message, 
                detected_type, 
                str(e), 
                original_type,
                is_sftp=True
            )
            raise

    except Exception as e:
        handle_parsing_failure(message_id, message, str(e), is_sftp=True)
        raise

# Parsing Helper Functions
def parse_content(message_type, content):
    """Parse content based on message type"""
    if message_type == 'CCDA':
        parsed = FileParserService.parse_ccda(content)
        error = parsed.get('error')
        return parsed, 'completed' if not error else 'failed', error
    elif message_type == 'X12':
        parsed = FileParserService.parse_x12(content)
        error = parsed.get('error')
        return parsed, 'completed' if not error else 'failed', error
    elif message_type == 'FHIR':
        try:
            parsed = json.loads(content)
            return parsed, 'completed', None
        except json.JSONDecodeError as e:
            return content, 'failed', f"Invalid FHIR JSON: {str(e)}"
    else:
        return content, 'completed', None

def prepare_parsing_update(content, final_type, parsing_status, detected_type, confidence, error_message):
    """Prepare update data for parsed message"""
    update_data = {
        "message": content,
        "type": final_type,
        "parsed": True,
        "parsing_status": parsing_status,
        "parsed_at": datetime.utcnow(),
        "content_analysis": {
            "detected_type": detected_type,
            "confidence": confidence
        }
    }

    if error_message:
        update_data.update({
            "parsing_error": error_message,
            "conversion_status": "failed"
        })
    else:
        update_data["conversion_status"] = "pending"

    return update_data

def queue_message_conversion(message_id, message_type, message):
    """Queue message for conversion"""
    task_mapping = {
        "HL7v2": hl7v2_to_fhir_conversion,
        "Clinical Notes": clinical_notes_to_fhir_conversion,
        "CCDA": ccda_to_fhir_conversion,
        "X12": x12_to_fhir_conversion
    }
    
    conversion_task = task_mapping.get(message_type)
    if conversion_task:
        task = conversion_task.apply_async(
            args=[str(message_id)],
            queue=f'{message_type.lower()}_conversion'
        )
        return task
    return None

# Maintenance Tasks
@celery.task
def cleanup_old_messages(days=30):
    """Clean up old messages"""
    cutoff_date = datetime.utcnow() - timedelta(days=days)
    result = mongo.db.messages.delete_many({"timestamp": {"$lt": cutoff_date}})
    return result.deleted_count

@celery.task
def check_stuck_messages():
    """Check and reset stuck messages"""
    timeout = datetime.utcnow() - timedelta(seconds=PROCESSING_TIMEOUT)
    stuck_messages = mongo.db.messages.find({
        "conversion_status": "processing",
        "last_processing_start": {"$lt": timeout}
    })
    
    count = 0
    for message in stuck_messages:
        update_message_status(str(message['_id']), "failed", error="Conversion timed out")
        generic_to_fhir_conversion.apply_async(
            args=[str(message['_id']), message['type']], 
            queue=f"{message['type'].lower()}_conversion"
        )
        count += 1
    
    return count

# Scheduled Tasks
@celery.task
def fetch_fhir_data():
    """Fetch FHIR data from configured servers"""
    current_app.logger.info("Starting scheduled FHIR data fetch")
    scheduled_fhir_fetch()
    current_app.logger.info("Completed scheduled FHIR data fetch")

@celery.task
def process_pending_conversions():
    """Process pending message conversions"""
    with current_app.app_context():
        return hdc_process_pending_conversions(current_app, mongo)

@celery.task
def parse_files():
    """Process pending file parsing"""
    current_app.logger.info("Starting file parsing process")
    FileParserService.process_pending_files()
    current_app.logger.info("Completed file parsing process")

@celery.task
def validate_fhir_messages():
    """Validate converted FHIR messages"""
    current_app.logger.info("Starting FHIR message validation process")
    FHIRValidator.validate_fhir_messages()
    current_app.logger.info("Completed FHIR message validation process")

@celery.task
def log_conversion_metrics():
    """Log conversion metrics and statistics"""
    from app.services.health_data_converter import get_conversion_statistics, get_conversion_queue_status
    
    stats = get_conversion_statistics(current_app, mongo)
    queue_status = get_conversion_queue_status(current_app, mongo)
    
    current_app.logger.info(f"Conversion Queue Status: {json.dumps(queue_status)}")
    current_app.logger.info(f"Conversion Statistics: {json.dumps(stats)}")

@celery.task
def scheduled_maintenance():
    """Run scheduled maintenance tasks"""
    current_app.logger.info("Starting scheduled maintenance tasks")
    cleanup_old_messages.delay()
    check_stuck_messages.delay()
    current_app.logger.info("Completed scheduled maintenance tasks")

@celery.task
def refresh_fhir_interfaces():
    """Refresh FHIR interface configurations"""
    from app.services.fhir_service import initialize_fhir_interfaces
    current_app.logger.info("Refreshing FHIR interfaces")
    initialize_fhir_interfaces()
    current_app.logger.info("FHIR interfaces refreshed")

# Batch Processing Tasks
@celery.task
def process_message_type(message_type):
    """Process all pending messages of a specific type"""
    current_app.logger.info(f"Starting process_{message_type}_messages function")
    query = {
        "type": message_type,
        "$or": [
            {"conversion_status": "pending"},
            {"conversion_status": "failed", "retry_count": {"$lt": MAX_RETRIES}},
            {"conversion_status": "pending_parsing", "parsing_status": "completed"}
        ],
        "organization_uuid": {"$exists": True}
    }
    
    pending_messages = list(mongo.db.messages.find(query).limit(current_app.config['BATCH_SIZE']))
    current_app.logger.info(f"Found {len(pending_messages)} {message_type} messages to process")
    
    for message in pending_messages:
        message_id = str(message['_id'])
        if not celery.AsyncResult(message_id).state:
            task_mapping = {
                "HL7v2": hl7v2_to_fhir_conversion,
                "Clinical Notes": clinical_notes_to_fhir_conversion,
                "CCDA": ccda_to_fhir_conversion,
                "X12": x12_to_fhir_conversion
            }
            conversion_task = task_mapping.get(message_type)
            if conversion_task:
                conversion_task.apply_async(
                    args=[message_id],
                    task_id=message_id,
                    queue=f'{message_type.lower()}_conversion'
                )
                update_message_status(message_id, "queued")
    
    return len(pending_messages)

@celery.task
def process_all_message_types():
    """Process messages of all supported types"""
    message_types = ["HL7v2", "Clinical Notes", "CCDA", "X12"]
    total_processed = 0
    
    for message_type in message_types:
        processed_count = process_message_type.delay(message_type)
        total_processed += processed_count
    
    return total_processed

@celery.task(name="tasks.process_fhir_validation")
def process_fhir_validation(message_uuid, fhir_content):
    """Background task for FHIR validation"""
    with current_app.app_context():
        return process_fhir_validation(message_uuid, fhir_content)

# Error Handling
from celery.signals import task_failure

@task_failure.connect
def handle_task_failure(task_id=None, exception=None, args=None, kwargs=None, traceback=None, einfo=None, sender=None, **other_kwargs):
    """Handle Celery task failures and infrastructure errors"""
    try:
        args = args or []
        message_id = args[0] if args else None
        message_type = args[1] if len(args) > 1 else None
        
        if message_id:
            try:
                message = mongo.db.messages.find_one({"_id": ObjectId(message_id)})
            except Exception:
                message = None

            # Log infrastructure error
            log_message_cycle(
                message['uuid'] if message else str(message_id),
                message.get('organization_uuid') if message else None,
                "celery_infrastructure_error",
                message_type or "unknown",
                {
                    "task_id": task_id,
                    "error": str(exception) if exception else "Unknown error",
                    "error_type": type(exception).__name__ if exception else "Unknown",
                    "traceback": str(einfo) if einfo else None,
                    "celery_task": sender.name if sender else None
                },
                "error"
            )

            # Update message status if available
            if message:
                try:
                    update_message_status(
                        message_id,
                        "error",
                        error=f"Infrastructure error: {str(exception) if exception else 'Unknown error'}"
                    )
                except Exception as update_error:
                    current_app.logger.error(f"Failed to update message status after infrastructure error: {str(update_error)}")

    except Exception as e:
        current_app.logger.error(f"Error in task failure handler: {str(e)}")
        current_app.logger.error(f"Original error: {str(exception) if exception else 'Unknown error'}")
        current_app.logger.error(f"Task ID: {task_id}")