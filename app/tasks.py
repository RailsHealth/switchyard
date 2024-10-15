from app.celery_app import celery
from app.extensions import mongo
from flask import current_app
import requests
import json
from datetime import datetime, timedelta
from bson import ObjectId
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from requests.exceptions import RequestException

# Constants
MAX_RETRY_ATTEMPTS = 5
PROCESSING_TIMEOUT = 300  # 5 minutes

@celery.task
def process_hl7v2_messages():
    current_app.logger.info("Starting process_hl7v2_messages Celery task")
    from app.services.health_data_converter import process_hl7v2_messages as process_messages
    result = process_messages(current_app, mongo)
    current_app.logger.info(f"Processed {result} HL7v2 messages in Celery task")
    return result

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry=retry_if_exception_type(RequestException))
def send_conversion_request(url, payload):
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response

@celery.task(bind=True, max_retries=MAX_RETRY_ATTEMPTS, default_retry_delay=300)
def hl7v2_to_fhir_conversion(self, message_id):
    try:
        current_app.logger.info(f"Starting HL7v2 to FHIR conversion for message {message_id}")
        
        message = mongo.db.messages.find_one_and_update(
            {"_id": ObjectId(message_id), "conversion_status": {"$in": ["pending", "queued", "failed"]}},
            {"$set": {"conversion_status": "processing", "last_processing_start": datetime.utcnow()}},
            return_document=True
        )
        
        if not message:
            current_app.logger.info(f"Message {message_id} is already being processed or completed")
            return

        api_url = current_app.config['HL7_TO_FHIR_API_URL']
        payload = {
            "UUID": str(message['_id']),
            "CreationTimestamp": message['timestamp'].timestamp(),
            "OriginalDataType": "HL7",
            "MessageBody": message['message']
        }

        current_app.logger.info(f"Sending HL7v2 conversion request for message {message['uuid']}")
        response = send_conversion_request(api_url, payload)
        
        conversion_time = datetime.utcnow() - message['timestamp']
        
        outgoing_message = response.json()
        
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
                "conversion_time": conversion_time.total_seconds()
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

        current_app.logger.info(f"HL7v2 conversion completed successfully for message {message['uuid']}")

    except Exception as e:
        current_app.logger.error(f"Error in HL7v2 conversion for message {message_id}: {str(e)}")
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
        hl7v2_to_fhir_conversion.apply_async(args=[str(message['_id'])], queue='hl7v2_conversion')
        count += 1
    
    current_app.logger.info(f"Reset {count} stuck messages")
    return count

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        3600.0,  # 1 hour
        cleanup_old_messages.s(days=30),
        name='cleanup old messages every hour'
    )
    sender.add_periodic_task(
        900.0,  # 15 minutes
        check_stuck_messages.s(),
        name='check for stuck messages every 15 minutes'
    )