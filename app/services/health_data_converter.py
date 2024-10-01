import requests
from flask import current_app
from app.extensions import mongo, scheduler
from datetime import datetime, timedelta
import time
import json
from bson import ObjectId

# Constants
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 600  # 10 minutes in seconds
BATCH_SIZE = 5
CHECK_INTERVAL = 10  # 10 seconds between checks for new messages
MAX_INSTANCES = 3

def convert_to_fhir(app, message):
    """
    Convert a single message to FHIR format using the appropriate API.
    """
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

    incoming_message = {
        "UUID": message['uuid'],
        "CreationTimestamp": creation_timestamp,
        "OriginalDataType": message['type'],
        "MessageBody": message['message'],
        "OrganizationUUID": message['organization_uuid']
    }

    app.logger.info(f"Sending request to {api_url}")
    app.logger.debug(f"Request payload: {json.dumps(incoming_message)}")

    try:
        request_start_time = time.time()
        response = requests.post(api_url, json=incoming_message, timeout=30)
        response.raise_for_status()
        request_end_time = time.time()
        
        app.logger.info(f"Request completed in {request_end_time - request_start_time:.2f} seconds")
        
        outgoing_message = response.json()
        app.logger.debug(f"Response received: {json.dumps(outgoing_message)}")

        if 'MessageBody' not in outgoing_message:
            raise ValueError("API response does not contain 'MessageBody'")

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
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        app.logger.error(f"Error processing API response for {message['type']} message {message['uuid']}: {str(e)}")
        return None
    finally:
        app.logger.info(f"Conversion process completed for message {message['uuid']}")

def process_message_type(app, mongo, message_type):
    """
    Process messages of a specific type, including retry logic for failed messages.
    """
    query = {
        "type": message_type,
        "$or": [
            {"conversion_status": "pending"},
            {"conversion_status": "failed", "retry_count": {"$lt": MAX_RETRY_ATTEMPTS}}
        ]
    }
    
    # Prioritize pending messages over failed ones
    sort_order = [("conversion_status", 1), ("timestamp", 1)]
    
    pending_messages = list(mongo.db.messages.find(query).sort(sort_order).limit(BATCH_SIZE))

    processed_count = 0
    
    for message in pending_messages:
        app.logger.info(f"Processing message {message['uuid']} (current status: {message.get('conversion_status', 'unknown')})")
        result = convert_to_fhir(app, message)
        if result:
            fhir_message = {
                "original_message_uuid": message['uuid'],
                "organization_uuid": message['organization_uuid'],
                "fhir_content": result['fhir_content'],
                "conversion_metadata": result['conversion_metadata']
            }
            
            mongo.db.fhir_messages.update_one(
                {"original_message_uuid": message['uuid']},
                {"$set": fhir_message},
                upsert=True
            )
            
            mongo.db.messages.update_one(
                {"_id": message['_id']},
                {
                    "$set": {
                        "conversion_status": "completed",
                        "retry_count": 0,
                        "last_converted_at": datetime.utcnow()
                    }
                }
            )
            app.logger.info(f"Message {message['uuid']} converted successfully and marked as completed")
            processed_count += 1
        else:
            update_failed_message(mongo, message)
            app.logger.info(f"Message {message['uuid']} conversion failed")
        
        time.sleep(1)  # Add a 1-second delay between messages
    
    return processed_count

def update_failed_message(mongo, message):
    """
    Update the status of a failed message and set the next retry time.
    """
    retry_count = message.get('retry_count', 0) + 1
    next_retry_time = datetime.utcnow() + timedelta(seconds=RETRY_DELAY)
    
    mongo.db.messages.update_one(
        {"_id": message['_id']},
        {
            "$set": {
                "conversion_status": "failed",
                "retry_count": retry_count,
                "next_retry_time": next_retry_time
            }
        }
    )

def process_pending_conversions(app, mongo):
    """
    Process all pending conversions for all message types.
    """
    message_types = ["HL7", "Clinical Notes", "CCDA", "X12"]
    total_processed = 0
    
    for message_type in message_types:
        processed_count = process_message_type(app, mongo, message_type)
        total_processed += processed_count
    
    return total_processed

def scheduled_process_pending_conversions():
    """
    Scheduled task to process pending conversions.
    """
    app = current_app._get_current_object()
    app.logger.info("Starting FHIR conversion process for all message types")
    
    total_processed = process_pending_conversions(app, mongo)
    
    app.logger.info(f"Completed FHIR conversion process. Processed {total_processed} messages.")
    
    return total_processed

def init_conversion_scheduler(app):
    @scheduler.task('interval', id='process_to_fhir', seconds=CHECK_INTERVAL, misfire_grace_time=300, max_instances=MAX_INSTANCES)
    def scheduled_conversion_task():
        with app.app_context():
            total_processed = scheduled_process_pending_conversions()
            
            # If messages were processed, we immediately schedule another run
            if total_processed > 0:
                scheduler.add_job(
                    func=scheduled_conversion_task,
                    trigger='date',
                    run_date=datetime.now() + timedelta(seconds=1),
                    id='immediate_process_to_fhir',
                    replace_existing=True
                )

    # Initial setup of the job
    with app.app_context():
        # Remove the job if it already exists
        existing_job = scheduler.get_job('process_to_fhir')
        if existing_job:
            scheduler.remove_job('process_to_fhir')
        
        # Add the job with the correct parameters
        scheduler.add_job(
            func=scheduled_conversion_task,
            trigger='interval',
            seconds=CHECK_INTERVAL,
            id='process_to_fhir',
            max_instances=MAX_INSTANCES
        )

def get_conversion_statistics(app, mongo):
    """
    Get statistics about the conversion process.
    """
    stats = {}
    message_types = ["HL7v2", "Clinical Notes", "CCDA", "X12"]
    
    for message_type in message_types:
        total_count = mongo.db.messages.count_documents({"type": message_type})
        pending_count = mongo.db.messages.count_documents({"type": message_type, "conversion_status": "pending"})
        completed_count = mongo.db.messages.count_documents({"type": message_type, "conversion_status": "completed"})
        failed_count = mongo.db.messages.count_documents({"type": message_type, "conversion_status": "failed"})
        
        stats[message_type] = {
            "total": total_count,
            "pending": pending_count,
            "completed": completed_count,
            "failed": failed_count
        }
    
    return stats

def manually_convert_message(app, mongo, message_uuid):
    """
    Manually trigger conversion for a specific message.
    """
    with app.app_context():
        message = mongo.db.messages.find_one({"uuid": message_uuid})
        if not message:
            return {"status": "error", "message": "Message not found"}
        
        result = convert_to_fhir(app, message)
        if result:
            fhir_message = {
                "original_message_uuid": message['uuid'],
                "organization_uuid": message['organization_uuid'],
                "fhir_content": result['fhir_content'],
                "conversion_metadata": result['conversion_metadata']
            }
            
            mongo.db.fhir_messages.update_one(
                {"original_message_uuid": message['uuid']},
                {"$set": fhir_message},
                upsert=True
            )
            
            mongo.db.messages.update_one(
                {"_id": message['_id']},
                {"$set": {"conversion_status": "completed", "retry_count": 0}}
            )
            return {"status": "success", "message": "Conversion completed successfully"}
        else:
            update_failed_message(mongo, message)
            return {"status": "error", "message": "Conversion failed"}

def get_conversion_queue_status(app, mongo):
    """
    Get the status of the conversion queue.
    """
    pending_count = mongo.db.messages.count_documents({"conversion_status": "pending"})
    failed_count = mongo.db.messages.count_documents({"conversion_status": "failed"})
    completed_count = mongo.db.messages.count_documents({"conversion_status": "completed"})
    
    return {
        "pending": pending_count,
        "failed": failed_count,
        "completed": completed_count,
        "total": pending_count + failed_count + completed_count
    }

def reset_failed_conversions(app, mongo):
    """
    Reset all failed conversions to pending status.
    """
    result = mongo.db.messages.update_many(
        {"conversion_status": "failed"},
        {"$set": {"conversion_status": "pending", "retry_count": 0}}
    )
    return {
        "reset_count": result.modified_count,
        "status": "success",
        "message": f"Reset {result.modified_count} failed conversions to pending status."
    }

def get_conversion_errors(app, mongo, limit=100):
    """
    Get recent conversion errors.
    """
    errors = list(mongo.db.messages.find(
        {"conversion_status": "failed"},
        {"uuid": 1, "type": 1, "retry_count": 1, "last_error": 1}
    ).sort("updated_at", -1).limit(limit))
    
    return errors

if __name__ == "__main__":
    from flask import Flask
    from config import Config
    
    app = Flask(__name__)
    app.config.from_object(Config)
    
    with app.app_context():
        init_conversion_scheduler(app)