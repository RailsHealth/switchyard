from fhir.resources import construct_fhir_element
from fhir.resources.resource import Resource
from datetime import datetime
from flask import current_app
from app.extensions import mongo
from pymongo.errors import PyMongoError

class FHIRValidator:
    @staticmethod
    def validate_fhir_message(fhir_json):
        try:
            # Attempt to construct a FHIR resource from the JSON
            resource = construct_fhir_element('Resource', fhir_json)
            
            # If successful, the message is valid
            return True, "FHIR message is valid"
        except Exception as e:
            # If an exception is raised, the message is invalid
            return False, str(e)

    @staticmethod
    def validate_fhir_messages():
        try:
            fhir_messages = mongo.db.fhir_messages.find({"validation_status": {"$ne": "completed"}})
            for message in fhir_messages:
                is_valid, validation_message = FHIRValidator.validate_fhir_message(message['fhir_content'])
                
                validation_status = "valid" if is_valid else "invalid"
                
                mongo.db.fhir_messages.update_one(
                    {"_id": message['_id']},
                    {
                        "$set": {
                            "validation_status": "completed",
                            "is_valid": is_valid,
                            "validation_message": validation_message
                        }
                    }
                )

                # Log the validation result
                mongo.db.validation_logs.insert_one({
                    "message_id": message['_id'],
                    "is_valid": is_valid,
                    "validation_message": validation_message,
                    "timestamp": datetime.utcnow()
                })
        except PyMongoError as e:
            current_app.logger.error(f"Database error in validate_fhir_messages: {str(e)}")

def init_fhir_validator(app):
    # This function is now a no-op, but we keep it for backwards compatibility
    pass