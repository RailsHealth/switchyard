from fhir.resources import construct_fhir_element
from datetime import datetime
import json
from flask import current_app
from app.extensions import mongo

class FHIRValidator:
    """FHIR validation service"""
    
    @staticmethod
    def validate_json_structure(fhir_content):
        """Validate if content is proper JSON"""
        try:
            if isinstance(fhir_content, str):
                json.loads(fhir_content)
            return True, None
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON structure: {str(e)}"

    @staticmethod
    def validate_fhir_structure(fhir_json):
        """Basic FHIR resource validation"""
        try:
            resource_type = fhir_json.get('resourceType')
            if not resource_type:
                return False, "Missing resourceType in FHIR resource"
            
            # Try to construct FHIR resource
            construct_fhir_element(resource_type, fhir_json)
            return True, None
        except Exception as e:
            return False, f"Invalid FHIR structure: {str(e)}"

    @staticmethod
    def validate_fhir_messages():
        """Validate all pending FHIR messages"""
        try:
            pending_messages = mongo.db.fhir_messages.find({
                "fhir_validation_status": "pending"
            })

            for message in pending_messages:
                is_valid, error = FHIRValidator.validate_fhir_structure(message['fhir_content'])
                
                # Update validation status
                mongo.db.fhir_messages.update_one(
                    {"_id": message['_id']},
                    {
                        "$set": {
                            "fhir_validation_status": "valid" if is_valid else "invalid",
                            "fhir_validation_error": error,
                            "validated_at": datetime.utcnow()
                        }
                    }
                )

                # Log validation result
                mongo.db.validation_logs.insert_one({
                    "message_uuid": message['original_message_uuid'],
                    "validation_type": "fhir_structure",
                    "is_valid": is_valid,
                    "error": error,
                    "timestamp": datetime.utcnow()
                })

                if not is_valid:
                    mongo.db.messages.update_one(
                        {"uuid": message['original_message_uuid']},
                        {
                            "$set": {
                                "conversion_status": "failed_parsing_convertedfhir",
                                "fhir_validation_error": error
                            }
                        }
                    )

        except Exception as e:
            current_app.logger.error(f"Error in validate_fhir_messages: {str(e)}")
            raise

def init_fhir_validator(app):
    """Initialize FHIR validator with application context"""
    app.logger.info("Initializing FHIR validator")
    try:
        # Initialize any required indexes for validation logs
        with app.app_context():
            mongo.db.validation_logs.create_index([
                ("message_uuid", 1),
                ("timestamp", -1)
            ])
            
            mongo.db.validation_logs.create_index([
                ("validation_type", 1),
                ("is_valid", 1)
            ])
            
        app.logger.info("FHIR validator initialized successfully")
    except Exception as e:
        app.logger.error(f"Error initializing FHIR validator: {str(e)}")
        raise

def process_fhir_validation(message_uuid, fhir_content):
    """Background process for FHIR validation"""
    try:
        # Validate FHIR structure
        is_valid, error = FHIRValidator.validate_fhir_structure(fhir_content)
        
        # Update validation status
        mongo.db.fhir_messages.update_one(
            {"original_message_uuid": message_uuid},
            {
                "$set": {
                    "fhir_validation_status": "valid" if is_valid else "invalid",
                    "fhir_validation_error": error,
                    "validated_at": datetime.utcnow()
                }
            }
        )

        # If validation failed, update original message
        if not is_valid:
            mongo.db.messages.update_one(
                {"uuid": message_uuid},
                {
                    "$set": {
                        "conversion_status": "failed_parsing_convertedfhir",
                        "fhir_validation_error": error
                    }
                }
            )

        # Log validation result
        mongo.db.validation_logs.insert_one({
            "message_uuid": message_uuid,
            "validation_type": "fhir_structure",
            "is_valid": is_valid,
            "error": error,
            "timestamp": datetime.utcnow()
        })

    except Exception as e:
        current_app.logger.error(f"Error in FHIR validation for message {message_uuid}: {str(e)}")