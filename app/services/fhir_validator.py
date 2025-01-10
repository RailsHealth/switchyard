from fhir.resources import construct_fhir_element
from datetime import datetime
import json
from typing import Tuple, Dict, Any, List, Optional
from flask import current_app
from app.extensions import mongo

class FHIRValidator:
    """
    FHIR validation service supporting both individual resources and bundles
    Handles validation for HAPI and Firely FHIR servers
    """
    
    # Common FHIR resource types
    VALID_RESOURCE_TYPES = {
        'Patient', 'Practitioner', 'Organization', 'Location',
        'Observation', 'Condition', 'Procedure', 'MedicationRequest',
        'DiagnosticReport', 'Immunization', 'AllergyIntolerance',
        'CarePlan', 'Encounter', 'Bundle'
    }

    # Valid bundle types
    VALID_BUNDLE_TYPES = {
        'searchset', 'transaction', 'batch', 'history',
        'collection', 'document'
    }

    @staticmethod
    def validate_json_structure(fhir_content: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate if content is proper JSON
        
        Args:
            fhir_content: Content to validate (string or dict)
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            if isinstance(fhir_content, str):
                json.loads(fhir_content)
            return True, None
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON structure: {str(e)}"

    @staticmethod
    def validate_resource_type(resource: Dict) -> Tuple[bool, Optional[str]]:
        """
        Validate FHIR resource type
        
        Args:
            resource: FHIR resource dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        resource_type = resource.get('resourceType')
        if not resource_type:
            return False, "Missing resourceType in FHIR resource"
            
        if resource_type not in FHIRValidator.VALID_RESOURCE_TYPES:
            return False, f"Unsupported resource type: {resource_type}"
            
        return True, None

    @staticmethod
    def validate_bundle_type(bundle: Dict) -> Tuple[bool, Optional[str]]:
        """
        Validate FHIR bundle type and structure
        
        Args:
            bundle: FHIR bundle dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if bundle.get('resourceType') != 'Bundle':
            return False, "Not a FHIR Bundle"
            
        bundle_type = bundle.get('type')
        if not bundle_type:
            return False, "Missing bundle type"
            
        if bundle_type not in FHIRValidator.VALID_BUNDLE_TYPES:
            return False, f"Unsupported bundle type: {bundle_type}"
            
        if not bundle.get('entry'):
            return False, "Bundle contains no entries"
            
        return True, None

    @staticmethod
    def validate_fhir_structure(fhir_json: Dict) -> Tuple[bool, Optional[str]]:
        """
        Basic FHIR resource validation using fhir.resources
        
        Args:
            fhir_json: FHIR resource dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Validate resource type first
            is_valid, error = FHIRValidator.validate_resource_type(fhir_json)
            if not is_valid:
                return False, error

            # For bundles, validate bundle structure
            if fhir_json['resourceType'] == 'Bundle':
                is_valid, error = FHIRValidator.validate_bundle_type(fhir_json)
                if not is_valid:
                    return False, error

            # Try to construct FHIR resource
            construct_fhir_element(fhir_json['resourceType'], fhir_json)
            return True, None
            
        except Exception as e:
            return False, f"Invalid FHIR structure: {str(e)}"

    @staticmethod
    def validate_fhir_resource(resource: Any) -> Tuple[bool, Optional[str]]:
        """
        Validate single FHIR resource - entry point for validation
        
        Args:
            resource: FHIR resource (string or dict)
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # First validate JSON structure
            if isinstance(resource, str):
                is_valid, error = FHIRValidator.validate_json_structure(resource)
                if not is_valid:
                    return False, error
                resource = json.loads(resource)

            # Then validate FHIR structure
            return FHIRValidator.validate_fhir_structure(resource)
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"

    @staticmethod
    def validate_bundle_references(bundle: Dict) -> Tuple[bool, Optional[str], Dict]:
        """
        Validate references within a bundle
        
        Args:
            bundle: FHIR bundle dictionary
            
        Returns:
            Tuple of (is_valid, error_message, reference_map)
        """
        reference_map = {}  # Maps temporary IDs to real IDs
        
        try:
            for entry in bundle.get('entry', []):
                resource = entry.get('resource')
                if not resource:
                    continue
                    
                # Collect fullUrl if present
                if 'fullUrl' in entry:
                    reference_map[entry['fullUrl']] = None  # Will be filled with real ID later
                    
                # Check for valid references
                refs = FHIRValidator._collect_references(resource)
                for ref in refs:
                    if ref.startswith('urn:uuid:') and ref not in reference_map:
                        return False, f"Invalid reference: {ref}", {}
                        
            return True, None, reference_map
            
        except Exception as e:
            return False, f"Reference validation error: {str(e)}", {}

    @staticmethod
    def _collect_references(resource: Dict) -> List[str]:
        """
        Collect all references from a resource
        
        Args:
            resource: FHIR resource dictionary
            
        Returns:
            List of reference strings
        """
        refs = []
        
        def process_value(value):
            if isinstance(value, dict):
                if 'reference' in value:
                    refs.append(value['reference'])
                else:
                    process_dict(value)
            elif isinstance(value, list):
                for item in value:
                    process_value(item)
                    
        def process_dict(d):
            for value in d.values():
                process_value(value)
                
        process_dict(resource)
        return refs

    @staticmethod
    def validate_transaction_bundle(bundle: Dict) -> Tuple[bool, Optional[str]]:
        """
        Validate transaction bundle
        
        Args:
            bundle: FHIR transaction bundle
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # First validate bundle structure
            is_valid, error = FHIRValidator.validate_bundle_type(bundle)
            if not is_valid:
                return False, error
                
            if bundle['type'] != 'transaction':
                return False, "Not a transaction bundle"
                
            # Validate each entry
            for entry in bundle['entry']:
                if 'request' not in entry:
                    return False, "Transaction entry missing request"
                    
                request = entry['request']
                if 'method' not in request or 'url' not in request:
                    return False, "Invalid transaction request"
                    
                if request['method'] not in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                    return False, f"Invalid request method: {request['method']}"
                    
                # Validate resource if present
                if 'resource' in entry:
                    is_valid, error = FHIRValidator.validate_fhir_resource(entry['resource'])
                    if not is_valid:
                        return False, f"Invalid resource in transaction: {error}"
                        
            # Validate references
            is_valid, error, _ = FHIRValidator.validate_bundle_references(bundle)
            if not is_valid:
                return False, error
                
            return True, None
            
        except Exception as e:
            return False, f"Transaction validation error: {str(e)}"

    @staticmethod
    def validate_fhir_messages() -> None:
        """Validate all pending FHIR messages"""
        try:
            pending_messages = mongo.db.fhir_messages.find({
                "fhir_validation_status": "pending"
            })

            for message in pending_messages:
                try:
                    content = message['fhir_content']
                    
                    # Determine validation type based on content
                    if isinstance(content, dict) and content.get('resourceType') == 'Bundle':
                        if content.get('type') == 'transaction':
                            is_valid, error = FHIRValidator.validate_transaction_bundle(content)
                        else:
                            is_valid, error = FHIRValidator.validate_fhir_resource(content)
                    else:
                        is_valid, error = FHIRValidator.validate_fhir_resource(content)
                    
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
                                    "conversion_status": "failed_validation",
                                    "fhir_validation_error": error
                                }
                            }
                        )
                        
                except Exception as e:
                    current_app.logger.error(
                        f"Error validating message {message['original_message_uuid']}: {str(e)}"
                    )

        except Exception as e:
            current_app.logger.error(f"Error in validate_fhir_messages: {str(e)}")
            raise

def init_fhir_validator(app):
    """Initialize FHIR validator with application context"""
    app.logger.info("Initializing FHIR validator")
    try:
        # Initialize indexes for validation logs
        with app.app_context():
            mongo.db.validation_logs.create_index([
                ("message_uuid", 1),
                ("timestamp", -1)
            ])
            
            mongo.db.validation_logs.create_index([
                ("validation_type", 1),
                ("is_valid", 1)
            ])
            
            # Add index for pending messages
            mongo.db.fhir_messages.create_index([
                ("fhir_validation_status", 1)
            ])
            
        app.logger.info("FHIR validator initialized successfully")
    except Exception as e:
        app.logger.error(f"Error initializing FHIR validator: {str(e)}")
        raise

def process_fhir_validation(message_uuid: str, fhir_content: Any) -> None:
    """
    Background process for FHIR validation
    
    Args:
        message_uuid: Message identifier
        fhir_content: FHIR content to validate
    """
    try:
        # Determine content type and validate accordingly
        if isinstance(fhir_content, dict) and fhir_content.get('resourceType') == 'Bundle':
            if fhir_content.get('type') == 'transaction':
                is_valid, error = FHIRValidator.validate_transaction_bundle(fhir_content)
            else:
                is_valid, error = FHIRValidator.validate_fhir_resource(fhir_content)
        else:
            is_valid, error = FHIRValidator.validate_fhir_resource(fhir_content)
        
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

        # Update original message if validation failed
        if not is_valid:
            mongo.db.messages.update_one(
                {"uuid": message_uuid},
                {
                    "$set": {
                        "conversion_status": "failed_validation",
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
            "timestamp": datetime.utcnow(),
            "resource_type": (
                fhir_content.get('resourceType') 
                if isinstance(fhir_content, dict) 
                else None
            )
        })

    except Exception as e:
        current_app.logger.error(f"Error in FHIR validation for message {message_uuid}: {str(e)}")