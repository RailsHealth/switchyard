# app/models/fhir_message.py

from app.extensions import mongo
from datetime import datetime

class FHIRMessage:
    @staticmethod
    def create(hl7_message_uuid, fhir_content, conversion_metadata, organization_uuid):
        fhir_message = {
            "hl7_message_uuid": hl7_message_uuid,
            "fhir_content": fhir_content,
            "conversion_metadata": conversion_metadata,
            "organization_uuid": organization_uuid,  # Add this line
            "created_at": datetime.utcnow()
        }
        result = mongo.db.fhir_messages.insert_one(fhir_message)
        return result.inserted_id

    @staticmethod
    def get_by_hl7_message_uuid(hl7_message_uuid, organization_uuid):
        return mongo.db.fhir_messages.find_one({
            "hl7_message_uuid": hl7_message_uuid,
            "organization_uuid": organization_uuid
        })

    @staticmethod
    def list_by_organization(organization_uuid, limit=None, skip=None):
        query = {"organization_uuid": organization_uuid}
        cursor = mongo.db.fhir_messages.find(query)
        
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)