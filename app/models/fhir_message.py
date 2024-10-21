from app.extensions import mongo
from datetime import datetime
from bson import ObjectId

class FHIRMessage:
    @staticmethod
    def create(hl7_message_uuid, fhir_content, conversion_metadata, organization_uuid):
        fhir_message = {
            "hl7_message_uuid": hl7_message_uuid,
            "fhir_content": fhir_content,
            "conversion_metadata": conversion_metadata,
            "organization_uuid": organization_uuid,
            "created_at": datetime.utcnow()
        }
        result = mongo.db.fhir_messages.insert_one(fhir_message)
        return str(result.inserted_id)

    @staticmethod
    def get_by_id(fhir_message_id, organization_uuid):
        return mongo.db.fhir_messages.find_one({
            "_id": ObjectId(fhir_message_id),
            "organization_uuid": organization_uuid
        })

    @staticmethod
    def get_by_hl7_message_uuid(hl7_message_uuid, organization_uuid):
        return mongo.db.fhir_messages.find_one({
            "hl7_message_uuid": hl7_message_uuid,
            "organization_uuid": organization_uuid
        })

    @staticmethod
    def list_by_organization(organization_uuid, limit=None, skip=None):
        query = {"organization_uuid": organization_uuid}
        cursor = mongo.db.fhir_messages.find(query).sort("created_at", -1)
        
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)

    @staticmethod
    def update(fhir_message_id, update_data, organization_uuid):
        result = mongo.db.fhir_messages.update_one(
            {"_id": ObjectId(fhir_message_id), "organization_uuid": organization_uuid},
            {"$set": update_data}
        )
        return result.modified_count > 0

    @staticmethod
    def delete(fhir_message_id, organization_uuid):
        result = mongo.db.fhir_messages.delete_one({
            "_id": ObjectId(fhir_message_id),
            "organization_uuid": organization_uuid
        })
        return result.deleted_count > 0