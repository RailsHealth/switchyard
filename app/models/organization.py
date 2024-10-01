# app/models/organization.py

from app.extensions import mongo
from datetime import datetime
import uuid
from bson import ObjectId
from pymongo.errors import PyMongoError
from flask import current_app

class Organization:
    @staticmethod
    def create(name, org_type, parent_org_uuid=None, website=None, address=None, phone=None, email=None, point_of_contact=None):
        try:
            org = {
                "uuid": str(uuid.uuid4()),
                "name": name,
                "type": org_type,
                "parent_org_uuid": parent_org_uuid,
                "website": website,
                "address": address or {},
                "phone": phone,
                "email": email,
                "point_of_contact": point_of_contact or {},
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            result = mongo.db.organizations.insert_one(org)
            return org["uuid"]
        except PyMongoError as e:
            current_app.logger.error(f"Failed to create organization: {str(e)}")
            return None

    @staticmethod
    def get_by_uuid(uuid):
        try:
            return mongo.db.organizations.find_one({"uuid": uuid})
        except PyMongoError as e:
            current_app.logger.error(f"Failed to get organization by UUID: {str(e)}")
            return None

    @staticmethod
    def get_by_name(name):
        try:
            return mongo.db.organizations.find_one({"name": name})
        except PyMongoError as e:
            current_app.logger.error(f"Failed to get organization by name: {str(e)}")
            return None

    @staticmethod
    def update(uuid, **kwargs):
        try:
            kwargs["updated_at"] = datetime.utcnow()
            result = mongo.db.organizations.update_one({"uuid": uuid}, {"$set": kwargs})
            return result.modified_count > 0
        except PyMongoError as e:
            current_app.logger.error(f"Failed to update organization: {str(e)}")
            return False

    @staticmethod
    def delete(uuid):
        try:
            result = mongo.db.organizations.delete_one({"uuid": uuid})
            return result.deleted_count > 0
        except PyMongoError as e:
            current_app.logger.error(f"Failed to delete organization: {str(e)}")
            return False

    @staticmethod
    def list_all():
        try:
            return list(mongo.db.organizations.find())
        except PyMongoError as e:
            current_app.logger.error(f"Failed to list all organizations: {str(e)}")
            return []

    @staticmethod
    def get_hierarchy(uuid):
        try:
            org = Organization.get_by_uuid(uuid)
            hierarchy = [org]
            while org and org.get('parent_org_uuid'):
                parent_org = Organization.get_by_uuid(org['parent_org_uuid'])
                if parent_org:
                    hierarchy.insert(0, parent_org)
                    org = parent_org
                else:
                    break
            return hierarchy
        except PyMongoError as e:
            current_app.logger.error(f"Failed to get organization hierarchy: {str(e)}")
            return []

    @staticmethod
    def get_children(uuid):
        try:
            return list(mongo.db.organizations.find({"parent_org_uuid": uuid}))
        except PyMongoError as e:
            current_app.logger.error(f"Failed to get child organizations: {str(e)}")
            return []

    @staticmethod
    def search(query, limit=10, skip=0):
        try:
            return list(mongo.db.organizations.find(
                {"$text": {"$search": query}},
                {"score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})]).skip(skip).limit(limit))
        except PyMongoError as e:
            current_app.logger.error(f"Failed to search organizations: {str(e)}")
            return []

    @staticmethod
    def count():
        try:
            return mongo.db.organizations.count_documents({})
        except PyMongoError as e:
            current_app.logger.error(f"Failed to count organizations: {str(e)}")
            return 0

    @staticmethod
    def bulk_create(organizations):
        try:
            for org in organizations:
                org["uuid"] = str(uuid.uuid4())
                org["created_at"] = datetime.utcnow()
                org["updated_at"] = datetime.utcnow()
            result = mongo.db.organizations.insert_many(organizations)
            return len(result.inserted_ids)
        except PyMongoError as e:
            current_app.logger.error(f"Failed to bulk create organizations: {str(e)}")
            return 0

    @staticmethod
    def bulk_update(updates):
        try:
            bulk = mongo.db.organizations.initialize_unordered_bulk_op()
            for update in updates:
                bulk.find({"uuid": update["uuid"]}).update_one({"$set": update["data"]})
            result = bulk.execute()
            return result["nModified"]
        except PyMongoError as e:
            current_app.logger.error(f"Failed to bulk update organizations: {str(e)}")
            return 0