# app/models/user.py

from bson import ObjectId
from app.extensions import mongo
from datetime import datetime
import uuid

class User:
    @staticmethod
    def create(google_id, name, email, profile_picture_url):
        user = {
            "uuid": str(uuid.uuid4()),
            "google_id": google_id,
            "name": name,
            "email": email,
            "profile_picture_url": profile_picture_url,
            "created_at": datetime.utcnow(),
            "last_login": datetime.utcnow(),
            "organizations": [],
            "current_organization_uuid": None,
            "status": "active"
        }
        result = mongo.db.users.insert_one(user)
        return user["uuid"]

    @staticmethod
    def get_by_google_id(google_id):
        return mongo.db.users.find_one({"google_id": google_id})

    @staticmethod
    def get_by_uuid(uuid):
        return mongo.db.users.find_one({"uuid": uuid})

    @staticmethod
    def get_by_email(email):
        return mongo.db.users.find_one({"email": email})

    @staticmethod
    def update(uuid, **kwargs):
        kwargs["updated_at"] = datetime.utcnow()
        result = mongo.db.users.update_one({"uuid": uuid}, {"$set": kwargs})
        return result.modified_count > 0

    @staticmethod
    def add_to_organization(user_uuid, org_uuid, role="admin"):
        result = mongo.db.users.update_one(
            {"uuid": user_uuid},
            {
                "$addToSet": {"organizations": {"uuid": org_uuid, "role": role}},
                "$set": {"current_organization_uuid": org_uuid}
            }
        )
        return result.modified_count > 0

    @staticmethod
    def remove_from_organization(user_uuid, org_uuid):
        result = mongo.db.users.update_one(
            {"uuid": user_uuid},
            {
                "$pull": {"organizations": {"uuid": org_uuid}},
                "$set": {"current_organization_uuid": None}
            }
        )
        return result.modified_count > 0

    @staticmethod
    def set_current_organization(user_uuid, org_uuid):
        result = mongo.db.users.update_one(
            {"uuid": user_uuid},
            {"$set": {"current_organization_uuid": org_uuid}}
        )
        return result.modified_count > 0

    @staticmethod
    def get_organizations(user_uuid):
        user = User.get_by_uuid(user_uuid)
        return user.get("organizations", []) if user else []

    @staticmethod
    def get_current_organization(user_uuid):
        user = User.get_by_uuid(user_uuid)
        return user.get("current_organization_uuid") if user else None

    @staticmethod
    def delete(uuid):
        result = mongo.db.users.update_one(
            {"uuid": uuid},
            {"$set": {"status": "deleted", "deleted_at": datetime.utcnow()}}
        )
        return result.modified_count > 0

    @staticmethod
    def get_user_role_in_organization(user_uuid, org_uuid):
        user = User.get_by_uuid(user_uuid)
        if user:
            for org in user.get("organizations", []):
                if org["uuid"] == org_uuid:
                    return org["role"]
        return None

    @staticmethod
    def update_user_role_in_organization(user_uuid, org_uuid, new_role):
        result = mongo.db.users.update_one(
            {"uuid": user_uuid, "organizations.uuid": org_uuid},
            {"$set": {"organizations.$.role": new_role}}
        )
        return result.modified_count > 0

    @staticmethod
    def get_all_users_in_organization(org_uuid):
        return list(mongo.db.users.find(
            {"organizations.uuid": org_uuid, "status": "active"},
            {"uuid": 1, "name": 1, "email": 1, "profile_picture_url": 1, "organizations.$": 1}
        ))

    @staticmethod
    def search_users(query, limit=10):
        regex_query = {"$regex": query, "$options": "i"}
        return list(mongo.db.users.find(
            {"$or": [
                {"name": regex_query},
                {"email": regex_query}
            ],
            "status": "active"
            },
            {"uuid": 1, "name": 1, "email": 1, "profile_picture_url": 1}
        ).limit(limit))