from flask import session, g, flash
from app.models.user import User
from app.models.organization import Organization
from app.extensions import mongo
from flask import current_app as app

def set_user_and_org_context():
    user_id = session.get('user_id')
    org_id = session.get('current_org_id')
    
    if user_id:
        user = User.get_by_uuid(user_id)
        if user:
            g.user = user
            org_uuids = User.get_organizations(user_id)
            g.user_organizations = org_uuids

            user_org_data = mongo.db.users.find_one({'uuid': user_id}, {'organizations': 1})
            if user_org_data and 'organizations' in user_org_data:
                full_org_uuids = [org['uuid'] for org in user_org_data['organizations']]
                g.user_full_organizations = list(mongo.db.organizations.find({'uuid': {'$in': full_org_uuids}}))
            else:
                g.user_full_organizations = []
            
            if org_id:
                org = Organization.get_by_uuid(org_id)
                if org:
                    g.organization = org
                    g.user_role = User.get_user_role_in_organization(user_id, org_id)
                else:
                    g.organization = None
                    g.user_role = None
                    session.pop('current_org_id', None)
                    flash('Selected organization not found. Please choose another.', 'warning')
            else:
                g.organization = None
                g.user_role = None
        else:
            g.user = None
            g.user_organizations = []
            g.user_full_organizations = []
            g.organization = None
            g.user_role = None
            session.clear()
            flash('User session expired. Please log in again.', 'warning')
    else:
        g.user = None
        g.user_organizations = []
        g.user_full_organizations = []
        g.organization = None
        g.user_role = None

def set_current_organization(org_id):
    if not g.user:
        return False

    user_orgs = g.user.get('organizations', [])
    org = next((org for org in user_orgs if org['uuid'] == org_id), None)
    
    if org:
        session['current_org_id'] = org_id
        g.organization = Organization.get_by_uuid(org_id)
        g.user_role = org['role']
        return True
    else:
        flash('Invalid organization selected.', 'error')
        return False

def clear_current_organization():
    session.pop('current_org_id', None)
    g.organization = None
    g.user_role = None

def get_user_organizations():
    if hasattr(g, 'user') and g.user:
        user_orgs = g.user.get('organizations', [])
        return [Organization.get_by_uuid(org['uuid']) for org in user_orgs]
    else:
        return []

def debug_context():
    app.logger.debug(f"User: {g.user['uuid'] if g.user else None}")
    app.logger.debug(f"User Organizations (UUIDs): {g.user_organizations}")
    app.logger.debug(f"User Full Organizations: {[org['name'] for org in get_user_organizations()]}")
    app.logger.debug(f"Current Organization: {g.organization['name'] if g.organization else None}")
    app.logger.debug(f"User Role: {g.user_role}")