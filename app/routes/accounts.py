from flask import Blueprint, render_template, g, redirect, url_for, flash, current_app, jsonify, request
from app.models.user import User
from app.models.organization import Organization
from app.middleware import login_required, admin_required
from bson.objectid import ObjectId
from datetime import datetime
from app.extensions import mongo

bp = Blueprint('accounts', __name__)

@bp.route('/manage')
@login_required
def manage():
    user_orgs = User.get_organizations(g.user['uuid'])
    is_sole_admin = any(
        len(User.get_all_users_in_organization(org['uuid'])) == 1 
        for org in user_orgs if User.get_user_role_in_organization(g.user['uuid'], org['uuid']) == 'admin'
    )
    return render_template('profile.html', user=g.user, is_sole_admin=is_sole_admin, organizations=user_orgs, active_tab='profile')

@bp.route('/profile')
@login_required
def profile():
    user_orgs = User.get_organizations(g.user['uuid'])
    is_sole_admin = any(
        len(User.get_all_users_in_organization(org['uuid'])) == 1 
        for org in user_orgs if User.get_user_role_in_organization(g.user['uuid'], org['uuid']) == 'admin'
    )
    return render_template('profile.html', user=g.user, is_sole_admin=is_sole_admin, organizations=user_orgs)

@bp.route('/organizations')
@login_required
def organizations():
    user_orgs = User.get_organizations(g.user['uuid'])
    organizations = [Organization.get_by_uuid(org['uuid']) for org in user_orgs]
    return render_template('organizations.html', organizations=organizations)

@bp.route('/organizations/<org_id>')
@login_required
def organization_details(org_id):
    organization = Organization.get_by_uuid(org_id)
    if not organization:
        flash('Organization not found', 'error')
        return redirect(url_for('accounts.organizations'))
    
    user_role = User.get_user_role_in_organization(g.user['uuid'], org_id)
    if not user_role:
        flash('You do not have access to this organization', 'error')
        return redirect(url_for('accounts.organizations'))
    
    org_users = User.get_all_users_in_organization(org_id)
    return render_template('organization_details.html', organization=organization, user_role=user_role, org_users=org_users)

@bp.route('/delete_user', methods=['POST'])
@login_required
def delete_user():
    user_orgs = User.get_organizations(g.user['uuid'])
    orgs_to_delete = []

    for org in user_orgs:
        org_users = User.get_all_users_in_organization(org['uuid'])
        if len(org_users) == 1 and org_users[0]['uuid'] == g.user['uuid']:
            orgs_to_delete.append(org['uuid'])

    try:
        if User.delete(g.user['uuid']):
            # Delete organizations where this user was the sole member
            for org_uuid in orgs_to_delete:
                Organization.delete(org_uuid)

            flash('Your account has been deleted', 'success')
            return jsonify({'status': 'success', 'message': 'Account deleted successfully'})
        else:
            raise Exception('Failed to delete user account')
    except Exception as e:
        current_app.logger.error(f"Error deleting user account: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to delete account'}), 500

@bp.route('/switch_organization/<org_id>', methods=['POST'])
@login_required
def switch_organization(org_id):
    if org_id not in [org['uuid'] for org in User.get_organizations(g.user['uuid'])]:
        return jsonify({'status': 'error', 'message': 'Invalid organization'}), 400

    if User.set_current_organization(g.user['uuid'], org_id):
        org = Organization.get_by_uuid(org_id)
        return jsonify({'status': 'success', 'message': f'Switched to organization: {org["name"]}'})
    else:
        return jsonify({'status': 'error', 'message': 'Failed to switch organization'}), 500

@bp.route('/user_role/<org_id>')
@login_required
def get_user_role(org_id):
    role = User.get_user_role_in_organization(g.user['uuid'], org_id)
    return jsonify({'role': role})

@bp.route('/update_profile', methods=['POST'])
@login_required
def update_profile():
    try:
        name = request.form.get('name')
        email = request.form.get('email')
        
        if not name or not email:
            return jsonify({'status': 'error', 'message': 'Name and email are required'}), 400
        
        User.update(g.user['uuid'], name=name, email=email)
        flash('Profile updated successfully', 'success')
        return jsonify({'status': 'success', 'message': 'Profile updated successfully'})
    except Exception as e:
        current_app.logger.error(f"Error updating user profile: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to update profile'}), 500

@bp.route('/invite_user', methods=['POST'])
@login_required
@admin_required
def invite_user():
    try:
        email = request.form.get('email')
        org_id = request.form.get('org_id')
        role = request.form.get('role')
        
        if not email or not org_id or not role:
            return jsonify({'status': 'error', 'message': 'Email, organization, and role are required'}), 400
        
        # Check if user already exists
        existing_user = User.get_by_email(email)
        if existing_user:
            User.add_to_organization(existing_user['uuid'], org_id, role=role)
        else:
            # Create new user and add to organization
            user_uuid = User.create(email=email, name=email.split('@')[0], google_id=None)
            User.add_to_organization(user_uuid, org_id, role=role)
        
        # TODO: Send invitation email
        
        flash(f'Invitation sent to {email}', 'success')
        return jsonify({'status': 'success', 'message': 'Invitation sent successfully'})
    except Exception as e:
        current_app.logger.error(f"Error inviting user: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to invite user'}), 500

@bp.route('/remove_user_from_org/<org_id>/<user_id>', methods=['POST'])
@login_required
@admin_required
def remove_user_from_org(org_id, user_id):
    try:
        if User.remove_from_organization(user_id, org_id):
            flash('User removed from organization successfully', 'success')
            return jsonify({'status': 'success', 'message': 'User removed from organization'})
        else:
            raise Exception('Failed to remove user from organization')
    except Exception as e:
        current_app.logger.error(f"Error removing user from organization: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to remove user from organization'}), 500

# Error handlers
@bp.app_errorhandler(404)
def not_found_error(error):
    current_app.logger.error('404 error occurred: %s', str(error))
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    current_app.logger.error('500 error occurred: %s', str(error))
    return render_template('500.html'), 500