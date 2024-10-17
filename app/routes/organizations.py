# app/routes/organizations.py

from flask import Blueprint, request, jsonify, render_template, redirect, url_for, current_app, flash, g, session

from app.models.organization import Organization
from app.models.user import User
from app.extensions import mongo
from app.middleware import set_current_organization
from app.auth import login_required, admin_required
from pymongo.errors import PyMongoError
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, TextAreaField
from wtforms.validators import DataRequired, Email, URL, Optional

bp = Blueprint('organizations', __name__)

class CreateOrganizationForm(FlaskForm):
    name = StringField('Organization Name', validators=[DataRequired()])
    org_type = SelectField('Type of Organisation', choices=[('healthcare', 'Healthcare'), ('insurance', 'Insurance'), ('other', 'Other')], validators=[DataRequired()])
    website = StringField('Website', validators=[Optional(), URL()])
    email = StringField('Contact Email', validators=[DataRequired(), Email()])
    address = TextAreaField('Address', validators=[Optional()])


@bp.route('/create_new_organization', methods=['GET', 'POST'])
@login_required
def create_new_organization():
    form = CreateOrganizationForm()
    if form.validate_on_submit():
        try:
            org_uuid = Organization.create(
                name=form.name.data,
                org_type=form.org_type.data,
                website=form.website.data,
                email=form.email.data,
                address={'line1': form.address.data}
            )
            User.add_to_organization(g.user['uuid'], org_uuid, role='admin')
            User.set_current_organization(g.user['uuid'], org_uuid)
            
            # Update session with new organization
            session['current_org_id'] = org_uuid
            
            flash('Organization created successfully', 'success')
            return redirect(url_for('main.index'))
        except Exception as e:
            current_app.logger.error(f"Error creating organization: {str(e)}")
            flash('An error occurred while creating the organization', 'error')
    
    return render_template('create_new_organisation.html', form=form)

@bp.route('/', methods=['GET'])
@login_required
@admin_required
def list_organizations():
    try:
        organizations = Organization.get_by_user(g.user['uuid'])
        return render_template('organizations/list.html', organizations=organizations)
    except PyMongoError as e:
        current_app.logger.error(f"Database error in list_organizations: {str(e)}")
        flash("An error occurred while fetching organizations.", "error")
        return render_template('error.html'), 500

@bp.route('/create', methods=['GET', 'POST'])
@login_required
@admin_required
def create_organization():
    if request.method == 'POST':
        try:
            data = request.form
            org_uuid = Organization.create(
                name=data['name'],
                org_type=data['type'],
                parent_org_uuid=data.get('parent_org_uuid'),
                website=data.get('website'),
                address={
                    'line1': data.get('address_line1'),
                    'line2': data.get('address_line2'),
                    'zip_code': data.get('zip_code'),
                    'country': data.get('country')
                },
                phone=data.get('phone'),
                email=data.get('email'),
                point_of_contact={
                    'name': data.get('poc_name'),
                    'email': data.get('poc_email'),
                    'phone': data.get('poc_phone')
                }
            )
            User.add_to_organization(g.user['uuid'], org_uuid, role='admin')
            flash('Organization created successfully', 'success')
            return redirect(url_for('organizations.organization_detail', uuid=org_uuid))
        except PyMongoError as e:
            current_app.logger.error(f"Database error in create_organization: {str(e)}")
            flash("An error occurred while creating the organization.", "error")
            return render_template('error.html'), 500
    
    parent_organizations = Organization.get_by_user(g.user['uuid'])
    return render_template('create_organization.html', parent_organizations=parent_organizations)

@bp.route('/<uuid>', methods=['GET'])
@login_required
@admin_required
def organization_detail(uuid):
    try:
        org = Organization.get_by_uuid(uuid)
        if not org or org['uuid'] not in [o['uuid'] for o in g.user['organizations']]:
            flash('Organization not found', 'error')
            return redirect(url_for('organizations.list_organizations'))
        return render_template('organization_details.html', organization=org)
    except PyMongoError as e:
        current_app.logger.error(f"Database error in organization_detail: {str(e)}")
        flash("An error occurred while fetching organization details.", "error")
        return render_template('error.html'), 500

@bp.route('/<uuid>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_organization(uuid):
    try:
        org = Organization.get_by_uuid(uuid)
        if not org or org['uuid'] not in [o['uuid'] for o in g.user['organizations']]:
            flash('Organization not found', 'error')
            return redirect(url_for('organizations.list_organizations'))

        user_role = User.get_user_role_in_organization(g.user['uuid'], uuid)
        if user_role != 'admin':
            flash('You do not have permission to edit this organization', 'error')
            return redirect(url_for('organizations.organization_detail', uuid=uuid))

        if request.method == 'POST':
            data = request.form
            updated = Organization.update(
                uuid,
                name=data['name'],
                org_type=data['type'],
                website=data.get('website'),
                address={
                    'line1': data.get('address_line1'),
                    'line2': data.get('address_line2'),
                    'zip_code': data.get('zip_code'),
                    'country': data.get('country')
                },
                email=data.get('email')
            )
            if updated:
                flash('Organization updated successfully', 'success')
            else:
                flash('Failed to update organization', 'error')
            return redirect(url_for('organizations.organization_detail', uuid=uuid))

        return render_template('edit_organization.html', organization=org)
    except PyMongoError as e:
        current_app.logger.error(f"Database error in edit_organization: {str(e)}")
        flash("An error occurred while editing the organization.", "error")
        return render_template('error.html'), 500

@bp.route('/<uuid>/delete', methods=['POST'])
@login_required
@admin_required
def delete_organization(uuid):
    try:
        org = Organization.get_by_uuid(uuid)
        if not org or org['uuid'] not in [o['uuid'] for o in g.user['organizations']]:
            return jsonify({'status': 'error', 'message': 'Organization not found'}), 404

        user_role = User.get_user_role_in_organization(g.user['uuid'], uuid)
        if user_role != 'admin':
            return jsonify({'status': 'error', 'message': 'You do not have permission to delete this organization'}), 403

        deleted = Organization.delete(uuid)
        if deleted:
            users = User.get_all_users_in_organization(uuid)
            for user in users:
                User.remove_from_organization(user['uuid'], uuid)
                if not User.get_organizations(user['uuid']):
                    User.delete(user['uuid'])

            return jsonify({'status': 'success', 'message': 'Organization deleted successfully'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to delete organization'})
    except PyMongoError as e:
        current_app.logger.error(f"Database error in delete_organization: {str(e)}")
        return jsonify({'status': 'error', 'message': 'An error occurred while deleting the organization'}), 500

@bp.route('/list_organizations', methods=['GET'])
@login_required
def list_organizations_by_name():
    try:
        user_orgs = get_user_organizations()
        org_list = [{'uuid': org['uuid'], 'name': org['name']} for org in user_orgs]
        return jsonify(org_list)
    except Exception as e:
        current_app.logger.error(f"Error in list_organizations_by_name: {str(e)}")
        return jsonify({'error': 'An error occurred while fetching organizations'}), 500


@bp.route('/switch/<uuid>', methods=['GET'])
@login_required
def switch_organization(uuid):
    try:
        if set_current_organization(uuid):
            org = Organization.get_by_uuid(uuid)
            
            # Update the user's current_organization_uuid in the database
            mongo.db.users.update_one(
                {'uuid': g.user['uuid']},
                {'$set': {'current_organization_uuid': uuid}}
            )
            
            flash(f'Switched to organization: {org["name"]}', 'success')
        else:
            flash('Failed to switch organization.', 'error')

        # Redirect to the previous page or the home page
        next_page = request.args.get('next') or url_for('main.index')
        return redirect(next_page)

    except Exception as e:
        current_app.logger.error(f"Error in switch_organization: {str(e)}")
        flash("An error occurred while switching organizations.", "error")
        return redirect(url_for('main.index'))

# Error handling for this blueprint
@bp.app_errorhandler(404)
def not_found_error(error):
    current_app.logger.error(f'404 error occurred: {str(error)}')
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    current_app.logger.error(f'500 error occurred: {str(error)}')
    return render_template('500.html'), 500