# app/auth/routes.py
from flask import Blueprint, current_app, redirect, url_for, session, request
from flask import render_template, flash, g
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, EmailField, URLField, SubmitField
from wtforms.validators import DataRequired, Email, URL, Optional
from app.models.user import User
from app.models.organization import Organization
from datetime import datetime
from app.extensions import oauth
from . import bp
from .decorators import login_required
import logging

logger = logging.getLogger(__name__)

class LoginForm(FlaskForm):
    pass

class CreateOrganizationForm(FlaskForm):
    type = SelectField('Type of Organisation', validators=[DataRequired()])
    name = StringField('Organization Name', validators=[DataRequired()])
    website = URLField('Website', validators=[Optional(), URL()])
    email = EmailField('Contact Email', validators=[Optional(), Email()])
    submit = SubmitField('Finish')

@bp.route('/login', methods=['GET', 'POST'])
def login():
    if g.user:
        return redirect(url_for('main.view_home'))
        
    form = LoginForm()
    if form.validate_on_submit():
        return redirect(url_for('auth.google_login'))
    return render_template('login.html', form=form)

@bp.route('/google_login')
def google_login():
    # Store the next URL in session if provided
    next_url = request.args.get('next')
    if next_url:
        session['next_url'] = next_url
        
    redirect_uri = url_for('auth.authorized', _external=True)
    return oauth.google.authorize_redirect(redirect_uri)

@bp.route('/logout')
@login_required
def logout():
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('main.index'))


@bp.route('/authorized')
def authorized():
    try:
        token = oauth.google.authorize_access_token()
        if not token:
            logger.error("Failed to get OAuth token")
            flash('Authentication failed. Please try again.', 'error')
            return redirect(url_for('auth.login'))

        resp = oauth.google.get('https://openidconnect.googleapis.com/v1/userinfo')
        if not resp:
            logger.error("Failed to get user info from Google")
            flash('Failed to get user information. Please try again.', 'error')
            return redirect(url_for('auth.login'))

        user_info = resp.json()
        logger.debug(f"Received user info: {user_info}")  # Debug log
        
        google_id = user_info['sub']
        email = user_info.get('email')
        
        if not email:
            logger.error("No email found in user info")
            flash('Failed to get email from Google. Please try again.', 'error')
            return redirect(url_for('auth.login'))

        # Check if user exists
        user = User.get_by_google_id(google_id)
        if not user:
            # Create new user with minimal info
            try:
                user_id = User.create(
                    google_id=google_id,
                    name=user_info.get('name', email.split('@')[0]),  # Use email prefix if name not available
                    email=email,
                    profile_picture_url=user_info.get('picture', '')
                )
                logger.info(f"Created new user with email {email}")
            except Exception as e:
                logger.error(f"Error creating user: {str(e)}")
                flash('Error creating user account. Please try again.', 'error')
                return redirect(url_for('auth.login'))
        else:
            user_id = user['uuid']
            # Update user with latest info
            try:
                User.update(
                    user_id,
                    last_login=datetime.utcnow(),
                    name=user_info.get('name', user.get('name', email.split('@')[0])),
                    profile_picture_url=user_info.get('picture', user.get('profile_picture_url', ''))
                )
                logger.info(f"Updated last login for user {email}")
            except Exception as e:
                logger.error(f"Error updating user: {str(e)}")
                # Continue anyway as this is not critical

        session['user_id'] = user_id

        # Check if this was an admin panel access attempt
        next_url = session.pop('next_url', None)
        if next_url and next_url.startswith('/admin'):
            # Verify admin access
            if email in current_app.config['ADMIN_PANEL_EMAILS']:
                return redirect(next_url)
            else:
                flash('You do not have admin access.', 'error')
                return redirect(url_for('main.dashboard'))

        # Check user's organizations
        user_orgs = User.get_organizations(user_id)
        if user_orgs:
            session['current_org_id'] = user_orgs[0]['uuid']
            logger.info(f"Set current organization to {user_orgs[0]['name']}")
            
            if next_url:
                return redirect(next_url)
            return redirect(url_for('main.dashboard'))
        else:
            logger.info(f"User {email} has no organizations, redirecting to create organization")
            return redirect(url_for('auth.create_organization'))

    except Exception as e:
        logger.error(f'Error during Google authentication: {str(e)}')
        flash('Authentication failed. Please try again.', 'error')
        return redirect(url_for('auth.login'))

@bp.route('/create_organization', methods=['GET', 'POST'])
@login_required
def create_organization():
    form = CreateOrganizationForm()
    
    # Set choices from config
    form.type.choices = [(t, t) for t in current_app.config['ORGANIZATION_TYPES']]
    
    if form.validate_on_submit():
        try:
            org_id = Organization.create(
                name=form.name.data,
                org_type=form.type.data,  # This matches the config types
                website=form.website.data,
                email=form.email.data
            )
            
            User.add_to_organization(session['user_id'], org_id, role='admin')
            session['current_org_id'] = org_id
            
            flash('Organization created successfully!', 'success')
            logger.info(f"Created organization {form.name.data} for user {session['user_id']}")
            
            return redirect(url_for('main.dashboard'))
            
        except Exception as e:
            logger.error(f"Error creating organization: {str(e)}")
            flash('Error creating organization. Please try again.', 'error')
    
    return render_template('create_organization.html', form=form, config=current_app.config)

@bp.route('/switch_organization/<org_id>')
@login_required
def switch_organization(org_id):
    try:
        user = User.get_by_uuid(session['user_id'])
        if not user:
            flash('User not found.', 'error')
            return redirect(url_for('auth.logout'))
            
        user_orgs = [org['uuid'] for org in User.get_organizations(user['uuid'])]
        if org_id not in user_orgs:
            flash('You do not have access to this organization', 'error')
            return redirect(url_for('main.dashboard'))
            
        session['current_org_id'] = org_id
        org = Organization.get_by_uuid(org_id)
        flash(f'Switched to organization: {org["name"]}', 'success')
        
        return redirect(request.referrer or url_for('main.dashboard'))
        
    except Exception as e:
        logger.error(f"Error switching organization: {str(e)}")
        flash('Error switching organization. Please try again.', 'error')
        return redirect(url_for('main.dashboard'))

@bp.route('/profile')
@login_required
def profile():
    try:
        user = User.get_by_uuid(session['user_id'])
        if not user:
            flash('User not found.', 'error')
            return redirect(url_for('auth.logout'))
            
        user_orgs = User.get_organizations(user['uuid'])
        return render_template('profile.html', user=user, organizations=user_orgs)
        
    except Exception as e:
        logger.error(f"Error accessing profile: {str(e)}")
        flash('Error accessing profile. Please try again.', 'error')
        return redirect(url_for('main.dashboard'))

@bp.before_app_request
def load_logged_in_user():
    user_id = session.get('user_id')
    if user_id is None:
        g.user = None
        g.organization = None
        g.user_role = None
    else:
        try:
            g.user = User.get_by_uuid(user_id)
            if not g.user:
                session.clear()
                return redirect(url_for('auth.login'))
                
            org_id = session.get('current_org_id')
            if org_id:
                g.organization = Organization.get_by_uuid(org_id)
                g.user_role = User.get_user_role_in_organization(user_id, org_id)
            else:
                g.organization = None
                g.user_role = None
        except Exception as e:
            logger.error(f"Error loading user data: {str(e)}")
            session.clear()
            return redirect(url_for('auth.login'))

# Error handlers
@bp.app_errorhandler(404)
def not_found_error(error):
    logger.error('404 error occurred: %s', str(error))
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    logger.error('500 error occurred: %s', str(error))
    return render_template('500.html'), 500