from flask import Blueprint, render_template, redirect, url_for, current_app, g, flash, request
from app.extensions import mongo
from pymongo.errors import PyMongoError
from app.auth import login_required, admin_required
from app.models.organization import Organization
from app.models.user import User
import logging

bp = Blueprint('main', __name__)
logger = logging.getLogger(__name__)

@bp.route('/')
def index():
    try:
        if not g.user:
            return redirect(url_for('auth.login'))
            
        # Check if user has any organizations
        user_orgs = User.get_organizations(g.user['uuid'])
        if not user_orgs:
            logger.info(f"User {g.user['email']} has no organizations, redirecting to create organization")
            return redirect(url_for('auth.create_organization'))
            
        # Check if current organization is set
        if not g.organization:
            logger.info(f"No current organization set for user {g.user['email']}")
            if len(user_orgs) == 1:
                # If user has only one organization, set it as current
                org_id = user_orgs[0]['uuid']
                session['current_org_id'] = org_id
                logger.info(f"Automatically set organization {org_id} as current")
                return redirect(url_for('main.dashboard'))
            else:
                # If user has multiple organizations, let them choose
                return redirect(url_for('organizations.list_organizations'))
                
        return redirect(url_for('main.dashboard'))
        
    except Exception as e:
        logger.error(f"Error in index route: {str(e)}")
        flash('An error occurred. Please try again.', 'error')
        return redirect(url_for('auth.login'))

@bp.route('/dashboard')
@login_required
def dashboard():
    try:
        if not g.organization:
            logger.info("No active organization, redirecting to organization selection")
            return redirect(url_for('organizations.list_organizations'))

        # Count active streams for the organization
        stream_count = mongo.db.streams.count_documents({
            "deleted": {"$ne": True},
            "is_default": {"$ne": True},
            "organization_uuid": g.organization['uuid']
        })
        logger.info(f"Found {stream_count} active streams for organization {g.organization['uuid']}")
        
        if stream_count > 0:
            logger.info("Active streams found, redirecting to view_streams")
            return redirect(url_for('streams.view_streams'))
        else:
            logger.info("No active streams found, rendering empty_state")
            return render_template('empty_state.html')
            
    except PyMongoError as e:
        logger.error(f"MongoDB error in dashboard: {str(e)}", exc_info=True)
        flash('A database error occurred. Please try again.', 'error')
        return render_template('500.html'), 500
    except Exception as e:
        logger.error(f"Unexpected error in dashboard: {str(e)}", exc_info=True)
        flash('An unexpected error occurred. Please try again.', 'error')
        return render_template('500.html'), 500

@bp.route('/switch_organization/<org_uuid>')
@login_required
def switch_organization(org_uuid):
    try:
        org = Organization.get_by_uuid(org_uuid)
        if not org:
            flash('Organization not found', 'error')
            return redirect(url_for('main.dashboard'))
            
        user_orgs = [o['uuid'] for o in User.get_organizations(g.user['uuid'])]
        if org_uuid not in user_orgs:
            flash('You do not have access to this organization', 'error')
            return redirect(url_for('main.dashboard'))

        User.set_current_organization(g.user['uuid'], org_uuid)
        flash(f'Switched to organization: {org["name"]}', 'success')
        
        return redirect(url_for('main.dashboard'))
        
    except Exception as e:
        logger.error(f"Error switching organization: {str(e)}")
        flash('Error switching organization. Please try again.', 'error')
        return redirect(url_for('main.dashboard'))

@bp.route('/health')
def health_check():
    """Health check endpoint to verify the application is running."""
    try:
        # Check MongoDB connection
        mongo.db.command('ping')
        
        return {
            'status': 'healthy',
            'database': 'connected',
            'environment': current_app.config['ENV']
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            'status': 'unhealthy',
            'error': str(e),
            'environment': current_app.config['ENV']
        }, 500

# Error handlers
@bp.app_errorhandler(404)
def not_found_error(error):
    logger.error('404 error occurred: %s', str(error))
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    logger.error('500 error occurred: %s', str(error))
    return render_template('500.html'), 500