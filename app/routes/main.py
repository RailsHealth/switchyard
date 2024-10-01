from flask import Blueprint, render_template, redirect, url_for, current_app, g, flash, request
from app.extensions import mongo
from pymongo.errors import PyMongoError
from app.middleware import login_required, admin_required
from app.models.organization import Organization
from app.models.user import User

bp = Blueprint('main', __name__)

@bp.route('/')
def index():
    if not g.user:
        return redirect(url_for('auth.login'))
    return redirect(url_for('main.dashboard'))

@bp.route('/dashboard')
@login_required
def dashboard():
    current_app.logger.info("Entering main dashboard route")
    try:
        if not g.organization:
            current_app.logger.info("No active organization, redirecting to organization selection")
            return redirect(url_for('organizations.list_organizations'))

        stream_count = mongo.db.streams.count_documents({
            "deleted": {"$ne": True},
            "is_default": {"$ne": True},
            "organization_uuid": g.organization['uuid']
        })
        current_app.logger.info(f"Found {stream_count} non-deleted, non-default streams for organization {g.organization['uuid']}")
        
        if stream_count > 0:
            current_app.logger.info("Active streams found, redirecting to view_streams")
            return redirect(url_for('streams.view_streams'))
        else:
            current_app.logger.info("No active streams found, rendering empty_state.html")
            return render_template('empty_state.html')
    except PyMongoError as e:
        current_app.logger.error(f"MongoDB error in main dashboard route: {str(e)}", exc_info=True)
        return render_template('500.html', error_message="A database error occurred while loading the dashboard"), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error in main dashboard route: {str(e)}", exc_info=True)
        return render_template('500.html', error_message="An unexpected error occurred while loading the dashboard"), 500

@bp.route('/health')
def health_check():
    """
    A simple health check endpoint to verify the application is running.
    """
    return "OK", 200

@bp.route('/switch_organization/<org_uuid>')
@login_required
def switch_organization(org_uuid):
    org = Organization.get_by_uuid(org_uuid)
    if not org or org_uuid not in [o['uuid'] for o in g.user['organizations']]:
        flash('Invalid organization selection', 'error')
        return redirect(url_for('main.dashboard'))

    User.set_current_organization(g.user['uuid'], org_uuid)
    flash(f'Switched to organization: {org["name"]}', 'success')
    return redirect(url_for('main.dashboard'))

# Error handling for this blueprint
@bp.app_errorhandler(404)
def not_found_error(error):
    current_app.logger.error('404 error occurred: %s', str(error))
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    current_app.logger.error('500 error occurred: %s', str(error))
    return render_template('500.html'), 500