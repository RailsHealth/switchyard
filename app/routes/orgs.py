# app/routes/orgs.py

from flask import Blueprint, render_template, g, flash, redirect, url_for
from app.auth import login_required
import logging

# Initialize blueprint
bp = Blueprint('orgs', __name__)
logger = logging.getLogger(__name__)

@bp.route('/')
@login_required
def view_orgs():
    """Organizations management page (coming soon)"""
    try:
        if not g.organization:
            logger.info("No active organization, redirecting to organization selection")
            return redirect(url_for('organizations.list_organizations'))

        logger.info(f"Rendering organizations view for org: {g.organization['uuid']}")
        return render_template(
            'coming_soon.html',
            page_title="Organizations",
            page_description="organization management and collaboration"
        )

    except Exception as e:
        logger.error(f"Error in view_orgs: {str(e)}", exc_info=True)
        flash('An unexpected error occurred.', 'error')
        return redirect(url_for('main.view_home'))

# Catch all other organization-related routes
@bp.route('/<path:subpath>')
@login_required
def catch_all(subpath):
    """Catch all other organization routes"""
    return redirect(url_for('orgs.view_orgs'))