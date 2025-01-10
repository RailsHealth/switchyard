# app/routes/settings.py

from flask import Blueprint, render_template, g, flash, redirect, url_for
from app.auth import login_required
from pymongo.errors import PyMongoError
import logging

# Initialize blueprint with url_prefix
bp = Blueprint('settings', __name__)
logger = logging.getLogger(__name__)

@bp.route('/')
@login_required
def view_settings():
    """View settings page (coming soon)"""
    try:
        if not g.organization:
            logger.info("No active organization, redirecting to organization selection")
            return redirect(url_for('organizations.list_organizations'))

        logger.info(f"Rendering settings view for org: {g.organization['uuid']}")
        return render_template(
            'coming_soon.html',
            page_title="Settings",
            page_description="application settings and configurations"
        )

    except Exception as e:
        logger.error(f"Error in view_settings: {str(e)}", exc_info=True)
        flash('An unexpected error occurred.', 'error')
        return redirect(url_for('main.view_home'))