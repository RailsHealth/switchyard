# app/routes/templates.py

from flask import Blueprint, render_template, g, flash, redirect, url_for
from app.auth import login_required
from pymongo.errors import PyMongoError
import logging

# Initialize blueprint with url_prefix
bp = Blueprint('templates', __name__)
logger = logging.getLogger(__name__)

@bp.route('/')
@login_required
def view_templates():
    """View templates page (coming soon)"""
    try:
        if not g.organization:
            logger.info("No active organization, redirecting to organization selection")
            return redirect(url_for('organizations.list_organizations'))

        logger.info(f"Rendering templates view for org: {g.organization['uuid']}")
        return render_template(
            'coming_soon.html',
            page_title="Templates",
            page_description="message templates and formats"
        )

    except Exception as e:
        logger.error(f"Error in view_templates: {str(e)}", exc_info=True)
        flash('An unexpected error occurred.', 'error')
        return redirect(url_for('main.view_home'))