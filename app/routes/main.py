from flask import Blueprint, render_template, redirect, url_for, flash, g, jsonify, current_app
from app.extensions import mongo
from app.auth.decorators import login_required, admin_required
from datetime import datetime
import logging

bp = Blueprint('main', __name__)
logger = logging.getLogger(__name__)

@bp.route('/')
def index():
    """Root route handler"""
    try:
        if not g.user:
            return redirect(url_for('auth.login'))
            
        # Check if user has any organizations
        user_orgs = mongo.db.users.find_one(
            {"uuid": g.user['uuid']},
            {"organizations": 1}
        )
        
        if not user_orgs or not user_orgs.get('organizations'):
            logger.info(f"User {g.user['email']} has no organizations")
            return redirect(url_for('auth.create_organization'))
            
        # Check if current organization is set
        if not g.organization:
            logger.info(f"No current organization set for user {g.user['email']}")
            return redirect(url_for('organizations.list_organizations'))
                
        return redirect(url_for('main.view_home'))
        
    except Exception as e:
        logger.error(f"Error in index route: {str(e)}")
        flash('An error occurred. Please try again.', 'error')
        return redirect(url_for('auth.login'))

@bp.route('/home')
@login_required
def view_home():
    """Dashboard home view"""
    try:
        if not g.organization:
            logger.info("No active organization, redirecting to organization selection")
            return redirect(url_for('organizations.list_organizations'))

        return render_template('dashboard.html')
            
    except Exception as e:
        logger.error(f"Error in home view: {str(e)}", exc_info=True)
        flash('An unexpected error occurred. Please try again.', 'error')
        return render_template('500.html'), 500

@bp.route('/api/dashboard/stats')
@login_required
def get_dashboard_stats():
    """Get real-time dashboard statistics"""
    try:
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        org_uuid = g.organization['uuid']

        # Basic counts remain the same
        streams_count = mongo.db.streams_v2.count_documents({
            "organization_uuid": org_uuid,
            "deleted": {"$ne": True}
        })

        all_endpoints = mongo.db.endpoints.count_documents({
            "organization_uuid": org_uuid,
            "deleted": {"$ne": True}
        })

        source_endpoints = mongo.db.endpoints.count_documents({
            "organization_uuid": org_uuid,
            "mode": "source",
            "deleted": {"$ne": True}
        })

        destination_endpoints = mongo.db.endpoints.count_documents({
            "organization_uuid": org_uuid,
            "mode": "destination",
            "deleted": {"$ne": True}
        })

        # Get endpoint UUIDs
        source_endpoint_uuids = [
            e["uuid"] for e in mongo.db.endpoints.find(
                {
                    "organization_uuid": org_uuid,
                    "mode": "source",
                    "deleted": {"$ne": True}
                },
                {"uuid": 1}
            )
        ]

        destination_endpoint_uuids = [
            e["uuid"] for e in mongo.db.endpoints.find(
                {
                    "organization_uuid": org_uuid,
                    "mode": "destination",
                    "deleted": {"$ne": True}
                },
                {"uuid": 1}
            )
        ]

        # Count received messages from messages collection
        messages_received = mongo.db.messages.count_documents({
            "organization_uuid": org_uuid,
            "timestamp": {"$gte": today_start},
            "endpoint_uuid": {"$in": source_endpoint_uuids}
        }) if source_endpoint_uuids else 0

        # Count sent messages - counting per endpoint in destinations array
        messages_sent = 0
        if destination_endpoint_uuids:
            for endpoint_uuid in destination_endpoint_uuids:
                endpoint_sent = mongo.db.destination_messages.count_documents({
                    "organization_uuid": org_uuid,
                    "destinations": {
                        "$elemMatch": {
                            "endpoint_uuid": endpoint_uuid,
                            "status": "COMPLETED",
                            "updated_at": {"$gte": today_start}
                        }
                    }
                })
                messages_sent += endpoint_sent

        # Log statistics for debugging
        logger.debug(f"Dashboard stats for org {org_uuid}: "
                    f"received={messages_received}, sent={messages_sent}, "
                    f"source_endpoints={len(source_endpoint_uuids)}, "
                    f"dest_endpoints={len(destination_endpoint_uuids)}")

        return jsonify({
            "streams_count": streams_count,
            "endpoints_count": all_endpoints,
            "source_endpoints_count": source_endpoints,
            "destination_endpoints_count": destination_endpoints,
            "messages_received_today": messages_received,
            "messages_sent_today": messages_sent,
            "server_time": today_start.isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching dashboard stats: {str(e)}", exc_info=True)
        return jsonify({
            "error": "Failed to fetch dashboard statistics",
            "streams_count": 0,
            "endpoints_count": 0,
            "source_endpoints_count": 0,
            "destination_endpoints_count": 0,
            "messages_received_today": 0,
            "messages_sent_today": 0
        }), 500

@bp.route('/switch_organization/<org_uuid>')
@login_required
def switch_organization(org_uuid):
    """Handle organization switching"""
    try:
        org = mongo.db.organizations.find_one({"uuid": org_uuid})
        if not org:
            flash('Organization not found', 'error')
            return redirect(url_for('main.view_home'))
            
        # Verify user has access to this organization
        user = mongo.db.users.find_one({
            "uuid": g.user['uuid'],
            "organizations.uuid": org_uuid
        })
        
        if not user:
            flash('You do not have access to this organization', 'error')
            return redirect(url_for('main.view_home'))

        # Update user's current organization
        mongo.db.users.update_one(
            {"uuid": g.user['uuid']},
            {"$set": {"current_organization": org_uuid}}
        )
        
        flash(f'Switched to organization: {org["name"]}', 'success')
        return redirect(url_for('main.view_home'))
        
    except Exception as e:
        logger.error(f"Error switching organization: {str(e)}")
        flash('Error switching organization. Please try again.', 'error')
        return redirect(url_for('main.view_home'))

@bp.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        # Verify MongoDB connection
        mongo.db.command('ping')
        
        # Check Redis if used
        redis_status = 'connected'
        try:
            from app.extensions import redis_client
            redis_client.ping()
        except:
            redis_status = 'unavailable'
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'redis': redis_status,
            'environment': current_app.config['ENV']
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'environment': current_app.config['ENV']
        }), 500

# Error handlers
@bp.app_errorhandler(404)
def not_found_error(error):
    logger.error('404 error occurred: %s', str(error))
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    logger.error('500 error occurred: %s', str(error))
    return render_template('500.html'), 500

# Context processor for common data
@bp.app_context_processor
def utility_processor():
    """Utility functions available in templates"""
    def get_user_organizations():
        if g.user:
            return mongo.db.organizations.find({
                "uuid": {"$in": [org["uuid"] for org in g.user.get("organizations", [])]}
            })
        return []
        
    def get_organization_role(org_uuid):
        if g.user:
            org = next((org for org in g.user.get("organizations", []) 
                       if org["uuid"] == org_uuid), None)
            return org["role"] if org else None
        return None
        
    return dict(
        get_user_organizations=get_user_organizations,
        get_organization_role=get_organization_role
    )