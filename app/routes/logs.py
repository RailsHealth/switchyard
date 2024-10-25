from flask import Blueprint, render_template, request, current_app, redirect, url_for, flash, g, Response
from app.extensions import mongo
from datetime import datetime, timedelta
from collections import defaultdict
from math import ceil
from app.models.organization import Organization
from app.auth import login_required, admin_required
from pymongo.errors import PyMongoError
from bson import ObjectId
import csv
from io import StringIO
import pytz

bp = Blueprint('logs', __name__)

LOG_TYPES = {
    'Generic': 'logs',
    'Validation': 'validation_logs',
    'Parsing': 'parsing_logs'
}

def get_log_collection_name(log):
    """Determine which collection a log came from based on its fields"""
    if 'event' in log:
        return 'parsing_logs'
    elif 'is_valid' in log:
        return 'validation_logs'
    return 'logs'

def format_validation_message(message):
    """Format validation message for better display"""
    if not message:
        return message
    return message.replace('\\n', '\n').replace('\\t', '\t')

def get_stream_display_name(stream):
    """Format stream name with message type"""
    if not stream:
        return 'Unknown Stream'
    return f"{stream.get('name', 'Unknown')}-{stream.get('message_type', 'Unknown')}"

def enrich_log_data(log, streams):
    """Enrich log data with stream information and determine log type"""
    stream = next((s for s in streams if s.get('uuid') == log.get('stream_uuid')), None)
    
    # Enrich with stream data
    log['stream_name'] = get_stream_display_name(stream)
    log['message_type'] = stream['message_type'] if stream else 'Unknown'
    
    # Determine log type and format messages
    collection_name = get_log_collection_name(log)
    
    if collection_name == 'parsing_logs':
        log['log_type'] = 'Parsing'
        if log.get('event') == 'type_mismatch':
            log['parsing_details'] = {
                'initial_type': log.get('initial_type'),
                'detected_type': log.get('detected_type'),
                'confidence': log.get('confidence')
            }
    elif collection_name == 'validation_logs':
        log['log_type'] = 'Validation'
        log['validation_message'] = format_validation_message(log.get('validation_message', ''))
    else:
        log['log_type'] = 'Generic'
    
    # Ensure timezone is preserved in timestamp
    if 'timestamp' in log and isinstance(log['timestamp'], datetime):
        if log['timestamp'].tzinfo is None:
            log['timestamp'] = pytz.UTC.localize(log['timestamp'])
    
    # Check for message truncation
    message = log.get('message', '')
    if isinstance(message, str) and len(message) > 1000:
        log['truncated'] = True
        log['full_message'] = message
        log['message'] = message[:1000] + '...'
    
    return log

def get_filtered_logs(org_uuid, stream_uuid=None, log_type='all', page=1, per_page=20):
    """Get filtered and paginated logs from all collections"""
    try:
        base_query = {"organization_uuid": org_uuid}
        if stream_uuid:
            base_query["stream_uuid"] = stream_uuid

        all_logs = []

        # Determine which collections to query based on log_type
        if log_type == 'all' or log_type not in LOG_TYPES:
            # Query all collections
            all_logs.extend(list(mongo.db.logs.find(base_query)))
            
            parsing_logs = list(mongo.db.parsing_logs.find(base_query))
            for log in parsing_logs:
                log['collection'] = 'parsing_logs'
            all_logs.extend(parsing_logs)
            
            validation_logs = list(mongo.db.validation_logs.find(base_query))
            for log in validation_logs:
                log['collection'] = 'validation_logs'
                if 'message_id' in log:
                    message = mongo.db.messages.find_one({'_id': log['message_id']})
                    if message:
                        log['stream_uuid'] = message.get('stream_uuid')
                        log['message_type'] = message.get('type')
                        log['message'] = log.get('validation_message')
            all_logs.extend(validation_logs)
        else:
            # Query specific collection based on log_type
            collection = mongo.db[LOG_TYPES[log_type]]
            logs = list(collection.find(base_query))
            for log in logs:
                log['collection'] = LOG_TYPES[log_type]
                if log_type == 'Validation' and 'message_id' in log:
                    message = mongo.db.messages.find_one({'_id': log['message_id']})
                    if message:
                        log['stream_uuid'] = message.get('stream_uuid')
                        log['message_type'] = message.get('type')
                        log['message'] = log.get('validation_message')
            all_logs.extend(logs)

        # Sort merged logs by timestamp
        all_logs.sort(key=lambda x: x['timestamp'], reverse=True)

        # Calculate pagination
        total_logs = len(all_logs)
        total_pages = ceil(total_logs / per_page)
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        
        return all_logs[start_index:end_index], total_logs, total_pages

    except PyMongoError as e:
        current_app.logger.error(f"Database error in get_filtered_logs: {str(e)}")
        raise

@bp.route('/')
@login_required
@admin_required
def view_logs():
    if not g.organization:
        flash('Please select an organization', 'warning')
        return redirect(url_for('organizations.list_organizations'))

    try:
        # Get streams for the organization
        streams = list(mongo.db.streams.find(
            {"organization_uuid": g.organization['uuid']}, 
            {'name': 1, 'uuid': 1, 'message_type': 1}
        ))
        
        # Get filter parameters
        page = int(request.args.get('page', 1))
        selected_stream_uuid = request.args.get('stream_uuid')
        log_type = request.args.get('type', 'all')

        # Get filtered and paginated logs
        logs, total_logs, total_pages = get_filtered_logs(
            g.organization['uuid'],
            selected_stream_uuid,
            log_type,
            page
        )

        # Group logs by date
        grouped_logs = defaultdict(list)
        today = datetime.now(pytz.UTC).date()
        yesterday = today - timedelta(days=1)

        for log in logs:
            # Enrich log data
            enriched_log = enrich_log_data(log, streams)
            
            # Group by date (handle timezone-aware timestamps)
            log_date = enriched_log['timestamp'].astimezone(pytz.UTC).date()
            if log_date == today:
                date_str = "Today"
            elif log_date == yesterday:
                date_str = "Yesterday"
            else:
                date_str = log_date.strftime('%Y-%m-%d')
            
            grouped_logs[date_str].append(enriched_log)

        return render_template('view_logs.html',
                             streams=streams,
                             grouped_logs=dict(grouped_logs),
                             page=page,
                             total_pages=total_pages,
                             selected_stream_uuid=selected_stream_uuid,
                             log_type=log_type,
                             log_types=list(LOG_TYPES.keys()))

    except PyMongoError as e:
        current_app.logger.error(f"Database error in view_logs: {str(e)}")
        flash("An error occurred while fetching logs.", "error")
        return render_template('error.html'), 500

@bp.route('/stream/<stream_uuid>')
@login_required
@admin_required
def stream_logs(stream_uuid):
    if not g.organization:
        flash('Please select an organization', 'warning')
        return redirect(url_for('organizations.list_organizations'))

    try:
        stream = mongo.db.streams.find_one({"uuid": stream_uuid, "organization_uuid": g.organization['uuid']})
        if not stream:
            flash('Stream not found', 'error')
            return redirect(url_for('logs.view_logs'))

        page = int(request.args.get('page', 1))
        log_type = request.args.get('type', 'all')

        # Get filtered and paginated logs for specific stream
        logs, total_logs, total_pages = get_filtered_logs(
            g.organization['uuid'],
            stream_uuid,
            log_type,
            page
        )

        return render_template('stream_logs.html',
                             stream=stream,
                             logs=logs,
                             page=page,
                             total_pages=total_pages,
                             log_type=log_type)

    except PyMongoError as e:
        current_app.logger.error(f"Database error in stream_logs: {str(e)}")
        flash("An error occurred while fetching stream logs.", "error")
        return render_template('error.html'), 500

@bp.route('/download')
@login_required
@admin_required
def download_logs():
    if not g.organization:
        flash('Please select an organization', 'warning')
        return redirect(url_for('organizations.list_organizations'))

    try:
        stream_uuid = request.args.get('stream_uuid')
        log_type = request.args.get('type', 'all')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        # Get all logs without pagination
        base_query = {"organization_uuid": g.organization['uuid']}
        if stream_uuid:
            base_query["stream_uuid"] = stream_uuid
        if start_date and end_date:
            base_query["timestamp"] = {
                "$gte": datetime.strptime(start_date, '%Y-%m-%d'),
                "$lte": datetime.strptime(end_date, '%Y-%m-%d')
            }

        # Get streams for enrichment
        streams = list(mongo.db.streams.find(
            {"organization_uuid": g.organization['uuid']}, 
            {'name': 1, 'uuid': 1, 'message_type': 1}
        ))

        # Fetch and merge logs from all collections
        all_logs = []
        all_logs.extend(list(mongo.db.logs.find(base_query)))
        all_logs.extend(list(mongo.db.parsing_logs.find(base_query)))
        validation_logs = list(mongo.db.validation_logs.find(base_query))
        for log in validation_logs:
            log['message'] = log.get('validation_message', '')
        all_logs.extend(validation_logs)

        # Sort by timestamp
        all_logs.sort(key=lambda x: x['timestamp'], reverse=True)

        # Filter by log type if specified
        if log_type != 'all':
            all_logs = [log for log in all_logs if (
                log.get('log_type') == log_type or 
                log.get('message_type') == log_type
            )]

        # Enrich logs with stream information
        enriched_logs = [enrich_log_data(log, streams) for log in all_logs]

        # Create CSV in memory
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["Timestamp", "Stream", "Log Type", "Message Type", "Message", "Details"])

        for log in enriched_logs:
            details = ""
            if log.get('log_type') == 'Parsing':
                if log.get('event') == 'type_mismatch':
                    details = (f"Initial Type: {log.get('initial_type')}, "
                             f"Detected Type: {log.get('detected_type')}, "
                             f"Confidence: {log.get('confidence')}")
            elif log.get('log_type') == 'Validation':
                details = f"Valid: {log.get('is_valid')}"

            writer.writerow([
                log['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                log['stream_name'],
                log.get('log_type', 'Generic'),
                log.get('message_type', ''),
                log.get('message', ''),
                details
            ])

        # Create response
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={
                'Content-Disposition': 'attachment; filename=logs.csv',
                'Content-Type': 'text/csv; charset=utf-8'
            }
        )

    except PyMongoError as e:
        current_app.logger.error(f"Database error in download_logs: {str(e)}")
        flash("An error occurred while downloading logs.", "error")
        return render_template('error.html'), 500

# Error handling for this blueprint
@bp.app_errorhandler(404)
def not_found_error(error):
    current_app.logger.error(f'404 error occurred: {str(error)}')
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    current_app.logger.error(f'500 error occurred: {str(error)}')
    return render_template('500.html'), 500