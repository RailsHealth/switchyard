# app/routes/logs.py

from flask import Blueprint, render_template, request, current_app, redirect, url_for, flash, g
from app.extensions import mongo
from datetime import datetime, timedelta
from collections import defaultdict
from math import ceil
from app.models.organization import Organization
from app.auth import login_required, admin_required
from pymongo.errors import PyMongoError

bp = Blueprint('logs', __name__)

@bp.route('/')
@login_required
@admin_required
def view_logs():
    if not g.organization:
        flash('Please select an organization', 'warning')
        return redirect(url_for('organizations.list_organizations'))

    try:
        streams = list(mongo.db.streams.find({"organization_uuid": g.organization['uuid']}, {'name': 1, 'uuid': 1, 'message_type': 1}))
        
        page = int(request.args.get('page', 1))
        per_page = 20
        selected_stream_uuid = request.args.get('stream_uuid')
        log_type = request.args.get('type', 'all')

        base_query = {"organization_uuid": g.organization['uuid']}
        if selected_stream_uuid:
            base_query["stream_uuid"] = selected_stream_uuid

        # Fetch logs from all collections
        logs = list(mongo.db.logs.find(base_query))
        logs.extend(mongo.db.parsing_logs.find(base_query))
        logs.extend(mongo.db.validation_logs.find(base_query))

        # Sort merged logs by timestamp
        logs.sort(key=lambda x: x['timestamp'], reverse=True)

        # Apply log type filter after merging
        if log_type != 'all':
            logs = [log for log in logs if log.get('log_type') == log_type]

        total_logs = len(logs)
        total_pages = ceil(total_logs / per_page)

        # Paginate logs
        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        paginated_logs = logs[start_index:end_index]

        grouped_logs = defaultdict(list)
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)

        for log in paginated_logs:
            log_date = log['timestamp'].date()
            if log_date == today:
                date_str = "Today"
            elif log_date == yesterday:
                date_str = "Yesterday"
            else:
                date_str = log_date.strftime('%Y-%m-%d')
            
            # Add stream name and message type to the log object
            stream = next((s for s in streams if s['uuid'] == log.get('stream_uuid')), None)
            log['stream_name'] = stream['name'] if stream else 'Unknown Stream'
            log['message_type'] = stream['message_type'] if stream else 'Unknown'
            
            # Add log type if not present
            if 'log_type' not in log:
                if 'event' in log:
                    log['log_type'] = 'Parsing'
                elif 'is_valid' in log:
                    log['log_type'] = 'Validation'
                else:
                    log['log_type'] = 'Generic'
            
            grouped_logs[date_str].append(log)

        log_types = ['Generic', 'Parsing', 'Validation', 'HL7v2', 'FHIR', 'CCDA', 'X12', 'Clinical Notes']

        return render_template('view_logs.html', 
                               streams=streams, 
                               grouped_logs=dict(grouped_logs),
                               page=page,
                               total_pages=total_pages,
                               selected_stream_uuid=selected_stream_uuid,
                               log_type=log_type,
                               log_types=log_types)
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
        per_page = 20
        log_type = request.args.get('type', 'all')

        query = {"stream_uuid": stream_uuid, "organization_uuid": g.organization['uuid']}
        if log_type != 'all':
            query["log_type"] = log_type

        total_logs = mongo.db.logs.count_documents(query)
        total_pages = ceil(total_logs / per_page)

        logs = list(mongo.db.logs.find(query)
                    .sort('timestamp', -1)
                    .skip((page - 1) * per_page)
                    .limit(per_page))

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

        query = {"organization_uuid": g.organization['uuid']}
        if stream_uuid:
            query["stream_uuid"] = stream_uuid
        if log_type != 'all':
            query["log_type"] = log_type
        if start_date and end_date:
            query["timestamp"] = {
                "$gte": datetime.strptime(start_date, '%Y-%m-%d'),
                "$lte": datetime.strptime(end_date, '%Y-%m-%d')
            }

        logs = list(mongo.db.logs.find(query).sort('timestamp', -1))

        # Generate CSV content
        csv_content = "Timestamp,Stream,Log Type,Message\n"
        for log in logs:
            csv_content += f"{log['timestamp']},{log.get('stream_name', 'N/A')},{log.get('log_type', 'N/A')},{log['message']}\n"

        # Create response
        response = current_app.response_class(csv_content, mimetype='text/csv')
        response.headers.set('Content-Disposition', 'attachment', filename='logs.csv')

        return response
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