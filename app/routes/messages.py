from flask import Blueprint, jsonify, render_template, request, current_app, flash, redirect, url_for, g, send_file
from app.extensions import mongo
from datetime import datetime, timedelta
from collections import defaultdict
from math import ceil
from bson import ObjectId
import json
import os
from app.services.fhir_translator import FHIRTranslator
from app.services.health_data_converter import convert_to_fhir
from app.models.fhir_message import FHIRMessage
import uuid
from app.models.organization import Organization
from app.auth import login_required, admin_required
from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SelectField, TextAreaField
from wtforms.validators import DataRequired, NumberRange
from wtforms import RadioField
from app.tasks import parse_pasted_message
from app.utils.logging_utils import log_message_cycle

bp = Blueprint('messages', __name__)

class MessageForm(FlaskForm):
    pass  # We don't need any fields, just the CSRF protection

class MessageFilterForm(FlaskForm):
    pass

class NewMessageForm(FlaskForm):
    message_type = SelectField('Message Type', choices=[
        ('HL7v2', 'HL7v2'),
        ('FHIR', 'FHIR'),
        ('Clinical Notes', 'Clinical Notes'),
        ('CCDA', 'CCDA'),
        ('X12', 'X12')
    ], validators=[DataRequired()])
    message_content = TextAreaField('Message Content', validators=[DataRequired()])

@bp.route('/')
@login_required
def view_messages():
    if not g.organization:
        flash('Please select an organization', 'warning')
        return redirect(url_for('organizations.list_organizations'))

    form = MessageFilterForm()
    streams = list(mongo.db.streams.find({"organization_uuid": g.organization['uuid']}, {'name': 1, 'uuid': 1, 'message_type': 1}))
    
    page = int(request.args.get('page', 1))
    per_page = 20
    selected_stream_uuid = request.args.get('stream_uuid')
    message_type = request.args.get('type', 'all')

    query = {"organization_uuid": g.organization['uuid']}
    if selected_stream_uuid:
        query["stream_uuid"] = selected_stream_uuid
    if message_type != 'all':
        query["type"] = message_type
    else:
        query["$or"] = [{"type": {"$ne": "searchset"}}, {"type": {"$exists": False}}]

    total_messages = mongo.db.messages.count_documents(query)
    total_pages = ceil(total_messages / per_page)

    messages = list(mongo.db.messages.find(query)
                    .sort('timestamp', -1)
                    .skip((page - 1) * per_page)
                    .limit(per_page))

    grouped_messages = defaultdict(list)
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)

    for message in messages:
        message_date = message['timestamp'].date()
        if message_date == today:
            date_str = "Today"
        elif message_date == yesterday:
            date_str = "Yesterday"
        else:
            date_str = message_date.strftime('%Y-%m-%d')
        
        stream = next((s for s in streams if s['uuid'] == message['stream_uuid']), None)
        message['stream_name'] = stream['name'] if stream else 'Unknown Stream'
        
        if isinstance(message['message'], dict):
            message['message'] = str(message['message'])
        
        message['parsed'] = message.get('parsed', True)
        message['type'] = message.get('type', 'HL7v2')
        
        message['truncated'] = len(message['message']) > 500
        message['message'] = message['message'][:500] + '...' if message['truncated'] else message['message']
        
        grouped_messages[date_str].append(message)

    form = MessageForm()  # Create an instance of the form
    return render_template('view_messages.html', 
                           form=form,
                           grouped_messages=grouped_messages, 
                           streams=streams, 
                           selected_stream_uuid=selected_stream_uuid, 
                           message_type=message_type,
                           page=page, 
                           total_pages=total_pages)

@bp.route('/create', methods=['POST'])
@login_required
@admin_required
def create_message():
    form = NewMessageForm()
    if form.validate_on_submit():
        try:
            message_type = form.message_type.data
            message_content = form.message_content.data
            
            # Find or create the "Pasted Messages" stream for the current organization
            pasted_stream = mongo.db.streams.find_one({
                "name": "Pasted Messages",
                "organization_uuid": g.organization['uuid']
            })
            
            if not pasted_stream:
                # Create the "Pasted Messages" stream if it doesn't exist for this organization
                pasted_stream = {
                    "uuid": str(uuid.uuid4()),
                    "name": "Pasted Messages",
                    "message_type": "Mixed",
                    "active": True,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "deleted": False,
                    "is_default": True,
                    "files_processed": 0,
                    "organization_uuid": g.organization['uuid']
                }
                mongo.db.streams.insert_one(pasted_stream)
                current_app.logger.info(f"Created 'Pasted Messages' stream for organization {g.organization['uuid']}")
            
            message_uuid = str(uuid.uuid4())
            message = {
                "uuid": message_uuid,
                "stream_uuid": pasted_stream['uuid'],
                "organization_uuid": g.organization['uuid'],
                "message": message_content,
                "type": message_type,
                "timestamp": datetime.utcnow(),
                "parsed": False,
                "parsing_status": "pending",
                "conversion_status": "pending_parsing"
            }
            
            # Log the message creation
            log_message_cycle(
                message_uuid, 
                g.organization['uuid'], 
                "message_created", 
                message_type, 
                {"stream": "Pasted Messages"}, 
                "success"
            )
            
            result = mongo.db.messages.insert_one(message)
            inserted_id = str(result.inserted_id)
            
            # Log the message storage
            log_message_cycle(
                message_uuid, 
                g.organization['uuid'], 
                "message_stored", 
                message_type, 
                {"mongodb_id": inserted_id}, 
                "success"
            )
            
            # Queue the message for parsing
            try:
                parse_task = parse_pasted_message.apply_async(args=[inserted_id], queue='file_parsing')
                
                # Log the queuing for parsing
                log_message_cycle(
                    message_uuid, 
                    g.organization['uuid'], 
                    "queued_for_parsing", 
                    message_type, 
                    {"task_id": parse_task.id}, 
                    "success"
                )
            except Exception as e:
                # Log the queuing failure
                log_message_cycle(
                    message_uuid, 
                    g.organization['uuid'], 
                    "queue_for_parsing_failed", 
                    message_type, 
                    {"error": str(e)}, 
                    "error",
                    error_message=str(e)
                )
                raise
            
            return jsonify({'status': 'success', 'message': 'Message saved and queued for processing.', 'message_id': inserted_id}), 200
        except Exception as e:
            current_app.logger.error(f"Error creating message: {str(e)}", exc_info=True)
            
            # Log the overall failure
            log_message_cycle(
                message_uuid, 
                g.organization['uuid'], 
                "message_creation_failed", 
                message_type, 
                {"error": str(e)}, 
                "error",
                error_message=str(e)
            )
            
            return jsonify({'status': 'error', 'message': 'An error occurred while creating the message.'}), 500
    else:
        return jsonify({'status': 'error', 'message': 'Invalid form submission', 'errors': form.errors}), 400

@bp.route('/new', methods=['GET'])
@login_required
@admin_required
def new_message():
    form = NewMessageForm()
    return render_template('new_message.html', form=form)

@bp.route('/messages/<message_id>')
@login_required
def message_detail(message_id):
    message = mongo.db.messages.find_one({"_id": ObjectId(message_id), "type": {"$ne": "searchset"}, "organization_uuid": g.organization['uuid']})
    if not message:
        return render_template('error.html', error_message="Message not found"), 404
    
    stream = mongo.db.streams.find_one({"uuid": message['stream_uuid']})
    message['stream_name'] = stream['name'] if stream else 'Unknown Stream'
    
    message['uuid'] = message.get('uuid', str(message['_id']))
    
    message_type = message.get('type', 'HL7v2')
    fhir_translator = FHIRTranslator(current_app.config['FHIR_MAPPING_FILE'])

    if message_type in ['FHIR', 'JSON']:
        try:
            message['formatted_message'] = json.dumps(json.loads(message['message']), indent=2)
            message['readable_message'] = fhir_translator.translate(message['message'])
        except json.JSONDecodeError:
            message['formatted_message'] = message['message']
            message['readable_message'] = "Error: Invalid JSON format"
        except Exception as e:
            message['readable_message'] = f"Error translating message: {str(e)}"
    elif message_type in ['CCDA', 'X12', 'Clinical Notes']:
        message['formatted_message'] = message['message']
        message['readable_message'] = f"{message_type} content"
        
        # Add file information for SFTP files
        if 'local_path' in message:
            message['filename'] = os.path.basename(message['local_path'])
            message['file_size'] = os.path.getsize(message['local_path'])
    elif message_type == 'XML':
        message['formatted_message'] = message['message']
        message['readable_message'] = "XML parsing not implemented"
    elif message_type == 'RAW':
        message['formatted_message'] = message['message']
        message['readable_message'] = "RAW message format"
    else:  # HL7v2
        message['formatted_message'] = message['message'].replace('\r', '\n')
        
    # Add parsing and conversion status
    message['parsing_status'] = message.get('parsing_status', 'N/A')
    message['conversion_status'] = message.get('conversion_status', 'N/A')

    def get_fhir_message(message_uuid):
        return mongo.db.fhir_messages.find_one({"original_message_uuid": message_uuid, "organization_uuid": g.organization['uuid']})

    fhir_message = get_fhir_message(message['uuid'])
    if fhir_message:
        message['fhir_message'] = json.dumps(fhir_message['fhir_content'], indent=2)
        try:
            message['readable_message'] = fhir_translator.translate(fhir_message['fhir_content'])
        except Exception as e:
            message['readable_message'] = f"Error translating FHIR message: {str(e)}"
    elif message_type in ['HL7v2', 'Clinical Notes', 'CCDA', 'X12']:
        if message['conversion_status'] == 'pending':
            message['readable_message'] = f"{message_type} conversion to FHIR is pending"
        elif message['conversion_status'] == 'failed':
            message['readable_message'] = f"{message_type} conversion to FHIR failed"
        else:
            message['readable_message'] = f"{message_type} conversion to FHIR not available"

    return render_template('message_detail.html', 
                           message=message, 
                           get_fhir_message=get_fhir_message, 
                           fhir_translator=fhir_translator)

@bp.route('/messages/<message_id>/original_file')
@login_required
def serve_original_file(message_id):
    message = mongo.db.messages.find_one({"_id": ObjectId(message_id), "organization_uuid": g.organization['uuid']})
    if not message or 'local_path' not in message:
        return "File not found", 404
    
    return send_file(message['local_path'], as_attachment=False)

@bp.route('/api/search_messages')
@login_required
def search_messages():
    query = request.args.get('query', '')
    stream_uuid = request.args.get('stream_uuid')
    message_type = request.args.get('type', 'all')
    
    search_query = {"$text": {"$search": query}, "organization_uuid": g.organization['uuid']}
    if stream_uuid:
        search_query["stream_uuid"] = stream_uuid
    if message_type != 'all':
        search_query["type"] = message_type
    
    search_query['$or'] = [{"type": {"$ne": "searchset"}}, {"type": {"$exists": False}}]

    messages = list(mongo.db.messages.find(search_query)
                    .sort('timestamp', -1)
                    .limit(50))
    
    return jsonify(messages), 200

@bp.route('/search')
@login_required
def search_messages_page():
    query = request.args.get('query', '')
    stream_uuid = request.args.get('stream_uuid')
    message_type = request.args.get('type', 'all')
    
    streams = list(mongo.db.streams.find({"organization_uuid": g.organization['uuid']}, {'name': 1, 'uuid': 1, 'message_type': 1}))
    
    search_query = {"$text": {"$search": query}, "organization_uuid": g.organization['uuid']}
    if stream_uuid:
        search_query["stream_uuid"] = stream_uuid
    if message_type != 'all':
        search_query["type"] = message_type

    search_query['$or'] = [{"type": {"$ne": "searchset"}}, {"type": {"$exists": False}}]
    
    messages = list(mongo.db.messages.find(search_query)
                    .sort('timestamp', -1)
                    .limit(50))
    
    for message in messages:
        stream = next((s for s in streams if s['uuid'] == message['stream_uuid']), None)
        message['stream_name'] = stream['name'] if stream else 'Unknown Stream'
        
        if isinstance(message['message'], dict):
            message['message'] = str(message['message'])
        
        message['parsed'] = message.get('parsed', True)
    
    return render_template('search_messages.html', 
                           streams=streams,
                           messages=messages,
                           query=query,
                           selected_stream_uuid=stream_uuid,
                           message_type=message_type)

# Error handling for this blueprint
@bp.app_errorhandler(404)
def not_found_error(error):
    current_app.logger.error(f'404 error occurred: {str(error)}')
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    current_app.logger.error(f'500 error occurred: {str(error)}')
    return render_template('500.html'), 500