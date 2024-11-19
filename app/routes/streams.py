from flask import Blueprint, request, jsonify, render_template, redirect, url_for, current_app, flash, g
from app.extensions import mongo
from app.models.hl7_interface import HL7v2Interface
from app.models.fhir_interface import FHIRInterface
from app.models.sftp_interface import SFTPInterface
from datetime import datetime
import urllib.parse
import uuid
from config import Config
from bson import json_util
import json
from math import ceil
from app.models.organization import Organization
from app.services.fhir_service import fhir_interfaces, initialize_fhir_interfaces
from app.auth import login_required, admin_required
from pymongo.errors import PyMongoError
from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SelectField
from wtforms.validators import DataRequired, NumberRange
from wtforms import RadioField
from app.services.test_hl7_client import TestHL7Client

bp = Blueprint('streams', __name__)

hl7_interfaces = {}
fhir_interfaces = {}
sftp_interfaces = {}

class FilterForm(FlaskForm):
    pass  # We don't need any fields, just the CSRF protection

class DeleteForm(FlaskForm):
    pass  # We don't need any fields, just the CSRF protection

class FHIRStreamForm(FlaskForm):
    pass  # We don't need any fields, just the CSRF protection

class HL7v2StreamForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired()])
    host = StringField('Host', validators=[DataRequired()])
    port = IntegerField('Port', validators=[DataRequired(), NumberRange(min=1, max=65535)])
    timeout = IntegerField('Timeout', validators=[DataRequired(), NumberRange(min=1)])

class SFTPStreamForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired()])
    host = StringField('Host', validators=[DataRequired()])
    port = IntegerField('Port', validators=[DataRequired(), NumberRange(min=1, max=65535)])
    username = StringField('Username', validators=[DataRequired()])
    auth_method = SelectField('Authentication Method', choices=[('password', 'Password'), ('key', 'SSH Key')])
    password = StringField('Password')
    private_key = StringField('Private Key')
    remote_path = StringField('Remote Path', validators=[DataRequired()])
    file_pattern = StringField('File Pattern')
    fetch_interval = IntegerField('Fetch Interval', validators=[DataRequired(), NumberRange(min=1)])

class CreateStreamForm(FlaskForm):
    message_type = RadioField('Message Type', choices=[
        ('HL7v2', 'HL7v2'),
        ('FHIR Test Server', 'FHIR Test Server'),
        ('test_hl7v2', 'Test HL7v2 Stream'),
        ('CCDA (SFTP)', 'CCDA (SFTP)'),
        ('X12 (SFTP)', 'X12 (SFTP)'),
        ('Clinical Notes (SFTP)', 'Clinical Notes (SFTP)')
    ], validators=[DataRequired()])

@bp.route('/view_streams', methods=['GET'])
@login_required
def view_streams():
    current_app.logger.info("Entering view_streams route")
    try:
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        form = FilterForm()  # Create an instance of the form
        page = int(request.args.get('page', 1))
        per_page = 10
        status_filter = request.args.get('status', 'all')
        type_filter = request.args.get('type', 'all')

        base_query = {"is_default": {"$ne": True}, "organization_uuid": g.organization['uuid']}
        if status_filter == 'active':
            base_query["deleted"] = {"$ne": True}
        elif status_filter == 'deleted':
            base_query["deleted"] = True

        if type_filter != 'all':
            base_query["message_type"] = type_filter

        total_streams = mongo.db.streams.count_documents(base_query)
        total_pages = ceil(total_streams / per_page)

        streams = list(mongo.db.streams.find(base_query)
                       .sort([("deleted", 1), ("name", 1)])
                       .skip((page - 1) * per_page)
                       .limit(per_page))
        
        current_app.logger.info(f"Retrieved {len(streams)} streams")
        
        pasted_messages_stream = get_pasted_messages_stream(g.organization['uuid'])
        
        return render_template('view_streams.html', 
                               form=form,  # Pass the form to the template
                               streams=streams,
                               pasted_messages_stream=pasted_messages_stream,
                               page=page,
                               total_pages=total_pages,
                               status_filter=status_filter,
                               type_filter=type_filter)
    except PyMongoError as e:
        current_app.logger.error(f"Database error in view_streams: {str(e)}", exc_info=True)
        flash('A database error occurred while retrieving streams.', 'error')
        return render_template('500.html', error_message="A database error occurred while retrieving streams"), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error in view_streams: {str(e)}", exc_info=True)
        flash('An unexpected error occurred while retrieving streams.', 'error')
        return render_template('500.html', error_message="An unexpected error occurred while retrieving streams"), 500

@bp.route('/create', methods=['GET', 'POST'])
@login_required
@admin_required
def create_stream():
    """Create a new stream, including test HL7v2 streams"""
    form = CreateStreamForm()
    
    if request.method == 'POST' and form.validate_on_submit():
        message_type = form.message_type.data
        
        try:
            if message_type == 'test_hl7v2':
                # Check if org already has a test stream
                existing_stream = mongo.db.streams.find_one({
                    "organization_uuid": g.organization['uuid'],
                    "is_test_stream": True,
                    "message_type": "HL7v2",
                    "deleted": {"$ne": True}
                })
                
                if existing_stream:
                    flash('Test HL7v2 stream already exists for this organization', 'error')
                    return redirect(url_for('streams.view_streams'))

                # Create test stream via service
                client = TestHL7Client()
                result = client.create_test_stream(g.organization['uuid'])
                
                if not result or not result.get('port'):
                    flash('Failed to get port from test service', 'error')
                    return redirect(url_for('streams.view_streams'))
                
                # Parse service URL to get host
                service_url = current_app.config['TEST_HL7_SERVICE_URL']
                mllp_host = current_app.config['TEST_HL7_SERVICE_MLLP_HOST']
                
                # Store stream info
                stream_data = {
                    "uuid": str(uuid.uuid4()),
                    "organization_uuid": g.organization['uuid'],
                    "name": "Test HL7v2 Stream",
                    "host": mllp_host,
                    "service_url": service_url,
                    "port": result['port'],
                    "message_type": "HL7v2",
                    "is_test_stream": True,
                    "connection_type": "mllp",
                    "active": False,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "last_active": None,
                    "deleted": False,
                    "timeout": 30
                }
                
                mongo.db.streams.insert_one(stream_data)
                current_app.logger.info(f"Test HL7v2 Stream created: {stream_data}")
                flash('Test HL7v2 Stream created successfully', 'success')
                return redirect(url_for('streams.view_streams'))
                
            elif message_type == 'HL7v2':
                return redirect(url_for('streams.create_hl7v2_stream'))
            elif message_type == 'FHIR Test Server':
                return redirect(url_for('streams.create_fhir_stream'))
            elif message_type in ['CCDA (SFTP)', 'X12 (SFTP)', 'Clinical Notes (SFTP)']:
                return redirect(url_for('streams.create_sftp_stream', stream_type=message_type))
                
        except Exception as e:
            current_app.logger.error(f"Error creating stream: {str(e)}")
            flash('Error creating stream', 'error')
            return redirect(url_for('streams.view_streams'))
    
    # Check if test stream exists for organization
    has_test_stream = mongo.db.streams.find_one({
        "organization_uuid": g.organization['uuid'],
        "is_test_stream": True,
        "message_type": "HL7v2",
        "deleted": {"$ne": True}
    }) is not None
    
    return render_template('create_stream.html', form=form, has_test_stream=has_test_stream)


@bp.route('/create/hl7v2', methods=['GET', 'POST'])
@login_required
@admin_required
def create_hl7v2_stream():
    form = HL7v2StreamForm()
    if form.validate_on_submit():
        try:
            # Check for existing stream with same name or host/port combination
            existing_stream = mongo.db.streams.find_one({
                "$or": [
                    {"name": form.name.data},
                    {"$and": [{"host": form.host.data}, {"port": form.port.data}]}
                ],
                "organization_uuid": g.organization['uuid'],
                "deleted": {"$ne": True}
            })
            if existing_stream:
                if existing_stream['name'] == form.name.data:
                    flash('A stream with this name already exists.', 'error')
                else:
                    flash('A stream with this host and port combination already exists.', 'error')
                return render_template('create_hl7v2_stream.html', form=form)

            stream_uuid = str(uuid.uuid4())
            stream_data = {
                "uuid": stream_uuid,
                "organization_uuid": g.organization['uuid'],
                "name": form.name.data,
                "host": form.host.data,
                "port": form.port.data,
                "timeout": form.timeout.data,
                "message_type": "HL7v2",
                "connection_type": "mllp",
                "active": False,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "last_active": None,
                "deleted": False
            }
            mongo.db.streams.insert_one(stream_data)
            current_app.logger.info(f"HL7v2 Stream created: {stream_data}")
            flash('HL7v2 Stream created successfully', 'success')
            return redirect(url_for('streams.view_streams'))
        except PyMongoError as e:
            current_app.logger.error(f"Database error creating HL7v2 stream: {str(e)}", exc_info=True)
            flash('A database error occurred while creating the stream.', 'error')
            return render_template('500.html', error_message="A database error occurred while creating the stream"), 500
        except Exception as e:
            current_app.logger.error(f"Unexpected error creating HL7v2 stream: {str(e)}", exc_info=True)
            flash('An unexpected error occurred while creating the stream.', 'error')
            return render_template('500.html', error_message="An unexpected error occurred while creating the stream"), 500

    return render_template('create_hl7v2_stream.html', form=form)

@bp.route('/create/sftp/<stream_type>', methods=['GET', 'POST'])
@login_required
@admin_required
def create_sftp_stream(stream_type):
    form = SFTPStreamForm()
    if form.validate_on_submit():
        try:
            # Check for existing stream with same name
            existing_stream = mongo.db.streams.find_one({
                "name": form.name.data,
                "organization_uuid": g.organization['uuid'],
                "deleted": {"$ne": True}
            })
            if existing_stream:
                flash('A stream with this name already exists.', 'error')
                return render_template('create_sftp_stream.html', stream_type=stream_type, form=form)

            stream_uuid = str(uuid.uuid4())
            message_type = Config.STREAM_TO_MESSAGE_TYPE.get(stream_type)
            stream_data = {
                "uuid": stream_uuid,
                "organization_uuid": g.organization['uuid'],
                "name": form.name.data,
                "host": form.host.data,
                "port": form.port.data,
                "username": form.username.data,
                "auth_method": form.auth_method.data,
                "password": form.password.data if form.auth_method.data == 'password' else None,
                "private_key": form.private_key.data if form.auth_method.data == 'key' else None,
                "remote_path": form.remote_path.data,
                "file_pattern": form.file_pattern.data,
                "fetch_interval": form.fetch_interval.data,
                "message_type": message_type,
                "stream_type": stream_type,
                "connection_type": "sftp",
                "active": False,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "last_active": None,
                "deleted": False,
                "files_processed": 0
            }
            mongo.db.streams.insert_one(stream_data)
            current_app.logger.info(f"SFTP Stream created: {stream_data}")
            flash('SFTP Stream created successfully', 'success')
            return redirect(url_for('streams.view_streams'))
        except PyMongoError as e:
            current_app.logger.error(f"Database error creating SFTP stream: {str(e)}", exc_info=True)
            flash('A database error occurred while creating the SFTP stream.', 'error')
            return render_template('500.html', error_message="A database error occurred while creating the SFTP stream"), 500
        except Exception as e:
            current_app.logger.error(f"Unexpected error creating SFTP stream: {str(e)}", exc_info=True)
            flash('An unexpected error occurred while creating the SFTP stream.', 'error')
            return render_template('500.html', error_message="An unexpected error occurred while creating the SFTP stream"), 500

    return render_template('create_sftp_stream.html', stream_type=stream_type, form=form)

@bp.route('/create/fhir', methods=['GET', 'POST'])
@login_required
@admin_required
def create_fhir_stream():
    form = FHIRStreamForm()

    if request.method == 'GET':
        # Fetch existing FHIR streams for the organization
        existing_streams = mongo.db.streams.find({
            "message_type": "FHIR",
            "deleted": {"$ne": True},
            "organization_uuid": g.organization['uuid']
        })
        existing_fhir_streams = [stream['name'].split()[0].lower() for stream in existing_streams]
        
        return render_template('create_fhir_stream.html', form=form, existing_fhir_streams=existing_fhir_streams)

    if form.validate_on_submit():
        try:
            data = request.form
            servers = data.get('servers', '').split(',')
            
            created_streams = []
            updated_streams = []
            for server in servers:
                if server not in Config.FHIR_SERVERS:
                    return jsonify({'status': 'error', 'message': f'Invalid FHIR server: {server}'}), 400
                
                server_config = Config.FHIR_SERVERS[server]
                existing_stream = mongo.db.streams.find_one({
                    "name": f"{server.capitalize()} FHIR Server",
                    "organization_uuid": g.organization['uuid'],
                    "deleted": {"$ne": True}
                })

                if existing_stream:
                    # Update existing stream
                    mongo.db.streams.update_one(
                        {"_id": existing_stream['_id']},
                        {"$set": {
                            "url": server_config['url'],
                            "fhir_version": server_config['version'],
                            "updated_at": datetime.utcnow(),
                            "deleted": False
                        }}
                    )
                    updated_streams.append(server)
                else:
                    # Create new stream
                    stream_uuid = str(uuid.uuid4())
                    stream_data = {
                        "uuid": stream_uuid,
                        "organization_uuid": g.organization['uuid'],
                        "name": f"{server.capitalize()} FHIR Server",
                        "url": server_config['url'],
                        "fhir_version": server_config['version'],
                        "message_type": "FHIR",
                        "connection_type": "fhir",
                        "active": False,
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                        "last_active": None,
                        "deleted": False
                    }
                    mongo.db.streams.insert_one(stream_data)
                    created_streams.append(server)
                
                current_app.logger.info(f"FHIR Stream created/updated: {server}")
            
            # Delete streams that were not in the selection
            mongo.db.streams.update_many(
                {
                    "organization_uuid": g.organization['uuid'],
                    "message_type": "FHIR",
                    "name": {"$nin": [f"{s.capitalize()} FHIR Server" for s in servers]}
                },
                {"$set": {"deleted": True, "updated_at": datetime.utcnow()}}
            )
            
            message = []
            if created_streams:
                message.append(f"Created: {', '.join(created_streams)}")
            if updated_streams:
                message.append(f"Updated: {', '.join(updated_streams)}")
            
            return jsonify({'status': 'success', 'message': ' | '.join(message)}), 200
        
        except PyMongoError as e:
            current_app.logger.error(f"Database error managing FHIR streams: {str(e)}", exc_info=True)
            return jsonify({'status': 'error', 'message': 'A database error occurred while managing the FHIR streams'}), 500
        except Exception as e:
            current_app.logger.error(f"Unexpected error managing FHIR streams: {str(e)}", exc_info=True)
            return jsonify({'status': 'error', 'message': 'An unexpected error occurred while managing the FHIR streams'}), 500
    
    # If form validation fails
    return jsonify({'status': 'error', 'message': 'Form validation failed'}), 400

@bp.route('/<uuid>/start', methods=['POST'])
@login_required
@admin_required
def start_stream(uuid):
    """Start a stream, handling both regular and test streams"""
    try:
        stream = mongo.db.streams.find_one({
            "uuid": uuid, 
            "deleted": {"$ne": True}, 
            "organization_uuid": g.organization['uuid']
        })
        
        if not stream:
            return jsonify({'status': 'error', 'message': 'Stream not found or has been deleted'}), 404

        if stream['active']:
            return jsonify({'status': 'error', 'message': 'Stream is already active'}), 400

        # Set stream to 'starting' state
        mongo.db.streams.update_one({"uuid": uuid}, {"$set": {"active": "starting"}})

        try:
            if stream.get('is_test_stream') and stream['message_type'] == 'HL7v2':
                # First start the test service stream
                client = TestHL7Client()
                result = client.start_stream(stream['organization_uuid'])
                
                if not result or result.get('status') != 'success':
                    raise Exception('Failed to start test stream service')
                
                # Then create a local interface to listen for messages
                if uuid not in hl7_interfaces:
                    hl7_interface = HL7v2Interface(
                        stream_uuid=uuid,
                        host=stream['host'],  # Using TEST_HL7_SERVICE_MLLP_HOST
                        port=stream['port'],
                        organization_uuid=stream['organization_uuid'],
                        timeout=stream.get('timeout', 30)
                    )
                    hl7_interfaces[uuid] = hl7_interface
                
                try:
                    hl7_interfaces[uuid].start_listening()
                except Exception as e:
                    # If local interface fails to start, stop the test service
                    client.stop_stream(stream['organization_uuid'])
                    raise Exception(f'Failed to start local interface: {str(e)}')
                    
            elif stream['message_type'] == 'HL7v2':
                # Handle regular HL7v2 stream (no changes needed here)
                if uuid not in hl7_interfaces:
                    hl7_interface = HL7v2Interface(
                        stream_uuid=uuid,
                        host=stream['host'],
                        port=stream['port'],
                        organization_uuid=stream['organization_uuid'],
                        timeout=stream['timeout']
                    )
                    hl7_interfaces[uuid] = hl7_interface
                hl7_interfaces[uuid].start_listening()
            
            # Update stream status
            mongo.db.streams.update_one(
                {"uuid": uuid}, 
                {
                    "$set": {
                        "active": True,
                        "last_active": datetime.utcnow()
                    }
                }
            )
            
            return jsonify({
                'status': 'success',
                'message': 'Stream started successfully'
            }), 200
            
        except Exception as e:
            # Reset stream state on error and cleanup
            mongo.db.streams.update_one({"uuid": uuid}, {"$set": {"active": False}})
            
            if uuid in hl7_interfaces:
                try:
                    hl7_interfaces[uuid].stop_listening()
                    hl7_interfaces.pop(uuid, None)
                except:
                    pass
                    
            if stream.get('is_test_stream') and stream['message_type'] == 'HL7v2':
                try:
                    client = TestHL7Client()
                    client.stop_stream(stream['organization_uuid'])
                except:
                    pass
            
            raise
            
    except Exception as e:
        current_app.logger.error(f"Error starting stream {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error starting stream: {str(e)}'
        }), 500

@bp.route('/<uuid>/stop', methods=['POST'])
@login_required
@admin_required
def stop_stream(uuid):
    """Stop a stream, handling both regular and test streams"""
    try:
        stream = mongo.db.streams.find_one({
            "uuid": uuid, 
            "deleted": {"$ne": True}, 
            "organization_uuid": g.organization['uuid']
        })
        
        if not stream:
            return jsonify({'status': 'error', 'message': 'Stream not found or has been deleted'}), 404

        if not stream['active']:
            return jsonify({'status': 'error', 'message': 'Stream is already inactive'}), 400

        # Set stream to 'stopping' state
        mongo.db.streams.update_one({"uuid": uuid}, {"$set": {"active": "stopping"}})

        error_message = None
        try:
            if stream.get('is_test_stream') and stream['message_type'] == 'HL7v2':
                # First stop the local interface
                if uuid in hl7_interfaces:
                    try:
                        hl7_interfaces[uuid].stop_listening()
                        hl7_interfaces.pop(uuid, None)
                    except Exception as e:
                        error_message = f"Warning: Failed to stop local interface: {str(e)}"
                        current_app.logger.warning(error_message)
                
                # Then stop the test service stream
                client = TestHL7Client()
                result = client.stop_stream(stream['organization_uuid'])
                
                if not result or result.get('status') != 'success':
                    error_message = "Warning: Failed to stop test service stream"
                    current_app.logger.warning(error_message)
                    
            elif stream['message_type'] == 'HL7v2' and uuid in hl7_interfaces:
                # Handle regular HL7v2 stream
                hl7_interfaces[uuid].stop_listening()
                hl7_interfaces.pop(uuid, None)
            
            # Update stream status regardless of warnings
            mongo.db.streams.update_one(
                {"uuid": uuid}, 
                {
                    "$set": {
                        "active": False,
                        "last_active": datetime.utcnow()
                    }
                }
            )
            
            if error_message:
                return jsonify({
                    'status': 'warning',
                    'message': f'Stream stopped with warnings: {error_message}'
                }), 200
            
            return jsonify({
                'status': 'success',
                'message': 'Stream stopped successfully'
            }), 200
            
        except Exception as e:
            current_app.logger.error(f"Error stopping stream {uuid}: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': f'Error stopping stream: {str(e)}'
            }), 500
            
    except Exception as e:
        current_app.logger.error(f"Error accessing stream {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error accessing stream: {str(e)}'
        }), 500

@bp.route('/<uuid>', methods=['GET'])
@login_required
def stream_detail(uuid):
    """Get stream details, handling both regular and test streams"""
    try:
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        stream = mongo.db.streams.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid']
        })

        if not stream:
            flash('Stream not found or you do not have access to this stream', 'error')
            return redirect(url_for('streams.view_streams'))

        user_role = g.user_role
        form = DeleteForm()
        
        # Get additional status for test streams
        if stream.get('is_test_stream') and stream['message_type'] == 'HL7v2':
            try:
                client = TestHL7Client()
                test_status = client.get_stream_status(stream['organization_uuid'])
                if test_status and test_status.get('status') == 'success':
                    stream['test_status'] = test_status.get('data', {})
            except Exception as e:
                current_app.logger.error(f"Error fetching test stream status: {str(e)}")
                stream['test_status'] = {'error': 'Unable to fetch test stream status'}

        return render_template('stream_detail.html', 
                            stream=stream, 
                            user_role=user_role, 
                            form=form)

    except Exception as e:
        current_app.logger.error(f"Error in stream_detail: {str(e)}")
        flash('An error occurred while retrieving the stream details', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/<uuid>/delete', methods=['POST'])
@login_required
@admin_required
def delete_stream(uuid):
    """Delete a stream, handling both regular and test streams"""
    try:
        stream = mongo.db.streams.find_one({
            "uuid": uuid, 
            "deleted": {"$ne": True}, 
            "organization_uuid": g.organization['uuid']
        })
        
        if not stream:
            return jsonify({'status': 'error', 'message': 'Stream not found or already deleted'}), 404

        # Stop and clean up based on stream type
        if stream.get('is_test_stream') and stream['message_type'] == 'HL7v2':
            # Handle test HL7v2 stream
            try:
                client = TestHL7Client()
                result = client.delete_stream(stream['organization_uuid'])
                
                if not result or result.get('status') != 'success':
                    raise Exception('Failed to delete test stream')
            except Exception as e:
                current_app.logger.error(f"Error deleting test stream {uuid}: {str(e)}")
                return jsonify({
                    'status': 'error',
                    'message': 'Failed to delete test stream'
                }), 500
                
        elif stream['message_type'] == 'HL7v2' and uuid in hl7_interfaces:
            # Handle regular HL7v2 stream
            hl7_interfaces[uuid].stop_listening()
            hl7_interfaces.pop(uuid, None)

        # Mark stream as deleted in database
        mongo.db.streams.update_one(
            {"uuid": uuid},
            {
                "$set": {
                    "deleted": True,
                    "deleted_at": datetime.utcnow(),
                    "active": False
                }
            }
        )

        return jsonify({
            'status': 'success',
            'message': 'Stream deleted successfully'
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error deleting stream {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error deleting stream: {str(e)}'
        }), 500

@bp.route('/<uuid>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_stream(uuid):
    try:
        stream = mongo.db.streams.find_one({"uuid": uuid, "deleted": {"$ne": True}, "organization_uuid": g.organization['uuid']})
        if not stream:
            flash('Stream not found or has been deleted', 'error')
            return redirect(url_for('streams.view_streams'))

        if stream['message_type'] == 'FHIR':
            flash('Test Servers streams cannot be edited', 'error')
            return redirect(url_for('streams.stream_detail', uuid=uuid))

        if stream['message_type'] == 'HL7v2':
            form = HL7v2StreamForm(obj=stream)
        elif stream['message_type'] in ['CCDA', 'X12', 'Clinical Notes']:
            form = SFTPStreamForm(obj=stream)
        else:
            flash('Unsupported stream type for editing', 'error')
            return redirect(url_for('streams.stream_detail', uuid=uuid))

        if request.method == 'POST' and form.validate_on_submit():
            update_data = {
                "name": form.name.data,
                "updated_at": datetime.utcnow()
            }
            
            if stream['message_type'] == 'HL7v2':
                update_data.update({
                    "host": form.host.data,
                    "port": form.port.data,
                    "timeout": form.timeout.data
                })
            elif stream['message_type'] in ['CCDA', 'X12', 'Clinical Notes']:
                update_data.update({
                    "host": form.host.data,
                    "port": form.port.data,
                    "username": form.username.data,
                    "auth_method": form.auth_method.data,
                    "remote_path": form.remote_path.data,
                    "file_pattern": form.file_pattern.data,
                    "fetch_interval": form.fetch_interval.data
                })
                if form.auth_method.data == 'password':
                    update_data["password"] = form.password.data
                    update_data["private_key"] = None
                else:
                    update_data["private_key"] = form.private_key.data
                    update_data["password"] = None

            mongo.db.streams.update_one({"uuid": uuid}, {"$set": update_data})
            flash('Stream updated successfully', 'success')
            return redirect(url_for('streams.stream_detail', uuid=uuid))

        return render_template('edit_stream.html', stream=stream, form=form)
    except PyMongoError as e:
        current_app.logger.error(f"Database error editing stream {uuid}: {str(e)}", exc_info=True)
        flash('A database error occurred while editing the stream.', 'error')
        return render_template('500.html', error_message="A database error occurred while editing the stream"), 500
    except Exception as e:
        current_app.logger.error(f"Error editing stream {uuid}: {str(e)}", exc_info=True)
        flash(f'Error editing stream: {str(e)}', 'error')
        return render_template('500.html', error_message="An unexpected error occurred while editing the stream"), 500

@bp.route('/fhir-stream-status', methods=['GET'])
@login_required
def get_fhir_stream_status():
    try:
        existing_streams = mongo.db.streams.find({
            "message_type": "FHIR",
            "deleted": {"$ne": True},
            "organization_uuid": g.organization['uuid']
        }, {"name": 1})
        
        active_servers = [stream['name'].split()[0].lower() for stream in existing_streams]
        return jsonify(active_servers), 200
    except PyMongoError as e:
        current_app.logger.error(f"Database error fetching FHIR stream status: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'A database error occurred while fetching FHIR stream status'}), 500
    except Exception as e:
        current_app.logger.error(f"Error fetching FHIR stream status: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error fetching FHIR stream status: {str(e)}'}), 500

@bp.route('/get_dashboard_metrics', methods=['GET'])
@login_required
def get_dashboard_metrics():
    try:
        total_messages = mongo.db.messages.count_documents({"organization_uuid": g.organization['uuid']})
        converted_messages = mongo.db.fhir_messages.count_documents({"organization_uuid": g.organization['uuid']})
        
        conversion_times = list(mongo.db.fhir_messages.aggregate([
            {"$match": {"organization_uuid": g.organization['uuid']}},
            {"$group": {"_id": None, "avg_time": {"$avg": "$conversion_metadata.conversion_time"}}}
        ]))
        
        avg_conversion_time = conversion_times[0]['avg_time'] if conversion_times else 0
        
        return jsonify({
            "total_messages": total_messages,
            "converted_messages": converted_messages,
            "avg_conversion_time": avg_conversion_time
        })
    except PyMongoError as e:
        current_app.logger.error(f"Database error fetching dashboard metrics: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'A database error occurred while fetching dashboard metrics'}), 500
    except Exception as e:
        current_app.logger.error(f"Error fetching dashboard metrics: {str(e)}", exc_info=True)
        return jsonify({'status': 'error', 'message': f'Error fetching dashboard metrics: {str(e)}'}), 500

def get_pasted_messages_stream(organization_uuid):
    try:
        pasted_stream = mongo.db.streams.find_one({
            "name": "Pasted Messages",
            "is_default": True,
            "organization_uuid": organization_uuid
        })
        if not pasted_stream:
            pasted_stream = {
                "uuid": str(uuid.uuid4()),
                "organization_uuid": organization_uuid,
                "name": "Pasted Messages",
                "message_type": "Mixed",
                "active": True,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "deleted": False,
                "is_default": True
            }
            mongo.db.streams.insert_one(pasted_stream)
        return pasted_stream
    except PyMongoError as e:
        current_app.logger.error(f"Database error getting pasted messages stream: {str(e)}", exc_info=True)
        return None
    except Exception as e:
        current_app.logger.error(f"Unexpected error getting pasted messages stream: {str(e)}", exc_info=True)
        return None

# Error handlers
@bp.app_errorhandler(404)
def not_found_error(error):
    current_app.logger.error(f'404 error occurred: {str(error)}')
    return render_template('404.html'), 404

@bp.app_errorhandler(500)
def internal_error(error):
    current_app.logger.error(f'500 error occurred: {str(error)}')
    return render_template('500.html'), 500

