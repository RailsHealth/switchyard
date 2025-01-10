# streamsv2.py - Part 1: Imports, Setup, and Helper Functions

from flask import Blueprint, render_template, redirect, url_for, jsonify, request, flash, g, session, current_app
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, HiddenField, validators
from wtforms.validators import DataRequired, Length, Regexp, ValidationError
from app.extensions import mongo
from app.auth.decorators import login_required, admin_required
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.models.stream_status import StreamStatus, StreamStatusManager
from app.streamsv2.core.cloud_stream import CloudStream
from app.streamsv2.models.message_format import CloudStorageMessageFormat
from datetime import datetime
import uuid
import logging
import asyncio
from functools import wraps
from typing import List, Dict, Optional, Tuple, Any

logger = logging.getLogger(__name__)
bp = Blueprint('streams', __name__, url_prefix='/streams')

# Custom Exceptions
class AsyncOperationError(Exception):
    """Exception for async operation failures"""
    pass

class StreamConfigurationError(Exception):
    """Exception for stream configuration issues"""
    pass

# Decorators
def async_route(f):
    """Decorator to handle async route handlers"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper

def handle_async_errors(f):
    """Decorator to handle async operation errors"""
    @wraps(f)
    async def wrapper(*args, **kwargs):
        try:
            return await f(*args, **kwargs)
        except asyncio.TimeoutError:
            logger.error("Async operation timed out")
            raise AsyncOperationError("Operation timed out")
        except asyncio.CancelledError:
            logger.error("Async operation cancelled")
            raise AsyncOperationError("Operation cancelled")
        except Exception as e:
            logger.error(f"Async operation failed: {str(e)}")
            raise
    return wrapper

# Forms
class StreamFilterForm(FlaskForm):
    """Form for stream listing filters with dynamic message type choices"""
    endpoint_type = SelectField('Endpoint Type', choices=[
        ('all', 'All'),
        ('mllp', 'MLLP'),
        ('sftp', 'SFTP'),
        ('aws_s3', 'AWS S3'),
        ('gcp_storage', 'Google Cloud Storage')
    ])
    message_type = SelectField('Message Type')
    
    def __init__(self, *args, mode=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.setup_choices(mode)
    
    def setup_choices(self, mode):
        """Setup form choices based on mode"""
        base_choices = [('all', 'All')]
        
        if mode == 'source':
            msg_choices = [
                (t, current_app.config['MESSAGE_TYPE_DISPLAY'].get(t, t))
                for t in current_app.config['STREAMSV2_MESSAGE_TYPES']['source'].keys()
            ]
            self.endpoint_type.choices = [
                ('all', 'All'),
                ('mllp', 'MLLP'),
                ('sftp', 'SFTP')
            ]
        elif mode == 'destination':
            msg_choices = [
                (t, current_app.config['MESSAGE_TYPE_DISPLAY'].get(t, t))
                for t in current_app.config['STREAMSV2_MESSAGE_TYPES']['destination'].keys()
            ]
            self.endpoint_type.choices = [
                ('all', 'All'),
                ('aws_s3', 'AWS S3'),
                ('gcp_storage', 'Google Cloud Storage')
            ]
        else:
            source_types = [
                (t, current_app.config['MESSAGE_TYPE_DISPLAY'].get(t, t))
                for t in current_app.config['STREAMSV2_MESSAGE_TYPES']['source'].keys()
            ]
            dest_types = [
                (t, current_app.config['MESSAGE_TYPE_DISPLAY'].get(t, t))
                for t in current_app.config['STREAMSV2_MESSAGE_TYPES']['destination'].keys()
            ]
            msg_choices = sorted(set(source_types + dest_types))
            
        self.message_type.choices = base_choices + msg_choices

class NewStreamForm(FlaskForm):
    """Form for stream configuration with dynamic message type handling"""
    name = StringField('Stream Name', validators=[
        DataRequired(),
        Length(min=3, max=64),
        Regexp(r'^[a-zA-Z0-9-_\s]+$', 
               message="Name can only contain letters, numbers, spaces, hyphens, and underscores")
    ])
    message_type = SelectField('Message Type', validators=[DataRequired()])
    source_endpoint_uuid = HiddenField('Source Endpoint')
    destination_endpoint_uuid = HiddenField('Destination Endpoint')

    def __init__(self, *args, source_type=None, **kwargs):
        super().__init__(*args, **kwargs)
        if source_type:
            config = current_app.config['STREAMSV2_MESSAGE_TYPES']['source'].get(source_type, {})
            if config:
                dest_type = config.get('destination_format')
                self.message_type.choices = [(dest_type, dest_type)]

# Helper Functions
def _get_compatible_endpoints(mode: str, message_type: Optional[str] = None) -> List[Dict]:
    """
    Get compatible endpoints based on mode and message type
    
    Currently limited to single source and destination, but structured for future expansion
    """
    try:
        query = {
            "organization_uuid": g.organization['uuid'],
            "mode": mode,
            "deleted": {"$ne": True},
            "status": {"$ne": StreamStatus.ERROR}
        }
        
        if message_type:
            query["message_type"] = message_type
            
        # Get endpoint configurations
        endpoints = list(mongo.db.endpoints.find(query))
        
        # Add compatibility flags for future use
        for endpoint in endpoints:
            endpoint['supports_multiple_streams'] = False  # Future expansion flag
            endpoint['max_streams'] = 1  # Current limitation
            endpoint['current_streams'] = _count_endpoint_streams(endpoint['uuid'])
            
        return endpoints
        
    except Exception as e:
        logger.error(f"Error getting compatible endpoints: {str(e)}")
        return []

def _count_endpoint_streams(endpoint_uuid: str) -> int:
    """Count active streams using an endpoint"""
    try:
        # Check source endpoints
        source_count = mongo.db.streams_v2.count_documents({
            "source_endpoint_uuid": endpoint_uuid,
            "deleted": {"$ne": True},
            "status": {"$ne": StreamStatus.ERROR}
        })
        
        # Check destination endpoints
        dest_count = mongo.db.streams_v2.count_documents({
            "destination_endpoint_uuid": endpoint_uuid,
            "deleted": {"$ne": True},
            "status": {"$ne": StreamStatus.ERROR}
        })
        
        return source_count + dest_count
        
    except Exception as e:
        logger.error(f"Error counting endpoint streams: {str(e)}")
        return 0

def _validate_endpoint_compatibility(source_endpoint: Dict, destination_endpoint: Dict) -> Tuple[bool, str]:
    """
    Validate endpoint compatibility
    
    Structured to support future expansion of compatibility rules
    """
    try:
        # Get supported message types and configurations
        message_types = current_app.config['STREAMSV2_MESSAGE_TYPES']
        endpoint_configs = current_app.config['ENDPOINT_TYPE_CONFIGS']
        
        # Basic validation
        source_type = source_endpoint['message_type']
        dest_type = f"{source_type} in JSON"
        
        if source_type not in message_types['source']:
            return False, f"Unsupported source message type: {source_type}"
            
        if dest_type not in message_types['destination']:
            return False, f"Unsupported destination message type: {dest_type}"
            
        # Check stream limits (current limitation: 1 per endpoint)
        if _count_endpoint_streams(source_endpoint['uuid']) >= 1:
            return False, "Source endpoint already in use"
            
        if _count_endpoint_streams(destination_endpoint['uuid']) >= 1:
            return False, "Destination endpoint already in use"
            
        return True, dest_type
        
    except Exception as e:
        logger.error(f"Error validating endpoint compatibility: {str(e)}")
        return False, str(e)
    

@bp.route('/view')
@login_required
def view_streams():
    """List all streams for current organization"""
    try:
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        form = StreamFilterForm(request.args)
        query = {"organization_uuid": g.organization['uuid']}
        
        # Add filters
        if form.endpoint_type.data and form.endpoint_type.data != 'all':
            query["$or"] = [
                {"source_endpoint_type": form.endpoint_type.data},
                {"destination_endpoint_type": form.endpoint_type.data}
            ]
            
        if form.message_type.data and form.message_type.data != 'all':
            query["message_type"] = form.message_type.data

        # Get streams
        streams = list(mongo.db.streams_v2.find(query))
        
        # Get endpoints for each stream
        for stream in streams:
            stream['source_endpoint'] = mongo.db.endpoints.find_one({
                "uuid": stream.get('source_endpoint_uuid')
            })
            stream['destination_endpoint'] = mongo.db.endpoints.find_one({
                "uuid": stream.get('destination_endpoint_uuid')
            })

        # Get endpoint counts for new stream button
        source_endpoints = _get_compatible_endpoints(mode='source')
        destination_endpoints = _get_compatible_endpoints(mode='destination')

        return render_template(
            'streamsv2/view_streams.html',
            streams=streams,
            form=form,
            source_count=len(source_endpoints),
            destination_count=len(destination_endpoints),
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error in view_streams: {str(e)}", exc_info=True)
        flash('Error retrieving streams', 'error')
        return render_template('500.html'), 500
    
# Routes with enhanced logging and error handling

@bp.route('/new')
@login_required
@admin_required
def new_stream():
    """Initialize new stream creation"""
    try:
        logger.info("Starting new stream creation workflow")
        
        if not g.organization:
            logger.warning("No organization selected for new stream creation")
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        # Clear any existing session data
        if 'stream_creation' in session:
            logger.debug("Clearing existing stream creation session data")
            session.pop('stream_creation', None)
        
        # Check if organization has required endpoints
        source_endpoints = _get_compatible_endpoints(mode='source')
        destination_endpoints = _get_compatible_endpoints(mode='destination')
        
        logger.info(f"Found {len(source_endpoints)} source endpoints and {len(destination_endpoints)} destination endpoints")
        
        if not destination_endpoints:
            logger.warning("No destination endpoints available")
            flash('Please create a destination endpoint first', 'warning')
            return redirect(url_for('endpoints.create_endpoint', mode='destination'))
            
        if not source_endpoints:
            logger.warning("No source endpoints available")
            flash('Please create a source endpoint first', 'warning')
            return redirect(url_for('endpoints.create_endpoint', mode='source'))
        
        logger.info("Prerequisites met, redirecting to destination selection")
        return redirect(url_for('streams.select_destination'))
        
    except Exception as e:
        logger.error(f"Error in new_stream: {str(e)}", exc_info=True)
        flash('Error starting stream creation', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/new/select-destination', methods=['GET', 'POST'])
@login_required
@admin_required
def select_destination():
    """Select destination endpoint for new stream"""
    try:
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        # Clear any existing session data at the start of the flow
        if 'stream_creation' in session:
            session.pop('stream_creation', None)

        # Create filter form for destination selection
        filter_form = StreamFilterForm(mode='destination')
        
        if request.method == 'POST':
            destination_uuid = request.form.get('destination_endpoint')
            if not destination_uuid:
                flash('Please select a destination endpoint', 'error')
                return redirect(url_for('streams.select_destination'))

            # Validate endpoint
            destination_endpoint = mongo.db.endpoints.find_one({
                "uuid": destination_uuid,
                "organization_uuid": g.organization['uuid'],
                "mode": "destination",
                "deleted": {"$ne": True}
            })

            if not destination_endpoint:
                flash('Selected endpoint is not available', 'error')
                return redirect(url_for('streams.select_destination'))

            # Store in session
            session['stream_creation'] = {
                'destination_endpoint_uuid': destination_uuid,
                'destination_message_type': destination_endpoint['message_type']
            }

            return redirect(url_for('streams.select_source'))

        # GET request - show destination selection
        endpoints = _get_compatible_endpoints(mode='destination')
        available_endpoints = [
            endpoint for endpoint in endpoints 
            if endpoint.get('current_streams', 0) < endpoint.get('max_streams', 1)
        ]

        return render_template(
            'streamsv2/new_stream.html',
            step='destination',
            endpoints=available_endpoints,
            form=filter_form,
            destination_endpoint=None,
            source_endpoint=None,
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error in select_destination: {str(e)}", exc_info=True)
        flash('Error selecting destination endpoint', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/new/select-source', methods=['GET', 'POST'])
@login_required
@admin_required
def select_source():
    """Select source endpoint for new stream"""
    try:
        # Verify previous step completion
        stream_data = session.get('stream_creation')
        if not stream_data or 'destination_endpoint_uuid' not in stream_data:
            flash('Please select a destination endpoint first', 'warning')
            return redirect(url_for('streams.select_destination'))

        # Get destination endpoint details
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream_data['destination_endpoint_uuid'],
            "organization_uuid": g.organization['uuid']
        })
        
        if not destination_endpoint:
            flash('Selected destination endpoint not found', 'error')
            return redirect(url_for('streams.select_destination'))

        # Create filter form
        filter_form = StreamFilterForm(mode='source')

        if request.method == 'POST':
            source_uuid = request.form.get('source_endpoint')
            if not source_uuid:
                flash('Please select a source endpoint', 'error')
                return redirect(url_for('streams.select_source'))

            # Validate endpoint
            source_endpoint = mongo.db.endpoints.find_one({
                "uuid": source_uuid,
                "organization_uuid": g.organization['uuid'],
                "mode": "source",
                "deleted": {"$ne": True}
            })

            if not source_endpoint:
                flash('Selected source endpoint not found or not available', 'error')
                return redirect(url_for('streams.select_source'))

            # Validate compatibility
            source_type = source_endpoint['message_type']
            if source_type not in current_app.config['STREAMSV2_MESSAGE_TYPES']['source']:
                flash('Invalid message type configuration', 'error')
                return redirect(url_for('streams.select_source'))

            # Get destination type mapping
            message_type_config = current_app.config['STREAMSV2_MESSAGE_TYPES']['source'][source_type]
            destination_type = message_type_config['destination_format']

            # Validate compatibility
            is_compatible, message = _validate_endpoint_compatibility(
                source_endpoint,
                destination_endpoint
            )
            
            if not is_compatible:
                flash(f'Incompatible endpoints: {message}', 'error')
                return redirect(url_for('streams.select_source'))

            # Update session data
            stream_data.update({
                'source_endpoint_uuid': source_uuid,
                'source_message_type': source_type,
                'destination_message_type': destination_type
            })
            session['stream_creation'] = stream_data

            return redirect(url_for('streams.configure_stream'))

        # GET request
        endpoints = _get_compatible_endpoints(mode='source')
        available_endpoints = [
            endpoint for endpoint in endpoints 
            if endpoint.get('current_streams', 0) < endpoint.get('max_streams', 1)
        ]

        return render_template(
            'streamsv2/new_stream.html',
            step='source',
            endpoints=available_endpoints,
            form=filter_form,
            destination_endpoint=destination_endpoint,
            source_endpoint=None,
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error in select_source: {str(e)}", exc_info=True)
        flash('Error selecting source endpoint', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/new/configure', methods=['GET', 'POST'])
@login_required
@admin_required
def configure_stream():
    """Configure and create new stream"""
    try:
        # Verify session data from previous steps
        stream_data = session.get('stream_creation')
        if not stream_data or 'source_endpoint_uuid' not in stream_data:
            logger.warning("Stream creation session data missing or incomplete")
            flash('Please complete endpoint selection first', 'warning')
            return redirect(url_for('streams.select_destination'))

        # Get endpoints
        source_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream_data['source_endpoint_uuid'],
            "organization_uuid": g.organization['uuid']
        })
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream_data['destination_endpoint_uuid'],
            "organization_uuid": g.organization['uuid']
        })

        if not source_endpoint or not destination_endpoint:
            logger.error("Required endpoints not found")
            flash('Error loading endpoint details', 'error')
            return redirect(url_for('streams.select_destination'))

        # Create form with correct message type
        form = NewStreamForm(source_type=source_endpoint['message_type'])
        form.message_type.choices = [(stream_data['destination_message_type'], 
                                    stream_data['destination_message_type'])]

        if form.validate_on_submit():
            logger.info("Processing stream creation form submission")
            
            # Final validation of endpoint compatibility
            is_compatible, compatibility_error = _validate_endpoint_compatibility(
                source_endpoint,
                destination_endpoint
            )
            if not is_compatible:
                logger.error(f"Endpoint compatibility check failed: {compatibility_error}")
                flash('Endpoint compatibility check failed', 'error')
                return redirect(url_for('streams.select_source'))

            # Create stream configuration
            stream_uuid = str(uuid.uuid4())
            stream_config = {
                "uuid": stream_uuid,
                "name": form.name.data,
                "organization_uuid": g.organization['uuid'],
                "source_endpoint_uuid": source_endpoint['uuid'],
                "destination_endpoint_uuid": destination_endpoint['uuid'],
                "message_type": form.message_type.data,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "status": StreamStatus.INACTIVE,
                "active": False,
                "metadata": {
                    "created_by": g.user['email'],
                    "source_type": source_endpoint['endpoint_type'],
                    "destination_type": destination_endpoint['endpoint_type'],
                    "source_message_type": stream_data['source_message_type'],
                    "destination_message_type": stream_data['destination_message_type']
                },
                # Initialize metrics
                "metrics": {
                    "total_messages_processed": 0,
                    "total_messages_failed": 0,
                    "total_bytes_transferred": 0,
                    "processing_time_avg": 0.0,
                    "error_rate": 0.0,
                    "last_processed_at": None,
                    "last_error": None,
                    "processing_history": [],
                    "hourly_metrics": {},
                    "daily_metrics": {}
                }
            }

            try:
                # Create stream in database
                result = mongo.db.streams_v2.insert_one(stream_config)
                if not result.acknowledged:
                    raise ValueError("Failed to create stream in database")

                logger.info(f"Successfully created stream: {stream_uuid}")
                
                # Clear session data
                session.pop('stream_creation', None)
                
                flash('Stream created successfully', 'success')
                return redirect(url_for('streams.view_streams'))

            except Exception as e:
                logger.error(f"Error creating stream: {str(e)}")
                flash(f'Error creating stream: {str(e)}', 'error')
                return redirect(url_for('streams.configure_stream'))

        # GET request - render configuration form
        try:
            sample_data = CloudStorageMessageFormat.get_sample_data(stream_data['destination_message_type'])
        except Exception as e:
            logger.warning(f"Failed to generate sample data: {str(e)}")
            sample_data = None

        return render_template(
            'streamsv2/new_stream.html',
            step='configure',
            form=form,
            source_endpoint=source_endpoint,
            destination_endpoint=destination_endpoint,
            sample_data=sample_data,
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error in configure_stream: {str(e)}", exc_info=True)
        flash('Error configuring stream', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/<uuid>/detail')
@login_required
def stream_detail(uuid):
    """Show stream details"""
    try:
        # Get stream
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid']
        })

        if not stream:
            flash('Stream not found', 'error')
            return redirect(url_for('streams.view_streams'))

        # Get and attach endpoint details
        source_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('source_endpoint_uuid')
        })
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('destination_endpoint_uuid')
        })
        
        # Add endpoints to stream object like view_streams does
        stream['source_endpoint'] = source_endpoint
        stream['destination_endpoint'] = destination_endpoint

        # Get stream logs
        stream_logs = list(mongo.db.message_cycle_logs.find({
            "organization_uuid": g.organization['uuid'],
            "details.stream_uuid": uuid
        }).sort("timestamp", -1).limit(100))

        # For the source endpoints section in template
        stream['source_endpoints'] = [source_endpoint] if source_endpoint else []

        return render_template(
            'streamsv2/stream_detail.html',
            stream=stream,
            stream_logs=stream_logs,
            active_tab='details'
        )

    except Exception as e:
        logger.error(f"Error getting stream details: {str(e)}", exc_info=True)
        flash('Error retrieving stream details', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/<uuid>/start', methods=['POST'])
@login_required
@admin_required
@async_route
@handle_async_errors
async def start_stream(uuid):
    """Start stream processing with endpoint validation"""
    try:
        if not request.is_json:
            return jsonify({
                'status': 'error',
                'message': 'JSON request required'
            }), 400

        # Get stream details
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })

        if not stream:
            return jsonify({
                'status': 'error',
                'message': 'Stream not found'
            }), 404

        # Get endpoints
        source_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('source_endpoint_uuid'),
            "deleted": {"$ne": True}
        })
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('destination_endpoint_uuid'),
            "deleted": {"$ne": True}
        })

        if not source_endpoint or not destination_endpoint:
            return jsonify({
                'status': 'error',
                'message': 'One or more endpoints not found'
            }), 404

        # Prepare endpoint status information
        endpoint_status = {
            'source': {
                'name': source_endpoint.get('name'),
                'status': source_endpoint.get('status'),
                'running': source_endpoint.get('status') == 'ACTIVE'
            },
            'destination': {
                'name': destination_endpoint.get('name'),
                'status': destination_endpoint.get('status'),
                'running': destination_endpoint.get('status') == 'ACTIVE'
            }
        }

        # Return endpoint status if any endpoint is not running
        if not endpoint_status['source']['running'] or not endpoint_status['destination']['running']:
            return jsonify({
                'status': 'endpoints_check',
                'endpoints': endpoint_status,
                'message': 'Endpoints need to be started first'
            }), 200

        # Validate current status
        current_status = StreamStatus(stream.get('status', StreamStatus.INACTIVE))
        if current_status == StreamStatus.ACTIVE:
            return jsonify({
                'status': 'error',
                'message': 'Stream is already active'
            }), 400

        # Remove MongoDB _id before creating StreamConfig
        if '_id' in stream:
            stream.pop('_id')

        # Create and start stream instance
        try:
            stream_config = StreamConfig(**stream)
            stream_instance = CloudStream(stream_config)
            
            try:
                success = await stream_instance.start()
                if not success:
                    logger.error(f"Failed to start stream {uuid}")
                    raise AsyncOperationError("Failed to start stream")
            except ValueError as ve:
                # Handle validation errors
                logger.warning(f"Validation error starting stream {uuid}: {str(ve)}")
                return jsonify({
                    'status': 'endpoints_check',
                    'endpoints': endpoint_status,
                    'message': str(ve)
                }), 200

            # Update stream status
            result = mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.ACTIVE,
                        "active": True,
                        "last_active": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                }
            )

            if not result.modified_count:
                raise AsyncOperationError("Failed to update stream status")

            # Return success with endpoint status
            logger.info(f"Successfully started stream {uuid}")
            return jsonify({
                'status': 'success',
                'message': 'Stream started successfully',
                'endpoints': endpoint_status
            })

        except AsyncOperationError as e:
            # Reset status on error
            logger.error(f"AsyncOperationError in stream {uuid}: {str(e)}")
            mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.ERROR,
                        "active": False,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            raise

    except Exception as e:
        logger.error(f"Error starting stream {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@bp.route('/<uuid>/stop', methods=['POST'])
@login_required
@admin_required
@async_route
@handle_async_errors
async def stop_stream(uuid):
    """Stop stream processing with status validation"""
    try:
        if not request.is_json:
            return jsonify({
                'status': 'error',
                'message': 'JSON request required'
            }), 400

        # Get stream details
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })

        if not stream:
            return jsonify({
                'status': 'error',
                'message': 'Stream not found'
            }), 404

        # Get endpoints for status tracking
        source_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('source_endpoint_uuid')
        })
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('destination_endpoint_uuid')
        })

        # Prepare endpoint status (for UI feedback)
        endpoint_status = {
            'source': {
                'name': source_endpoint.get('name') if source_endpoint else 'Unknown',
                'status': source_endpoint.get('status') if source_endpoint else 'UNKNOWN'
            },
            'destination': {
                'name': destination_endpoint.get('name') if destination_endpoint else 'Unknown',
                'status': destination_endpoint.get('status') if destination_endpoint else 'UNKNOWN'
            }
        }

        # Validate current status
        current_status = StreamStatus(stream.get('status', StreamStatus.INACTIVE))
        if current_status != StreamStatus.ACTIVE:
            return jsonify({
                'status': 'error',
                'message': 'Stream is not active'
            }), 400

        # Remove MongoDB _id
        if '_id' in stream:
            stream.pop('_id')

        # Stop stream instance
        try:
            stream_config = StreamConfig(**stream)
            stream_instance = CloudStream(stream_config)
            
            success = await stream_instance.stop()
            if not success:
                logger.error(f"Failed to stop stream {uuid}")
                raise AsyncOperationError("Failed to stop stream")

            # Update stream status
            result = mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.INACTIVE,
                        "active": False,
                        "updated_at": datetime.utcnow()
                    }
                }
            )

            if not result.modified_count:
                raise AsyncOperationError("Failed to update stream status")

            logger.info(f"Successfully stopped stream {uuid}")
            return jsonify({
                'status': 'success',
                'message': 'Stream stopped successfully',
                'endpoints': endpoint_status
            })

        except AsyncOperationError as e:
            # Set error status
            logger.error(f"AsyncOperationError in stream {uuid}: {str(e)}")
            mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.ERROR,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            raise

    except Exception as e:
        logger.error(f"Error stopping stream {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@bp.route('/<uuid>/delete', methods=['POST'])
@login_required
@admin_required
@async_route
@handle_async_errors
async def delete_stream(uuid):
    """Delete stream"""
    try:
        if not request.is_json:
            return jsonify({
                'status': 'error',
                'message': 'JSON request required'
            }), 400

        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })

        if not stream:
            return jsonify({
                'status': 'error',
                'message': 'Stream not found'
            }), 404

        # Stop stream if active
        if stream.get('status') == StreamStatus.ACTIVE:
            stream_config = StreamConfig(**stream)
            stream_instance = CloudStream(stream_config)
            await stream_instance.stop()

        # Mark as deleted
        result = mongo.db.streams_v2.update_one(
            {"uuid": uuid},
            {
                "$set": {
                    "deleted": True,
                    "deleted_at": datetime.utcnow(),
                    "deleted_by": g.user['email'],
                    "status": StreamStatus.DELETED,
                    "active": False
                }
            }
        )

        if result.modified_count:
            return jsonify({
                'status': 'success',
                'message': 'Stream deleted successfully'
            })
        
        return jsonify({
            'status': 'error',
            'message': 'Failed to delete stream'
        }), 500

    except Exception as e:
        logger.error(f"Error deleting stream {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# Error Handlers
@bp.app_errorhandler(AsyncOperationError)
def handle_async_error(error):
    return jsonify({
        'status': 'error',
        'message': str(error)
    }), 500

@bp.app_errorhandler(StreamConfigurationError)
def handle_config_error(error):
    return jsonify({
        'status': 'error',
        'message': str(error)
    }), 400

# Context Processors
@bp.context_processor
def utility_processor():
    """Add utility functions to template context"""
    def format_stream_status(status: str) -> dict:
        """Format status for display"""
        return StreamStatusManager.get_status_display(StreamStatus(status))

    return dict(format_stream_status=format_stream_status)