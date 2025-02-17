"""
StreamsV2 Blueprint
Routes for stream management and operations.
"""

from flask import Blueprint, render_template, redirect, url_for, jsonify, request, flash, g, session, current_app
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, HiddenField
from wtforms.validators import DataRequired, Length, Regexp, ValidationError
from app.extensions import mongo
from app.auth.decorators import login_required, admin_required
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.models.stream_status import StreamStatus
from app.streamsv2.core.stream_processor import StreamProcessor
from app.utils.logging_utils import log_message_cycle
from datetime import datetime, timedelta
from uuid import uuid4
import logging
from typing import List, Dict, Optional, Tuple, Any

# Initialize Blueprint and logger
bp = Blueprint('streams', __name__, url_prefix='/streams')
logger = logging.getLogger(__name__)

# Form Classes
class StreamFilterForm(FlaskForm):
    """Form for stream listing filters"""
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
        base_choices = [('all', 'All')]
        if mode == 'source':
            self.message_type.choices = base_choices + [
                (t, current_app.config['MESSAGE_TYPE_DISPLAY'].get(t, t))
                for t in current_app.config['STREAMSV2_MESSAGE_TYPES']['source'].keys()
            ]
        elif mode == 'destination':
            self.message_type.choices = base_choices + [
                (t, current_app.config['MESSAGE_TYPE_DISPLAY'].get(t, t))
                for t in current_app.config['STREAMSV2_MESSAGE_TYPES']['destination'].keys()
            ]
        else:
            self.message_type.choices = base_choices + [
                (t, current_app.config['MESSAGE_TYPE_DISPLAY'].get(t, t))
                for t in list(current_app.config['STREAMSV2_MESSAGE_TYPES']['source'].keys()) +
                         list(current_app.config['STREAMSV2_MESSAGE_TYPES']['destination'].keys())
            ]

# Helper Functions
def _get_compatible_endpoints(mode: str, message_type: Optional[str] = None) -> List[Dict]:
    """Get compatible endpoints based on mode and message type"""
    try:
        query = {
            "organization_uuid": g.organization['uuid'],
            "mode": mode,
            "deleted": {"$ne": True}
        }
        
        if message_type:
            query["message_type"] = message_type
            
        endpoints = list(mongo.db.endpoints.find(query))
        
        # Add stream usage info
        for endpoint in endpoints:
            endpoint['supports_multiple_streams'] = False
            endpoint['max_streams'] = 1
            endpoint['current_streams'] = _count_endpoint_streams(endpoint['uuid'])
            
        return endpoints
        
    except Exception as e:
        logger.error(f"Error getting compatible endpoints: {str(e)}")
        return []

def _count_endpoint_streams(endpoint_uuid: str) -> int:
    """Count active streams using an endpoint"""
    try:
        return mongo.db.streams_v2.count_documents({
            "$or": [
                {"source_endpoint_uuid": endpoint_uuid},
                {"destination_endpoint_uuid": endpoint_uuid}
            ],
            "deleted": {"$ne": True},
            "status": {"$nin": [StreamStatus.ERROR, StreamStatus.DELETED]}
        })
    except Exception as e:
        logger.error(f"Error counting endpoint streams: {str(e)}")
        return 0

# Main View Routes
@bp.route('/view')
@login_required
def view_streams():
    """List all streams with filtering and pagination"""
    try:
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        # Setup pagination
        page = int(request.args.get('page', 1))
        per_page = current_app.config.get('STREAMS_PER_PAGE', 10)

        # Initialize filter form
        form = StreamFilterForm(request.args)
        
        # Build query
        query = {
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        }
        
        if form.endpoint_type.data and form.endpoint_type.data != 'all':
            query["$or"] = [
                {"metadata.source_type": form.endpoint_type.data},
                {"metadata.destination_type": form.endpoint_type.data}
            ]
            
        if form.message_type.data and form.message_type.data != 'all':
            query["message_type"] = form.message_type.data

        # Get paginated streams
        total_streams = mongo.db.streams_v2.count_documents(query)
        total_pages = (total_streams + per_page - 1) // per_page

        streams = list(mongo.db.streams_v2.find(query)
                      .sort([("status", -1), ("updated_at", -1)])
                      .skip((page - 1) * per_page)
                      .limit(per_page))

        # Add endpoint details
        for stream in streams:
            stream['source_endpoint'] = mongo.db.endpoints.find_one({
                "uuid": stream.get('source_endpoint_uuid')
            })
            stream['destination_endpoint'] = mongo.db.endpoints.find_one({
                "uuid": stream.get('destination_endpoint_uuid')
            })
            stream['metrics'] = stream.get('metrics', {
                "total_messages_processed": 0,
                "total_messages_failed": 0,
                "total_bytes_transferred": 0,
                "error_rate": 0.0
            })

        # Get endpoint counts for new stream button
        source_endpoints = _get_compatible_endpoints(mode='source')
        destination_endpoints = _get_compatible_endpoints(mode='destination')

        return render_template(
            'streamsv2/view_streams.html',
            streams=streams,
            form=form,
            page=page,
            total_pages=total_pages,
            source_count=len(source_endpoints),
            destination_count=len(destination_endpoints),
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error in view_streams: {str(e)}", exc_info=True)
        flash('Error retrieving streams', 'error')
        return render_template('500.html'), 500
    
# Part 2: Stream Creation Workflow Routes

class NewStreamForm(FlaskForm):
    """Form for final stream configuration"""
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

    def validate_name(self, field):
        """Validate stream name uniqueness"""
        existing = mongo.db.streams_v2.find_one({
            "name": field.data,
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })
        if existing:
            raise ValidationError('A stream with this name already exists')

@bp.route('/new')
@login_required
@admin_required
def new_stream():
    """Initialize new stream creation workflow"""
    try:
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        # Clear existing session data
        if 'stream_creation' in session:
            session.pop('stream_creation', None)
        
        # Check for required endpoints
        source_endpoints = _get_compatible_endpoints(mode='source')
        destination_endpoints = _get_compatible_endpoints(mode='destination')
        
        if not source_endpoints and not destination_endpoints:
            flash('Please create source and destination endpoints first', 'warning')
            return redirect(url_for('endpoints.create_endpoint'))
            
        if not destination_endpoints:
            flash('Please create a destination endpoint first', 'warning')
            return redirect(url_for('endpoints.create_endpoint', mode='destination'))
            
        if not source_endpoints:
            flash('Please create a source endpoint first', 'warning')
            return redirect(url_for('endpoints.create_endpoint', mode='source'))

        return redirect(url_for('streams.select_destination'))
        
    except Exception as e:
        logger.error(f"Error starting stream creation: {str(e)}", exc_info=True)
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

        # Start fresh
        if 'stream_creation' in session:
            session.pop('stream_creation', None)

        # Create filter form
        form = StreamFilterForm(mode='destination')
        
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

        # Get available endpoints
        endpoints = _get_compatible_endpoints(mode='destination')
        available_endpoints = [
            endpoint for endpoint in endpoints 
            if endpoint.get('current_streams', 0) < endpoint.get('max_streams', 1)
        ]

        return render_template(
            'streamsv2/new_stream.html',
            step='destination',
            endpoints=available_endpoints,
            form=form,
            destination_endpoint=None,
            source_endpoint=None,
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error selecting destination: {str(e)}", exc_info=True)
        flash('Error selecting destination endpoint', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/new/select-source', methods=['GET', 'POST'])
@login_required
@admin_required
def select_source():
    """Select source endpoint for new stream"""
    try:
        # Check previous step completion
        stream_data = session.get('stream_creation')
        if not stream_data or 'destination_endpoint_uuid' not in stream_data:
            flash('Please select a destination endpoint first', 'warning')
            return redirect(url_for('streams.select_destination'))

        # Get destination endpoint
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream_data['destination_endpoint_uuid'],
            "organization_uuid": g.organization['uuid']
        })
        
        if not destination_endpoint:
            flash('Selected destination endpoint not found', 'error')
            return redirect(url_for('streams.select_destination'))

        # Handle POST
        if request.method == 'POST':
            source_uuid = request.form.get('source_endpoint')
            if not source_uuid:
                flash('Please select a source endpoint', 'error')
                return redirect(url_for('streams.select_source'))

            # Validate source endpoint
            source_endpoint = mongo.db.endpoints.find_one({
                "uuid": source_uuid,
                "organization_uuid": g.organization['uuid'],
                "mode": "source",
                "deleted": {"$ne": True}
            })

            if not source_endpoint:
                flash('Selected source endpoint not found or not available', 'error')
                return redirect(url_for('streams.select_source'))

            # Validate message type compatibility
            source_type = source_endpoint['message_type']
            if source_type not in current_app.config['STREAMSV2_MESSAGE_TYPES']['source']:
                flash('Invalid message type configuration', 'error')
                return redirect(url_for('streams.select_source'))

            # Get destination format
            message_type_config = current_app.config['STREAMSV2_MESSAGE_TYPES']['source'][source_type]
            destination_type = message_type_config['destination_format']

            # Validate compatibility
            is_compatible, compatibility_message = _validate_endpoint_compatibility(
                source_endpoint,
                destination_endpoint
            )
            
            if not is_compatible:
                flash(f'Incompatible endpoints: {compatibility_message}', 'error')
                return redirect(url_for('streams.select_source'))

            # Update session
            stream_data.update({
                'source_endpoint_uuid': source_uuid,
                'source_message_type': source_type,
                'destination_message_type': destination_type
            })
            session['stream_creation'] = stream_data

            return redirect(url_for('streams.configure_stream'))

        # Get available endpoints
        form = StreamFilterForm(mode='source')
        endpoints = _get_compatible_endpoints(mode='source')
        available_endpoints = [
            endpoint for endpoint in endpoints 
            if endpoint.get('current_streams', 0) < endpoint.get('max_streams', 1)
        ]

        return render_template(
            'streamsv2/new_stream.html',
            step='source',
            endpoints=available_endpoints,
            form=form,
            destination_endpoint=destination_endpoint,
            source_endpoint=None,
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error selecting source: {str(e)}", exc_info=True)
        flash('Error selecting source endpoint', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/new/configure', methods=['GET', 'POST'])
@login_required
@admin_required
def configure_stream():
    """Configure and create new stream"""
    try:
        # Verify workflow completion
        stream_data = session.get('stream_creation')
        if not stream_data or 'source_endpoint_uuid' not in stream_data:
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
            flash('Error loading endpoint details', 'error')
            return redirect(url_for('streams.select_destination'))

        # Initialize form
        form = NewStreamForm(source_type=source_endpoint['message_type'])
        form.message_type.choices = [(stream_data['destination_message_type'], 
                                    stream_data['destination_message_type'])]

        if form.validate_on_submit():
            # Create stream configuration
            stream_uuid = str(uuid4())
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
                }
            }

            # Validate stream configuration
            config = StreamConfig(**stream_config)
            valid, error = config.validate()
            if not valid:
                flash(f'Invalid stream configuration: {error}', 'error')
                return redirect(url_for('streams.configure_stream'))

            # Store in database
            result = mongo.db.streams_v2.insert_one(stream_config)
            if not result.acknowledged:
                flash('Failed to create stream', 'error')
                return redirect(url_for('streams.configure_stream'))

            # Log creation with enhanced details
            log_message_cycle(
                message_uuid=str(uuid4()),
                event_type="stream_created",
                details={
                    "stream_uuid": stream_uuid,
                    "stream_name": form.name.data,
                    "message_type": form.message_type.data,
                    "organization_uuid": g.organization['uuid'],
                    "source_type": source_endpoint['endpoint_type'],
                    "destination_type": destination_endpoint['endpoint_type'],
                    "source_message_type": stream_data['source_message_type'],
                    "destination_message_type": stream_data['destination_message_type']
                },
                status="success"
            )

            # Clear session
            session.pop('stream_creation', None)
            
            logger.info(f"Stream {stream_uuid} created successfully with name {form.name.data}")
            flash('Stream created successfully', 'success')
            return redirect(url_for('streams.stream_detail', uuid=stream_uuid))

        return render_template(
            'streamsv2/new_stream.html',
            step='configure',
            form=form,
            source_endpoint=source_endpoint,
            destination_endpoint=destination_endpoint,
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error configuring stream: {str(e)}", exc_info=True)
        flash('Error configuring stream', 'error')
        return redirect(url_for('streams.view_streams'))

# Part 3: Stream Detail View and Operations

def _validate_endpoint_compatibility(source_endpoint: Dict, destination_endpoint: Dict) -> Tuple[bool, str]:
    """Validate endpoint compatibility"""
    try:
        # Get message types
        source_type = source_endpoint['message_type']
        dest_type = destination_endpoint['message_type']

        # Get source configuration
        source_config = current_app.config['STREAMSV2_MESSAGE_TYPES']['source'].get(source_type)
        if not source_config:
            return False, f"Unsupported source message type: {source_type}"

        # Get destination configuration
        dest_config = current_app.config['STREAMSV2_MESSAGE_TYPES']['destination'].get(dest_type)
        if not dest_config:
            return False, f"Unsupported destination message type: {dest_type}"

        # Check endpoint types
        if source_endpoint['endpoint_type'] not in source_config['allowed_endpoints']:
            return False, f"Source endpoint type {source_endpoint['endpoint_type']} not allowed for {source_type}"

        if destination_endpoint['endpoint_type'] not in dest_config['allowed_endpoints']:
            return False, f"Destination endpoint type {destination_endpoint['endpoint_type']} not allowed for {dest_type}"

        # Check if source type can output to this destination type
        if dest_type != source_config['destination_format']:
            return False, f"Source type {source_type} cannot be transformed to {dest_type}"

        logger.debug(f"Validated compatibility: {source_type} -> {dest_type}")
        return True, dest_type

    except Exception as e:
        logger.error(f"Error validating endpoint compatibility: {str(e)}")
        return False, str(e)

def _get_stream_metrics(stream: Dict) -> Dict[str, Any]:
    """Get formatted stream metrics with enhanced error tracking"""
    try:
        metrics = stream.get('metrics', {})
        
        # Calculate error rate properly
        total_processed = metrics.get('total_messages_processed', 0)
        error_rate = (metrics.get('total_messages_failed', 0) / total_processed 
                     if total_processed > 0 else 0.0)

        # Get recent processing rate
        recent_messages = mongo.db.messages.count_documents({
            "stream_tracker.stream_uuid": stream['uuid'],
            "stream_tracker.updated_at": {
                "$gte": datetime.utcnow() - timedelta(minutes=5)
            }
        })

        return {
            'messages_processed': metrics.get('total_messages_processed', 0),
            'messages_failed': metrics.get('total_messages_failed', 0),
            'bytes_transferred': metrics.get('total_bytes_transferred', 0),
            'error_rate': f"{error_rate:.1%}",
            'last_processed': metrics.get('last_processed_at'),
            'processing_time': metrics.get('processing_time_avg', 0),
            'recent_rate': recent_messages / 5.0,  # messages per minute
            'active_messages': metrics.get('active_messages', 0),
            'queued_messages': metrics.get('queued_messages', 0),
            'last_error': metrics.get('last_error'),
            'last_error_time': metrics.get('last_error_time')
        }
    except Exception as e:
        logger.error(f"Error calculating metrics for stream {stream.get('uuid')}: {str(e)}")
        return {
            'messages_processed': 0,
            'messages_failed': 0,
            'bytes_transferred': 0,
            'error_rate': "0.0%",
            'last_processed': None,
            'processing_time': 0
        }

@bp.route('/<uuid>/detail')
@login_required
def stream_detail(uuid):
    """Show stream details and logs with tabbed interface"""
    try:
        # Get stream with organization check
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid']
        })

        if not stream:
            flash('Stream not found', 'error')
            return redirect(url_for('streams.view_streams'))

        # Get endpoints
        source_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('source_endpoint_uuid')
        })
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('destination_endpoint_uuid')
        })
        
        # Add endpoints to stream object
        stream['source_endpoint'] = source_endpoint
        stream['destination_endpoint'] = destination_endpoint
        stream['source_endpoints'] = [source_endpoint] if source_endpoint else []

        # Get stream metrics
        stream['metrics'] = _get_stream_metrics(stream)

        # Get stream logs with pagination
        page = int(request.args.get('page', 1))
        per_page = 50
        logs_query = {
            "organization_uuid": g.organization['uuid'],
            "$or": [
                {"details.stream_uuid": uuid},
                {"details.stream_id": uuid}
            ]
        }
        
        total_logs = mongo.db.message_cycle_logs.count_documents(logs_query)
        total_log_pages = (total_logs + per_page - 1) // per_page
        
        stream_logs = list(mongo.db.message_cycle_logs.find(logs_query)
                         .sort("timestamp", -1)
                         .skip((page - 1) * per_page)
                         .limit(per_page))

        # Get active tab
        active_tab = request.args.get('tab', 'details')

        return render_template(
            'streamsv2/stream_detail.html',
            stream=stream,
            stream_logs=stream_logs,
            page=page,
            total_pages=total_log_pages,
            active_tab=active_tab
        )

    except Exception as e:
        logger.error(f"Error getting stream details: {str(e)}", exc_info=True)
        flash('Error retrieving stream details', 'error')
        return redirect(url_for('streams.view_streams'))

@bp.route('/<uuid>/start', methods=['POST'])
@login_required
@admin_required
def start_stream(uuid):
    """Start stream operations"""
    try:
        # Validate stream and state check
        stream = mongo.db.streams_v2.find_one_and_update(
            {
                "uuid": uuid,
                "organization_uuid": g.organization['uuid'],
                "deleted": {"$ne": True},
                "status": {"$nin": [StreamStatus.ACTIVE, StreamStatus.STARTING]}
            },
            {
                "$set": {
                    "status": StreamStatus.STARTING,
                    "updated_at": datetime.utcnow(),
                    "last_error": None,
                    "last_error_time": None
                }
            },
            return_document=True
        )

        if not stream:
            current_status = mongo.db.streams_v2.find_one(
                {"uuid": uuid},
                {"status": 1}
            )
            if current_status and current_status.get('status') == StreamStatus.ACTIVE:
                logger.warning(f"Attempted to start already active stream {uuid}")
                return jsonify({
                    'status': 'error',
                    'message': 'Stream is already active'
                }), 400
            return jsonify({
                'status': 'error',
                'message': 'Stream not found or in invalid state'
            }), 404

        # Validate endpoints
        source_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream['source_endpoint_uuid'],
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream['destination_endpoint_uuid'],
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })

        if not source_endpoint or not destination_endpoint:
            error_msg = "One or more endpoints not found or not accessible"
            logger.error(f"Stream {uuid} start failed: {error_msg}")
            
            # Update stream with error
            mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.ERROR,
                        "last_error": error_msg,
                        "last_error_time": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            return jsonify({
                'status': 'error',
                'message': error_msg
            }), 404

        # Clean document for StreamConfig
        if '_id' in stream:
            del stream['_id']

        # Create config using from_dict to handle extra fields safely
        config = StreamConfig.from_dict(stream)
        
        # Validate stream configuration
        valid, error = config.validate()
        if not valid:
            logger.error(f"Stream {uuid} configuration validation failed: {error}")
            
            # Update stream with validation error
            mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.ERROR,
                        "last_error": f"Invalid stream configuration: {error}",
                        "last_error_time": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            return jsonify({
                'status': 'error',
                'message': f'Invalid stream configuration: {error}'
            }), 400

        # Create processor and start stream
        processor = StreamProcessor(config)
        success = processor.start()
        
        if not success:
            error_msg = "Failed to start stream processor"
            logger.error(f"Stream {uuid} start failed: {error_msg}")
            
            # Update stream with error
            mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.ERROR,
                        "last_error": error_msg,
                        "last_error_time": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            return jsonify({
                'status': 'error',
                'message': error_msg
            }), 500

        # Log successful start with enhanced details
        log_message_cycle(
            message_uuid=str(uuid4()),
            event_type="stream_started",
            details={
                "stream_uuid": uuid,
                "stream_name": stream.get('name'),
                "source_endpoint": stream['source_endpoint_uuid'],
                "destination_endpoint": stream['destination_endpoint_uuid'],
                "source_type": stream['metadata']['source_type'],
                "destination_type": stream['metadata']['destination_type'],
                "message_type": stream['message_type']
            },
            organization_uuid=g.organization['uuid'],
            status="success"
        )

        logger.info(f"Stream {uuid} started successfully")
        return jsonify({
            'status': 'success',
            'message': 'Stream started successfully'
        })

    except Exception as e:
        logger.error(f"Error starting stream {uuid}: {str(e)}", exc_info=True)
        
        # Update stream with error
        try:
            mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.ERROR,
                        "last_error": str(e),
                        "last_error_time": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                }
            )
        except Exception as db_error:
            logger.error(f"Failed to update stream error state: {str(db_error)}")
            
        return jsonify({
            'status': 'error',
            'message': f'Error starting stream: {str(e)}'
        }), 500

@bp.route('/<uuid>/stop', methods=['POST'])
@login_required
@admin_required
def stop_stream(uuid):
    """Stop stream operations with graceful shutdown"""
    try:
        # Get stream with state check
        stream = mongo.db.streams_v2.find_one_and_update(
            {
                "uuid": uuid,
                "organization_uuid": g.organization['uuid'],
                "deleted": {"$ne": True},
                "status": StreamStatus.ACTIVE
            },
            {
                "$set": {
                    "status": StreamStatus.STOPPING,
                    "updated_at": datetime.utcnow()
                }
            },
            return_document=True
        )

        if not stream:
            current_status = mongo.db.streams_v2.find_one(
                {"uuid": uuid},
                {"status": 1}
            )
            if not current_status:
                logger.warning(f"Attempted to stop non-existent stream {uuid}")
                return jsonify({
                    'status': 'error',
                    'message': 'Stream not found'
                }), 404
            logger.warning(f"Attempted to stop stream {uuid} in invalid state: {current_status.get('status', 'unknown')}")
            return jsonify({
                'status': 'error',
                'message': f"Cannot stop stream in {current_status.get('status', 'unknown')} state"
            }), 400

        # Clean document and filter fields for StreamConfig
        allowed_fields = {
            'uuid', 'name', 'organization_uuid', 'source_endpoint_uuid',
            'destination_endpoint_uuid', 'message_type', 'status', 'active',
            'created_at', 'updated_at', 'metrics', 'last_error', 'last_error_time'
        }
        
        # Create filtered stream dict with only allowed fields
        filtered_stream = {k: v for k, v in stream.items() 
                         if k in allowed_fields and k != '_id'}

        # Preserve original stream data for logging
        stream_data = {
            'name': stream.get('name'),
            'source_endpoint_uuid': stream.get('source_endpoint_uuid'),
            'destination_endpoint_uuid': stream.get('destination_endpoint_uuid')
        }

        # Stop stream with processor using filtered config
        processor = StreamProcessor(StreamConfig(**filtered_stream))
        success = processor.stop()
        
        if not success:
            error_msg = "Failed to stop stream processor"
            logger.error(f"Stream {uuid} stop failed: {error_msg}")
            mongo.db.streams_v2.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": StreamStatus.ERROR,
                        "updated_at": datetime.utcnow(),
                        "last_error": error_msg,
                        "last_error_time": datetime.utcnow()
                    }
                }
            )
            return jsonify({
                'status': 'error',
                'message': error_msg
            }), 500

        # Update status to INACTIVE after successful stop
        mongo.db.streams_v2.update_one(
            {"uuid": uuid},
            {
                "$set": {
                    "status": StreamStatus.INACTIVE,
                    "updated_at": datetime.utcnow(),
                    "active": False,
                    "last_stopped": datetime.utcnow()
                }
            }
        )

        # Log stop event with enhanced details
        log_message_cycle(
            message_uuid=str(uuid4()),
            event_type="stream_stopped",
            details={
                "stream_uuid": uuid,
                "stream_name": stream_data['name'],
                "reason": "Stopped by user",
                "organization_uuid": g.organization['uuid'],
                "source_endpoint": stream_data['source_endpoint_uuid'],
                "destination_endpoint": stream_data['destination_endpoint_uuid']
            },
            status="success"
        )

        logger.info(f"Stream {uuid} stopped successfully")
        return jsonify({
            'status': 'success',
            'message': 'Stream stopped successfully'
        })

    except Exception as e:
        logger.error(f"Error stopping stream {uuid}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error stopping stream: {str(e)}'
        }), 500

@bp.route('/<uuid>/delete', methods=['POST'])
@login_required
@admin_required
def delete_stream(uuid):
    """Delete stream with cleanup"""
    try:
        # Get and validate stream
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })

        if not stream:
            logger.warning(f"Attempted to delete non-existent stream {uuid}")
            return jsonify({
                'status': 'error',
                'message': 'Stream not found'
            }), 404

        # Stop if active
        if stream.get('status') == StreamStatus.ACTIVE:
            logger.info(f"Stopping active stream {uuid} before deletion")
            # Clean document for StreamConfig
            stream_copy = stream.copy()
            if '_id' in stream_copy:
                del stream_copy['_id']
                
            processor = StreamProcessor(StreamConfig(**stream_copy))
            stop_success = processor.stop()
            if not stop_success:
                error_msg = "Failed to stop active stream before deletion"
                logger.error(f"Stream {uuid} deletion failed: {error_msg}")
                return jsonify({
                    'status': 'error',
                    'message': error_msg
                }), 500

            # Allow time for cleanup
            time.sleep(2)  # Brief delay to ensure cleanup completes

        # Mark as deleted with comprehensive metadata
        result = mongo.db.streams_v2.update_one(
            {"uuid": uuid},
            {
                "$set": {
                    "deleted": True,
                    "deleted_at": datetime.utcnow(),
                    "deleted_by": g.user['email'],
                    "status": StreamStatus.DELETED,
                    "updated_at": datetime.utcnow(),
                    "active": False,
                    "deletion_metadata": {
                        "previous_status": stream.get('status', 'unknown'),
                        "total_messages_processed": stream.get('metrics', {}).get('total_messages_processed', 0),
                        "deletion_reason": "User initiated deletion"
                    }
                }
            }
        )

        if result.modified_count == 0:
            logger.error(f"Failed to update deletion status for stream {uuid}")
            return jsonify({
                'status': 'error',
                'message': 'Failed to delete stream'
            }), 500

        # Log deletion with enhanced details
        log_message_cycle(
            message_uuid=str(uuid4()),
            event_type="stream_deleted",
            details={
                "stream_uuid": uuid,
                "stream_name": stream.get('name'),
                "deleted_by": g.user['email'],
                "organization_uuid": g.organization['uuid'],
                "previous_status": stream.get('status', 'unknown'),
                "source_endpoint": stream['source_endpoint_uuid'],
                "destination_endpoint": stream['destination_endpoint_uuid'],
                "total_messages_processed": stream.get('metrics', {}).get('total_messages_processed', 0)
            },
            status="success"
        )

        logger.info(f"Stream {uuid} deleted successfully by {g.user['email']}")
        return jsonify({
            'status': 'success',
            'message': 'Stream deleted successfully'
        })

    except Exception as e:
        logger.error(f"Error deleting stream {uuid}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error deleting stream: {str(e)}'
        }), 500


# Part 4: Stream Monitoring and Additional Operations

@bp.route('/<uuid>/status', methods=['GET'])
@login_required
def get_stream_status(uuid):
    """Get detailed stream status and metrics"""
    try:
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid']
        })

        if not stream:
            logger.warning(f"Status check attempted for non-existent stream {uuid}")
            return jsonify({
                'status': 'error',
                'message': 'Stream not found'
            }), 404

        # Get endpoints status with detailed info
        source_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('source_endpoint_uuid')
        })
        destination_endpoint = mongo.db.endpoints.find_one({
            "uuid": stream.get('destination_endpoint_uuid')
        })

        # Get recent errors if any
        recent_errors = list(mongo.db.message_cycle_logs.find({
            "organization_uuid": g.organization['uuid'],
            "details.stream_uuid": uuid,
            "status": "error",
            "timestamp": {"$gte": datetime.utcnow() - timedelta(hours=1)}
        }).sort("timestamp", -1).limit(5))

        # Create comprehensive status response
        status_data = {
            'uuid': stream['uuid'],
            'name': stream['name'],
            'status': stream.get('status', StreamStatus.INACTIVE),
            'last_active': stream.get('last_active'),
            'metrics': _get_stream_metrics(stream),
            'endpoints': {
                'source': {
                    'uuid': source_endpoint.get('uuid'),
                    'name': source_endpoint.get('name'),
                    'status': source_endpoint.get('status') if source_endpoint else 'NOT_FOUND',
                    'last_active': source_endpoint.get('last_active') if source_endpoint else None,
                    'type': source_endpoint.get('endpoint_type') if source_endpoint else None
                },
                'destination': {
                    'uuid': destination_endpoint.get('uuid'),
                    'name': destination_endpoint.get('name'),
                    'status': destination_endpoint.get('status') if destination_endpoint else 'NOT_FOUND',
                    'last_active': destination_endpoint.get('last_active') if destination_endpoint else None,
                    'type': destination_endpoint.get('endpoint_type') if destination_endpoint else None
                }
            },
            'recent_errors': [{
                'timestamp': error.get('timestamp'),
                'message': error.get('details', {}).get('error'),
                'type': error.get('event_type')
            } for error in recent_errors],
            'updated_at': stream.get('updated_at'),
            'configuration': {
                'message_type': stream.get('message_type'),
                'source_type': stream.get('metadata', {}).get('source_type'),
                'destination_type': stream.get('metadata', {}).get('destination_type')
            }
        }

        logger.debug(f"Status retrieved for stream {uuid}")
        return jsonify(status_data), 200

    except Exception as e:
        logger.error(f"Error getting stream status {uuid}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error getting stream status: {str(e)}'
        }), 500

@bp.route('/<uuid>/metrics', methods=['GET'])
@login_required
def get_stream_metrics(uuid):
    """Get detailed stream metrics"""
    try:
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid']
        })

        if not stream:
            return jsonify({
                'status': 'error',
                'message': 'Stream not found'
            }), 404

        # Get recent metrics
        metrics_query = {
            "stream_uuid": uuid,
            "timestamp": {
                "$gte": datetime.utcnow() - timedelta(hours=24)
            }
        }

        recent_metrics = list(mongo.db.stream_metrics.find(metrics_query)
                            .sort("timestamp", -1)
                            .limit(100))

        metrics_data = {
            'current': stream.get('metrics', {}),
            'history': recent_metrics,
            'updated_at': datetime.utcnow()
        }

        return jsonify(metrics_data), 200

    except Exception as e:
        logger.error(f"Error getting stream metrics {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error getting stream metrics: {str(e)}'
        }), 500

@bp.route('/<uuid>/logs', methods=['GET'])
@login_required
def get_stream_logs(uuid):
    """Get stream logs with filtering and pagination"""
    try:
        # Validate stream access
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid']
        })

        if not stream:
            logger.warning(f"Log request for non-existent stream {uuid}")
            return jsonify({
                'status': 'error',
                'message': 'Stream not found'
            }), 404

        # Get query parameters with defaults
        page = int(request.args.get('page', 1))
        per_page = min(int(request.args.get('per_page', 50)), 100)  # Limit max records
        log_type = request.args.get('type')
        severity = request.args.get('severity')
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')

        # Build query
        query = {
            "organization_uuid": g.organization['uuid'],
            "$or": [
                {"details.stream_uuid": uuid},
                {"details.stream_id": uuid}
            ]
        }

        if log_type:
            query["event_type"] = log_type
        if severity:
            query["severity"] = severity
        if start_time or end_time:
            query["timestamp"] = {}
            if start_time:
                query["timestamp"]["$gte"] = datetime.fromisoformat(start_time)
            if end_time:
                query["timestamp"]["$lte"] = datetime.fromisoformat(end_time)

        # Get logs with pagination
        total_logs = mongo.db.message_cycle_logs.count_documents(query)
        total_pages = (total_logs + per_page - 1) // per_page

        logs = list(mongo.db.message_cycle_logs.find(query)
                   .sort("timestamp", -1)
                   .skip((page - 1) * per_page)
                   .limit(per_page))

        # Enhance log data
        enhanced_logs = []
        for log in logs:
            if '_id' in log:
                del log['_id']
            log['formatted_timestamp'] = log['timestamp'].isoformat()
            enhanced_logs.append(log)

        logger.debug(f"Retrieved {len(enhanced_logs)} logs for stream {uuid}")
        return jsonify({
            'logs': enhanced_logs,
            'page': page,
            'total_pages': total_pages,
            'total_logs': total_logs,
            'per_page': per_page
        }), 200

    except Exception as e:
        logger.error(f"Error getting stream logs {uuid}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error getting stream logs: {str(e)}'
        }), 500

@bp.route('/<uuid>/reset', methods=['POST'])
@login_required
@admin_required
def reset_stream(uuid):
    """Reset stream state and clear errors"""
    try:
        stream = mongo.db.streams_v2.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })

        if not stream:
            logger.warning(f"Reset attempted for non-existent stream {uuid}")
            return jsonify({
                'status': 'error',
                'message': 'Stream not found'
            }), 404

        if stream.get('status') == StreamStatus.ACTIVE:
            logger.warning(f"Reset attempted for active stream {uuid}")
            return jsonify({
                'status': 'error',
                'message': 'Cannot reset active stream'
            }), 400

        # Store previous state for logging
        previous_state = {
            'status': stream.get('status'),
            'metrics': stream.get('metrics', {}),
            'last_error': stream.get('last_error'),
            'last_error_time': stream.get('last_error_time')
        }

        # Reset stream state
        result = mongo.db.streams_v2.update_one(
            {"uuid": uuid},
            {
                "$set": {
                    "status": StreamStatus.INACTIVE,
                    "metrics": {
                        "total_messages_processed": 0,
                        "total_messages_failed": 0,
                        "total_bytes_transferred": 0,
                        "error_rate": 0.0,
                        "processing_time_avg": 0.0,
                        "last_processed_at": None,
                        "last_error": None,
                        "last_error_time": None
                    },
                    "updated_at": datetime.utcnow(),
                    "reset_history": {
                        "last_reset": datetime.utcnow(),
                        "reset_by": g.user['email'],
                        "previous_state": previous_state
                    }
                }
            }
        )

        if result.modified_count == 0:
            logger.error(f"Failed to reset stream {uuid}")
            return jsonify({
                'status': 'error',
                'message': 'Failed to reset stream'
            }), 500

        # Reset associated message statuses
        mongo.db.messages.update_many(
            {
                "stream_tracker.stream_uuid": uuid,
                "stream_tracker.transformation_status": {"$in": [
                    "ERROR", "QUEUED", "PROCESSING"
                ]}
            },
            {
                "$set": {
                    "stream_tracker.$.transformation_status": "CANCELLED",
                    "stream_tracker.$.updated_at": datetime.utcnow(),
                    "stream_tracker.$.reset_by": g.user['email']
                }
            }
        )

        # Log reset event with enhanced details
        log_message_cycle(
            message_uuid=str(uuid4()),
            event_type="stream_reset",
            details={
                "stream_uuid": uuid,
                "stream_name": stream.get('name'),
                "reset_by": g.user['email'],
                "organization_uuid": g.organization['uuid'],
                "previous_state": previous_state,
                "source_endpoint": stream['source_endpoint_uuid'],
                "destination_endpoint": stream['destination_endpoint_uuid']
            },
            status="success"
        )

        logger.info(f"Stream {uuid} reset successfully by {g.user['email']}")
        return jsonify({
            'status': 'success',
            'message': 'Stream reset successfully'
        })

    except Exception as e:
        logger.error(f"Error resetting stream {uuid}: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'message': f'Error resetting stream: {str(e)}'
        }), 500

@bp.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        # Check MongoDB connection
        mongo.db.command('ping')
        
        # Get active streams count
        active_streams = mongo.db.streams_v2.count_documents({
            "status": StreamStatus.ACTIVE,
            "deleted": {"$ne": True}
        })
        
        return jsonify({
            'status': 'healthy',
            'active_streams': active_streams,
            'database': 'connected',
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

# Error handlers
@bp.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors"""
    logger.error(f'404 error occurred: {str(error)}')
    return render_template('404.html'), 404

@bp.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f'500 error occurred: {str(error)}')
    return render_template('500.html'), 500

@bp.app_errorhandler(Exception)
def handle_unexpected_error(error):
    """Handle unexpected errors"""
    logger.error(f'Unexpected error occurred: {str(error)}', exc_info=True)
    return render_template('500.html'), 500