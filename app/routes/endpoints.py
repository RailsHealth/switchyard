# app/routes/endpoints.py

import threading
from typing import Dict, Any, Optional, List, Tuple
from flask import Blueprint, request, jsonify, render_template, redirect
from flask import url_for, current_app, flash, g
from flask_wtf import FlaskForm
from wtforms import (StringField, IntegerField, SelectField, BooleanField, 
                    RadioField, TextAreaField, HiddenField, PasswordField)
from wtforms.validators import (DataRequired, NumberRange, Optional as OptionalValidator, 
                              ValidationError, Regexp, URL, Length)
from app.extensions import mongo
from app.auth.decorators import login_required, admin_required
from datetime import datetime
import uuid
from pymongo.errors import PyMongoError
import logging
import json
import re

# Import endpoint implementations
from app.endpoints.base import BaseEndpoint
from app.endpoints.mllp_endpoint import MLLPEndpoint
from app.endpoints.https_endpoint import HTTPSEndpoint
from app.endpoints.sftp_endpoint import SFTPEndpoint
from app.endpoints.cloud_storage_endpoint import CloudStorageEndpoint, CloudStorageError
from app.endpoints.endpoint_types import EndpointConfig, EndpointStatus

# Create blueprint
bp = Blueprint('endpoints', __name__)

@bp.app_template_filter('datetime')
def format_datetime(value):
    """Format datetime for display"""
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return value

# Initialize logger
logger = logging.getLogger(__name__)

# Form Validators
def validate_bucket_name(form, field):
    """Validate cloud storage bucket name format"""
    if not field.data:
        return
    
    # Determine provider based on form type
    if isinstance(form, AWSS3EndpointForm):
        pattern = r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$'
        error_msg = 'Invalid S3 bucket name format'
    else:  # GCP
        pattern = r'^[a-z0-9][a-z0-9-_.]*[a-z0-9]$'
        error_msg = 'Invalid GCP bucket name format'
        
    if not re.match(pattern, field.data):
        raise ValidationError(error_msg)

def validate_json_credentials(form, field):
    """Validate JSON format for GCP credentials"""
    if not field.data:
        return
    try:
        json.loads(field.data)
    except json.JSONDecodeError:
        raise ValidationError('Invalid JSON format')

# Form Definitions
class FilterForm(FlaskForm):
    """Form for endpoint list filtering"""
    status = SelectField('Status', choices=[
        ('', 'Filter by status'),
        ('active', 'Active (Running)'),
        ('inactive', 'Inactive (Stopped)'),
        ('deleted', 'Deleted')
    ])
    mode = SelectField('Mode', choices=[
        ('', 'Filter by mode'),
        ('source', 'Source'),
        ('destination', 'Destination')
    ])
    message_type = SelectField('Message Type')  # Changed from 'type' to 'message_type'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        try:
            # Get message types from config
            message_types = []
            
            # Add source message types
            source_types = current_app.config['STREAMSV2_MESSAGE_TYPES']['source'].keys()
            message_types.extend([(t, t) for t in source_types])
            
            # Add destination message types
            dest_types = current_app.config['STREAMSV2_MESSAGE_TYPES']['destination'].keys()
            message_types.extend([(t, t) for t in dest_types])
            
            # Set choices with placeholder
            self.message_type.choices = [('', 'Filter by message type')] + sorted(list(set(message_types)))
            
        except Exception as e:
            current_app.logger.error(f"Error setting up filter form: {str(e)}")
            # Provide fallback choices if config access fails
            self.message_type.choices = [
                ('', 'Filter by message type'),
                ('HL7v2', 'HL7v2'),
                ('HL7v2 in JSON', 'HL7v2 (JSON)'),
                ('CCDA', 'CCDA'),
                ('CCDA in JSON', 'CCDA (JSON)'),
                ('X12', 'X12'),
                ('X12 in JSON', 'X12 (JSON)'),
                ('Clinical Notes', 'Clinical Notes'),
                ('Clinical Notes in JSON', 'Clinical Notes (JSON)')
            ]

class EndpointModeForm(FlaskForm):
    """Form for initial endpoint mode selection"""
    mode = RadioField(
        'Mode',
        choices=[
            ('destination', 'Destination'),
            ('source', 'Source'),
            ('test', 'Test Endpoints')
        ],
        validators=[DataRequired()]
    )

class EndpointOptionsForm(FlaskForm):
    """Form for message type and endpoint type selection"""
    message_type = SelectField('Message Type', validators=[DataRequired()])
    endpoint_type = SelectField('Endpoint Type', validators=[DataRequired()])

    def __init__(self, *args, mode=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = mode
        self.setup_choices(mode)
        
    def is_submitted(self):
        """Override to handle AJAX submissions"""
        return super().is_submitted() or request.is_xhr

    def setup_choices(self, mode):
        """Setup form choices based on mode"""
        if mode == 'source':
            self.message_type.choices = [
                ('', 'Select message type'),
                ('HL7v2', 'HL7v2'),
                ('CCDA', 'CCDA (supports PDF only)'),
                ('X12', 'X12 (supports PDF only)'),
                ('Clinical Notes', 'Clinical Notes (supports PDF only)')
            ]
        elif mode == 'destination':
            self.message_type.choices = [
                ('', 'Select message type'),
                ('HL7v2 in JSON', 'HL7v2 (JSON)'),
                ('CCDA in JSON', 'CCDA (JSON)'),
                ('X12 in JSON', 'X12 (JSON)'),
                ('Clinical Notes in JSON', 'Clinical Notes (JSON)')
            ]

        # Initialize with empty endpoint type choices
        self.endpoint_type.choices = [('', 'Select message type first')]

class MLLPEndpointForm(FlaskForm):
    """Form for MLLP endpoint configuration"""
    name = StringField('Endpoint Name', validators=[
        DataRequired(),
        Length(min=3, max=64)
    ])
    host = StringField('Host', validators=[
        DataRequired(),
        Regexp(r'^[a-zA-Z0-9\-\.]+$|^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$', 
               message="Invalid host format. Use hostname or IP address")
    ])
    port = IntegerField('Port', validators=[
        DataRequired(),
        NumberRange(min=1, max=65535, message="Port must be between 1 and 65535")
    ])
    timeout = IntegerField('Timeout (seconds)', validators=[
        DataRequired(),
        NumberRange(min=1, max=300, message="Timeout must be between 1 and 300 seconds")
    ], default=60)

class SFTPEndpointForm(FlaskForm):
    """Form for SFTP endpoint configuration"""
    name = StringField('Name', validators=[
        DataRequired(),
        Length(min=3, max=64)
    ])
    host = StringField('Host', validators=[DataRequired()])
    port = IntegerField('Port', validators=[
        DataRequired(),
        NumberRange(min=1, max=65535)
    ])
    username = StringField('Username', validators=[DataRequired()])
    auth_method = SelectField(
        'Authentication Method',
        choices=[('password', 'Password'), ('key', 'SSH Key')]
    )
    password = PasswordField('Password')
    private_key = TextAreaField('Private Key')
    remote_path = StringField('Remote Path', validators=[DataRequired()])
    file_pattern = StringField('File Pattern', validators=[OptionalValidator()])
    fetch_interval = IntegerField(
        'Fetch Interval (seconds)',
        validators=[DataRequired(), NumberRange(min=1)]
    )

    def validate_password(self, field):
        if self.auth_method.data == 'password' and not field.data:
            raise ValidationError('Password is required')

    def validate_private_key(self, field):
        if self.auth_method.data == 'key' and not field.data:
            raise ValidationError('Private key is required')

class AWSS3EndpointForm(FlaskForm):
    """Form for AWS S3 endpoint configuration"""
    name = StringField('Name', validators=[
        DataRequired(),
        Length(min=3, max=64)
    ])
    aws_bucket = StringField('Bucket Name', validators=[
        DataRequired(),
        validate_bucket_name
    ])
    aws_region = SelectField('Region', choices=[
        ('us-east-1', 'US East (N. Virginia)'),
        ('us-east-2', 'US East (Ohio)'),
        ('us-west-1', 'US West (N. California)'),
        ('us-west-2', 'US West (Oregon)'),
        ('eu-west-1', 'Europe (Ireland)'),
        ('eu-west-2', 'Europe (London)'),
        ('eu-central-1', 'Europe (Frankfurt)'),
        ('ap-southeast-1', 'Asia Pacific (Singapore)'),
        ('ap-southeast-2', 'Asia Pacific (Sydney)'),
        ('ap-northeast-1', 'Asia Pacific (Tokyo)')
    ])
    aws_access_key = StringField('Access Key ID', validators=[DataRequired()])
    aws_secret_key = PasswordField('Secret Access Key', validators=[DataRequired()])
    aws_encryption = BooleanField('Enable Server-Side Encryption')

class GCPStorageEndpointForm(FlaskForm):
    """Form for GCP Storage endpoint configuration"""
    name = StringField('Name', validators=[
        DataRequired(),
        Length(min=3, max=64)
    ])
    gcp_bucket = StringField('Bucket Name', validators=[
        DataRequired(),
        validate_bucket_name
    ])
    gcp_project_id = StringField('Project ID', validators=[DataRequired()])
    gcp_credentials = TextAreaField('Service Account Credentials', validators=[
        DataRequired(),
        validate_json_credentials
    ])
    gcp_encryption = BooleanField('Enable Server-Side Encryption')

class TestEndpointForm(FlaskForm):
    """Form for test endpoint creation"""
    test_type = SelectField(
        'Test Type', 
        choices=[
            ('hl7v2', 'HL7v2 Test Server'),
            ('firely', 'Firely FHIR Server'),
            ('hapi', 'HAPI FHIR Server')
        ],
        validators=[DataRequired()]
    )

# Helper Functions
def _get_endpoint_instance(endpoint_data: Dict[str, Any]) -> Optional[BaseEndpoint]:
    """
    Create endpoint instance based on configuration
    
    Args:
        endpoint_data: Endpoint configuration dictionary
        
    Returns:
        BaseEndpoint instance or None if creation fails
    """
    try:
        # Debug logging
        current_app.logger.debug(f"Creating endpoint instance with config: {endpoint_data}")
        
        # Create endpoint config
        config = EndpointConfig.from_dict(endpoint_data)
        
        # Validate configuration
        is_valid, error = config.validate()
        if not is_valid:
            current_app.logger.error(f"Invalid endpoint configuration: {error}")
            raise ValueError(f"Invalid endpoint configuration: {error}")
        
        # Remove base fields from extra config
        extra_config = {k: v for k, v in endpoint_data.items() 
                       if k not in EndpointConfig.__annotations__}
        
        current_app.logger.debug(f"Extra config for endpoint: {extra_config}")
        
        # Create instance based on endpoint type
        if config.endpoint_type == 'mllp':
            # Additional validation for MLLP config
            if not isinstance(extra_config.get('port'), int):
                raise ValueError(f"Invalid port number: {extra_config.get('port')}")
            return MLLPEndpoint(config=config, **extra_config)
            
        elif config.endpoint_type == 'sftp':
            return SFTPEndpoint(config=config, **extra_config)
            
        elif config.endpoint_type == 'https':
            return HTTPSEndpoint(config=config, **extra_config)
            
        elif config.endpoint_type in ['aws_s3', 'gcp_storage']:
            return CloudStorageEndpoint(config=config, **extra_config)
        
        raise ValueError(f"Unsupported endpoint type: {config.endpoint_type}")
        
    except Exception as e:
        current_app.logger.error(f"Error creating endpoint instance: {str(e)}")
        raise ValueError(f"Failed to create endpoint instance: {str(e)}")

def check_duplicate_endpoint(mode: str, message_type: str, endpoint_type: str, 
                           organization_uuid: str, **extra_params) -> bool:
    """Check for duplicate endpoints"""
    query = {
        "mode": mode,
        "message_type": message_type,
        "endpoint_type": endpoint_type,
        "organization_uuid": organization_uuid,
        "deleted": {"$ne": True}
    }
    
    # Add type-specific checks
    if endpoint_type == 'sftp':
        query["host"] = extra_params.get('host')
        query["remote_path"] = extra_params.get('remote_path')
    elif endpoint_type in ['aws_s3', 'gcp_storage']:
        storage_config = extra_params.get('storage_config', {})
        query["storage_config.bucket"] = storage_config.get('bucket')
    
    return mongo.db.endpoints.find_one(query) is not None

# Route Handlers
@bp.route('/view', methods=['GET'])
@login_required
def view_endpoints():
    """List all endpoints for current organization"""
    try:
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        form = FilterForm()
        
        # Get filter parameters
        page = int(request.args.get('page', 1))
        per_page = 10
        status_filter = request.args.get('status', '')
        mode_filter = request.args.get('mode', '')
        message_type_filter = request.args.get('message_type', '')  # Changed from type_filter

        # Build query
        base_query = {
            "organization_uuid": g.organization['uuid']
        }

        # Add filters only if they are not empty
        if status_filter:
            if status_filter == 'active':
                base_query["status"] = EndpointStatus.ACTIVE
                base_query["deleted"] = {"$ne": True}
            elif status_filter == 'inactive':
                base_query["status"] = EndpointStatus.INACTIVE
                base_query["deleted"] = {"$ne": True}
            elif status_filter == 'deleted':
                base_query["deleted"] = True

        if mode_filter:
            base_query["mode"] = mode_filter

        if message_type_filter:  # Changed from type_filter
            base_query["message_type"] = message_type_filter

        # Get total count and paginated results
        total_endpoints = mongo.db.endpoints.count_documents(base_query)
        total_pages = (total_endpoints + per_page - 1) // per_page

        endpoints = list(mongo.db.endpoints.find(base_query)
                       .sort([("deleted", 1), ("name", 1)])
                       .skip((page - 1) * per_page)
                       .limit(per_page))

        return render_template(
            'view_endpoints.html',
            endpoints=endpoints,
            page=page,
            total_pages=total_pages,
            form=form,
            has_filters=any([status_filter, mode_filter, message_type_filter]),
            status_filter=status_filter,
            mode_filter=mode_filter,
            message_type_filter=message_type_filter,
            show_filters=True
        )

    except Exception as e:
        current_app.logger.error(f"Error in view_endpoints: {str(e)}")
        # Even in case of error, create a form to avoid template errors
        form = FilterForm()
        return render_template(
            'view_endpoints.html',
            endpoints=[],
            form=form,
            show_filters=True,
            error=True
        )

@bp.route('/new', methods=['GET', 'POST'])
@login_required
@admin_required
def create_endpoint():
    """Initial endpoint creation - select mode"""
    if not g.organization:
        flash('Please select an organization', 'warning')
        return redirect(url_for('organizations.list_organizations'))

    form = EndpointModeForm()
    if form.validate_on_submit():
        mode = form.mode.data
        if mode == 'test':
            return redirect(url_for('endpoints.create_test_endpoint'))
        return redirect(url_for('endpoints.select_endpoint_options', mode=mode))

    return render_template('new_endpoint.html', form=form)

@bp.route('/<mode>/options', methods=['GET', 'POST'])
@login_required
@admin_required
def select_endpoint_options(mode):
    """Select message type and endpoint type"""
    if mode not in ['source', 'destination']:
        flash('Invalid endpoint mode', 'error')
        return redirect(url_for('endpoints.create_endpoint'))

    form = EndpointOptionsForm(mode=mode)
    
    if request.method == 'POST':
        current_app.logger.debug(f"Form Data: {request.form}")
        
        message_type = request.form.get('message_type')
        endpoint_type = request.form.get('endpoint_type')
        
        if not message_type or not endpoint_type:
            return jsonify({
                'status': 'error',
                'message': 'Please select both message type and endpoint type'
            }), 400

        # Get valid endpoint types from configuration
        valid_endpoints = []
        if mode == 'destination':
            endpoint_mappings = current_app.config['STREAMSV2_ENDPOINT_MAPPINGS']['destination']
            for etype, config in endpoint_mappings.items():
                if message_type in config['supported_types']:
                    valid_endpoints.append(etype)
        else:
            # Handle source mode similarly
            endpoint_mappings = current_app.config['STREAMSV2_ENDPOINT_MAPPINGS']['source']
            for etype, config in endpoint_mappings.items():
                if message_type in config['supported_types']:
                    valid_endpoints.append(etype)

        if endpoint_type not in valid_endpoints:
            return jsonify({
                'status': 'error',
                'message': 'Invalid endpoint type for selected message type'
            }), 400

        # Redirect to configuration page
        redirect_url = url_for(
            'endpoints.configure_endpoint',
            mode=mode,
            message_type=message_type,
            endpoint_type=endpoint_type
        )
        
        return jsonify({
            'status': 'success',
            'redirect': redirect_url
        })

    return render_template(
        'select_endpoint_options.html',
        form=form,
        mode=mode,
        endpoint_mappings=current_app.config['ENDPOINT_MAPPINGS']
    )

@bp.route('/configure/<mode>/<message_type>/<endpoint_type>', methods=['GET', 'POST'])
@login_required
@admin_required
def configure_endpoint(mode, message_type, endpoint_type):
    """Configure specific endpoint type"""
    try:
        # Debug logging
        current_app.logger.debug(f"Configure endpoint request - Mode: {mode}, Message Type: {message_type}, Endpoint Type: {endpoint_type}")

        # Validate parameters
        if mode not in ['source', 'destination']:
            flash('Invalid endpoint mode', 'error')
            return redirect(url_for('endpoints.create_endpoint'))

        if endpoint_type == 'mllp':
            form = MLLPEndpointForm()
            template = 'configure_mllp_endpoint.html'
            
            if form.validate_on_submit():
                try:
                    # Create base endpoint configuration
                    endpoint_config = {
                        "uuid": str(uuid.uuid4()),
                        "name": form.name.data,
                        "mode": mode,
                        "message_type": message_type,
                        "endpoint_type": endpoint_type,
                        "organization_uuid": g.organization['uuid'],
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                        "status": EndpointStatus.INACTIVE,
                        "metadata": {
                            "created_by": g.user['email']
                        },
                        # Add MLLP specific configuration
                        "host": form.host.data,
                        "port": int(form.port.data),  # Ensure port is converted to integer
                        "timeout": form.timeout.data
                    }

                    current_app.logger.debug(f"MLLP Endpoint config: {endpoint_config}")

                    # Validate configuration
                    endpoint_instance = _get_endpoint_instance(endpoint_config)
                    if not endpoint_instance:
                        raise ValueError("Failed to create endpoint instance")

                    # Store configuration
                    result = mongo.db.endpoints.insert_one(endpoint_config)
                    if not result.acknowledged:
                        raise ValueError("Failed to save endpoint configuration")

                    flash('Endpoint created successfully', 'success')
                    return redirect(url_for('endpoints.endpoint_detail', uuid=endpoint_config['uuid']))

                except Exception as e:
                    current_app.logger.error(f"Error creating MLLP endpoint: {str(e)}")
                    flash(f'Error creating endpoint: {str(e)}', 'error')
                    return render_template(template, form=form, mode=mode, message_type=message_type)

        elif endpoint_type == 'sftp':
            form = SFTPEndpointForm()
            template = 'configure_sftp_endpoint.html'
            
            if form.validate_on_submit():
                try:
                    # Create base endpoint configuration
                    endpoint_config = {
                        "uuid": str(uuid.uuid4()),
                        "name": form.name.data,
                        "mode": mode,
                        "message_type": message_type,
                        "endpoint_type": endpoint_type,
                        "organization_uuid": g.organization['uuid'],
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                        "status": EndpointStatus.INACTIVE,
                        "metadata": {
                            "created_by": g.user['email']
                        },
                        # Add SFTP specific configuration
                        "host": form.host.data,
                        "port": int(form.port.data),
                        "username": form.username.data,
                        "remote_path": form.remote_path.data,
                        "file_pattern": form.file_pattern.data,
                        "fetch_interval": form.fetch_interval.data
                    }

                    # Add authentication based on method
                    if form.auth_method.data == 'password':
                        endpoint_config["password"] = form.password.data
                    else:
                        endpoint_config["private_key"] = form.private_key.data

                    current_app.logger.debug(f"SFTP Endpoint config: {endpoint_config}")

                    # Check for duplicate endpoints
                    if check_duplicate_endpoint(
                        mode=mode,
                        message_type=message_type,
                        endpoint_type=endpoint_type,
                        organization_uuid=g.organization['uuid'],
                        host=endpoint_config['host'],
                        remote_path=endpoint_config['remote_path']
                    ):
                        flash('An endpoint with these settings already exists', 'error')
                        return render_template(template, form=form, mode=mode, message_type=message_type)

                    # Validate configuration
                    endpoint_instance = _get_endpoint_instance(endpoint_config)
                    if not endpoint_instance:
                        raise ValueError("Failed to create endpoint instance")

                    # Store configuration
                    result = mongo.db.endpoints.insert_one(endpoint_config)
                    if not result.acknowledged:
                        raise ValueError("Failed to save endpoint configuration")

                    flash('Endpoint created successfully', 'success')
                    return redirect(url_for('endpoints.endpoint_detail', uuid=endpoint_config['uuid']))

                except Exception as e:
                    current_app.logger.error(f"Error creating SFTP endpoint: {str(e)}")
                    flash(f'Error creating endpoint: {str(e)}', 'error')
                    return render_template(template, form=form, mode=mode, message_type=message_type)

        elif endpoint_type == 'aws_s3':
            form = AWSS3EndpointForm()
            template = 'configure_aws_s3_endpoint.html'
            
            if form.validate_on_submit():
                try:
                    # Create base endpoint configuration
                    endpoint_config = {
                        "uuid": str(uuid.uuid4()),
                        "name": form.name.data,
                        "mode": mode,
                        "message_type": message_type,
                        "endpoint_type": endpoint_type,
                        "organization_uuid": g.organization['uuid'],
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                        "status": EndpointStatus.INACTIVE,
                        "provider": "aws_s3",
                        "metadata": {
                            "created_by": g.user['email']
                        },
                        # Add S3 specific configuration
                        "storage_config": {
                            "bucket": form.aws_bucket.data,
                            "region": form.aws_region.data,
                            "aws_access_key_id": form.aws_access_key.data,
                            "aws_secret_access_key": form.aws_secret_key.data,
                            "encryption": {
                                "type": "AES256" if form.aws_encryption.data else "none"
                            }
                        }
                    }

                    current_app.logger.debug(f"AWS S3 Endpoint config: {endpoint_config}")

                    # Check for duplicate endpoints
                    if check_duplicate_endpoint(
                        mode=mode,
                        message_type=message_type,
                        endpoint_type=endpoint_type,
                        organization_uuid=g.organization['uuid'],
                        storage_config=endpoint_config['storage_config']
                    ):
                        flash('An endpoint with these settings already exists', 'error')
                        return render_template(template, form=form, mode=mode, message_type=message_type)

                    # Validate configuration
                    endpoint_instance = _get_endpoint_instance(endpoint_config)
                    if not endpoint_instance:
                        raise ValueError("Failed to create endpoint instance")

                    # Store configuration
                    result = mongo.db.endpoints.insert_one(endpoint_config)
                    if not result.acknowledged:
                        raise ValueError("Failed to save endpoint configuration")

                    flash('Endpoint created successfully', 'success')
                    return redirect(url_for('endpoints.endpoint_detail', uuid=endpoint_config['uuid']))

                except Exception as e:
                    current_app.logger.error(f"Error creating AWS S3 endpoint: {str(e)}")
                    flash(f'Error creating endpoint: {str(e)}', 'error')
                    return render_template(template, form=form, mode=mode, message_type=message_type)

        elif endpoint_type == 'gcp_storage':
            form = GCPStorageEndpointForm()
            template = 'configure_gcp_storage_endpoint.html'
            
            if form.validate_on_submit():
                try:
                    # Create base endpoint configuration
                    endpoint_config = {
                        "uuid": str(uuid.uuid4()),
                        "name": form.name.data,
                        "mode": mode,
                        "message_type": message_type,
                        "endpoint_type": endpoint_type,
                        "organization_uuid": g.organization['uuid'],
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                        "status": EndpointStatus.INACTIVE,
                        "provider": "gcp_storage",
                        "metadata": {
                            "created_by": g.user['email']
                        },
                        # Add GCP Storage specific configuration
                        "storage_config": {
                            "bucket": form.gcp_bucket.data,
                            "project_id": form.gcp_project_id.data,
                            "credentials_json": form.gcp_credentials.data,
                            "encryption": {
                                "type": "google-managed" if form.gcp_encryption.data else "none"
                            }
                        }
                    }

                    current_app.logger.debug(f"GCP Storage Endpoint config: {endpoint_config}")

                    # Check for duplicate endpoints
                    if check_duplicate_endpoint(
                        mode=mode,
                        message_type=message_type,
                        endpoint_type=endpoint_type,
                        organization_uuid=g.organization['uuid'],
                        storage_config=endpoint_config['storage_config']
                    ):
                        flash('An endpoint with these settings already exists', 'error')
                        return render_template(template, form=form, mode=mode, message_type=message_type)

                    # Validate configuration
                    endpoint_instance = _get_endpoint_instance(endpoint_config)
                    if not endpoint_instance:
                        raise ValueError("Failed to create endpoint instance")

                    # Store configuration
                    result = mongo.db.endpoints.insert_one(endpoint_config)
                    if not result.acknowledged:
                        raise ValueError("Failed to save endpoint configuration")

                    flash('Endpoint created successfully', 'success')
                    return redirect(url_for('endpoints.endpoint_detail', uuid=endpoint_config['uuid']))

                except Exception as e:
                    current_app.logger.error(f"Error creating GCP Storage endpoint: {str(e)}")
                    flash(f'Error creating endpoint: {str(e)}', 'error')
                    return render_template(template, form=form, mode=mode, message_type=message_type)

        elif endpoint_type == 'https':
            form = HTTPSEndpointForm()  # You'll need to create this form class
            template = 'configure_https_endpoint.html'
            
            if form.validate_on_submit():
                try:
                    # Create base endpoint configuration for HTTPS/FHIR endpoint
                    endpoint_config = {
                        "uuid": str(uuid.uuid4()),
                        "name": form.name.data,
                        "mode": mode,
                        "message_type": message_type,
                        "endpoint_type": endpoint_type,
                        "organization_uuid": g.organization['uuid'],
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                        "status": EndpointStatus.INACTIVE,
                        "metadata": {
                            "created_by": g.user['email']
                        },
                        # Add HTTPS specific configuration
                        "base_url": form.base_url.data,
                        "verify_ssl": form.verify_ssl.data,
                        "auth_token": form.auth_token.data,
                        "custom_headers": form.custom_headers.data
                    }

                    current_app.logger.debug(f"HTTPS Endpoint config: {endpoint_config}")

                    # Validate configuration
                    endpoint_instance = _get_endpoint_instance(endpoint_config)
                    if not endpoint_instance:
                        raise ValueError("Failed to create endpoint instance")

                    # Store configuration
                    result = mongo.db.endpoints.insert_one(endpoint_config)
                    if not result.acknowledged:
                        raise ValueError("Failed to save endpoint configuration")

                    flash('Endpoint created successfully', 'success')
                    return redirect(url_for('endpoints.endpoint_detail', uuid=endpoint_config['uuid']))

                except Exception as e:
                    current_app.logger.error(f"Error creating HTTPS endpoint: {str(e)}")
                    flash(f'Error creating endpoint: {str(e)}', 'error')
                    return render_template(template, form=form, mode=mode, message_type=message_type)

        else:
            flash('Invalid endpoint type', 'error')
            return redirect(url_for('endpoints.create_endpoint'))

        # Handle GET request or invalid form submission
        return render_template(
            template,
            form=form,
            mode=mode,
            message_type=message_type
        )

    except Exception as e:
        current_app.logger.error(f"Error configuring endpoint: {str(e)}")
        flash('An error occurred while configuring the endpoint', 'error')
        return redirect(url_for('endpoints.create_endpoint'))

# Test Endpoint Handlers
@bp.route('/new/test', methods=['GET', 'POST'])
@login_required
@admin_required
def create_test_endpoint():
    """Create test endpoint (HL7v2 or FHIR)"""
    try:
        if not g.organization:
            flash('Please select an organization', 'warning')
            return redirect(url_for('organizations.list_organizations'))

        form = TestEndpointForm()
        
        if form.validate_on_submit():
            test_type = form.test_type.data
            if test_type == 'hl7v2':
                return _create_test_hl7v2_endpoint()
            elif test_type in ['firely', 'hapi']:
                return _create_test_fhir_endpoint(test_type)
            else:
                flash('Invalid test endpoint type', 'error')

        # Check for existing test endpoints
        has_hl7v2_test = mongo.db.endpoints.find_one({
            "organization_uuid": g.organization['uuid'],
            "is_test": True,
            "message_type": "HL7v2",
            "deleted": {"$ne": True}
        }) is not None

        return render_template(
            'test_endpoints.html',
            form=form,
            has_hl7v2_test=has_hl7v2_test,
            config=current_app.config
        )

    except Exception as e:
        logger.error(f"Error creating test endpoint: {str(e)}")
        flash('An error occurred while creating the test endpoint', 'error')
        return redirect(url_for('endpoints.view_endpoints'))

def _create_test_hl7v2_endpoint():
    """Create test HL7v2 endpoint"""
    try:
        from app.services.test_hl7_client import TestHL7Client
        client = TestHL7Client()
        result = client.create_test_stream(g.organization['uuid'])

        if not result or not result.get('port'):
            flash('Failed to get port from test service', 'error')
            return redirect(url_for('endpoints.view_endpoints'))

        # Create endpoint configuration
        endpoint_config = {
            "uuid": str(uuid.uuid4()),
            "name": "Test HL7v2 Endpoint",
            "mode": "source",
            "message_type": "HL7v2",
            "endpoint_type": "mllp",
            "organization_uuid": g.organization['uuid'],
            "host": current_app.config['TEST_HL7_SERVICE_HOST'],
            "port": result['port'],
            "is_test": True,
            "service_url": current_app.config['TEST_HL7_SERVICE_URL'],
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "status": EndpointStatus.INACTIVE,
            "metadata": {
                "created_by": g.user['email'],
                "test_service": "HL7v2"
            }
        }

        # Validate configuration
        endpoint_instance = _get_endpoint_instance(endpoint_config)
        if not endpoint_instance:
            raise Exception("Failed to create endpoint instance")

        # Store configuration
        result = mongo.db.endpoints.insert_one(endpoint_config)
        if result.acknowledged:
            flash('Test HL7v2 endpoint created successfully', 'success')
            return redirect(url_for('endpoints.endpoint_detail',
                                  uuid=endpoint_config['uuid']))
        else:
            flash('Failed to create test endpoint', 'error')
            return redirect(url_for('endpoints.view_endpoints'))

    except Exception as e:
        logger.error(f"Error creating test HL7v2 endpoint: {str(e)}")
        flash('Error creating test endpoint', 'error')
        return redirect(url_for('endpoints.view_endpoints'))

def _create_test_fhir_endpoint(server_type):
    """Create test FHIR endpoint"""
    try:
        server_config = current_app.config['FHIR_SERVERS'].get(server_type)
        if not server_config:
            flash('Invalid FHIR server type', 'error')
            return redirect(url_for('endpoints.view_endpoints'))

        # Check for existing FHIR test endpoint
        existing = mongo.db.endpoints.find_one({
            "organization_uuid": g.organization['uuid'],
            "is_test": True,
            "message_type": "FHIR",
            "server_type": server_type,
            "deleted": {"$ne": True}
        })
        
        if existing:
            flash(f'A test endpoint for {server_type} already exists', 'error')
            return redirect(url_for('endpoints.view_endpoints'))

        # Create endpoint configuration
        endpoint_config = {
            "uuid": str(uuid.uuid4()),
            "name": f"{server_type.capitalize()} FHIR Test Server",
            "mode": "source",
            "message_type": "FHIR",
            "endpoint_type": "https",
            "organization_uuid": g.organization['uuid'],
            "active": False,
            "is_test": True,
            "status": EndpointStatus.INACTIVE,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "metadata": {
                "created_by": g.user['email'],
                "test_service": "FHIR",
                "server_type": server_type
            }
        }

        # Add HTTPS-specific configuration
        extra_config = {
            "server_type": server_type,
            "base_url": server_config['url'],
            "fhir_version": server_config['version'],
            "verify_ssl": True,
            "auth_token": None,  # Test servers don't require auth
            "custom_headers": {}
        }

        # Create full configuration
        full_config = {**endpoint_config, **extra_config}

        # Create and validate endpoint instance
        try:
            endpoint_instance = _get_endpoint_instance(full_config)
            if not endpoint_instance:
                raise Exception("Failed to create endpoint instance")

            # Test connection
            is_valid, message = endpoint_instance.test_connection()
            if not is_valid:
                raise ValueError(f"Connection test failed: {message}")

            # Store configuration in database
            result = mongo.db.endpoints.insert_one(full_config)
            
            if result.acknowledged:
                flash(f'{server_type.capitalize()} FHIR test endpoint created successfully', 'success')
                return redirect(url_for('endpoints.endpoint_detail',
                                      uuid=endpoint_config['uuid']))
            else:
                flash('Failed to create test endpoint', 'error')
                
        except Exception as e:
            logger.error(f"Error initializing FHIR endpoint: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"Error creating test FHIR endpoint: {str(e)}")
        flash('Error creating test endpoint', 'error')
        
    return redirect(url_for('endpoints.view_endpoints'))

# Endpoint Operations
@bp.route('/<uuid>/start', methods=['POST'])
@login_required
@admin_required
def start_endpoint(uuid):
    try:
        endpoint = mongo.db.endpoints.find_one({
            "uuid": uuid,
            "deleted": {"$ne": True},
            "organization_uuid": g.organization['uuid']
        })

        if not endpoint:
            return jsonify({
                'status': 'error',
                'message': 'Endpoint not found or has been deleted'
            }), 404

        # Update status check
        current_status = endpoint.get('status', EndpointStatus.INACTIVE)
        if current_status == EndpointStatus.ACTIVE:
            return jsonify({
                'status': 'error',
                'message': 'Endpoint is already active'
            }), 400

        # Set starting state
        mongo.db.endpoints.update_one(
            {"uuid": uuid},
            {"$set": {"status": EndpointStatus.STARTING}}
        )

        try:
            # Handle test endpoints
            if endpoint.get('is_test') and endpoint['message_type'] == 'HL7v2':
                from app.services.test_hl7_client import TestHL7Client
                client = TestHL7Client()
                result = client.start_stream(endpoint['organization_uuid'])
                if not result or result.get('status') != 'success':
                    raise Exception('Failed to start test stream service')

            # Create and start endpoint instance
            endpoint_instance = _get_endpoint_instance(endpoint)
            if not endpoint_instance:
                raise Exception('Failed to initialize endpoint')

            if not endpoint_instance.start():
                raise Exception('Failed to start endpoint')

            # Update status
            mongo.db.endpoints.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": EndpointStatus.ACTIVE,
                        "last_active": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                }
            )

            return jsonify({
                'status': 'success',
                'message': 'Endpoint started successfully'
            }), 200

        except Exception as e:
            # Reset state on error
            mongo.db.endpoints.update_one(
                {"uuid": uuid},
                {"$set": {"status": EndpointStatus.ERROR}}
            )
            raise

    except Exception as e:
        logger.error(f"Error starting endpoint {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error starting endpoint: {str(e)}'
        }), 500

@bp.route('/<uuid>/stop', methods=['POST'])
@login_required
@admin_required
def stop_endpoint(uuid):
    """Stop endpoint operations"""
    try:
        endpoint = mongo.db.endpoints.find_one({
            "uuid": uuid,
            "deleted": {"$ne": True},
            "organization_uuid": g.organization['uuid']
        })

        if not endpoint:
            return jsonify({
                'status': 'error',
                'message': 'Endpoint not found or has been deleted'
            }), 404

        if endpoint.get('status') != EndpointStatus.ACTIVE:
            return jsonify({
                'status': 'error',
                'message': 'Endpoint is not active'
            }), 400

        # Set stopping state
        mongo.db.endpoints.update_one(
            {"uuid": uuid},
            {"$set": {"status": EndpointStatus.STOPPING}}
        )

        error_message = None
        try:
            # Handle test endpoints
            if endpoint.get('is_test') and endpoint['message_type'] == 'HL7v2':
                try:
                    from app.services.test_hl7_client import TestHL7Client
                    client = TestHL7Client()
                    result = client.stop_stream(endpoint['organization_uuid'])
                    if not result or result.get('status') != 'success':
                        error_message = "Warning: Failed to stop test service stream"
                        logger.warning(error_message)
                except Exception as e:
                    error_message = f"Warning: Failed to stop test service: {str(e)}"
                    logger.warning(error_message)

            # Stop endpoint instance
            endpoint_instance = _get_endpoint_instance(endpoint)
            if endpoint_instance:
                endpoint_instance.stop()

            # Update status
            mongo.db.endpoints.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "status": EndpointStatus.INACTIVE,
                        "updated_at": datetime.utcnow()
                    }
                }
            )

            if error_message:
                return jsonify({
                    'status': 'warning',
                    'message': f'Endpoint stopped with warnings: {error_message}'
                }), 200

            return jsonify({
                'status': 'success',
                'message': 'Endpoint stopped successfully'
            }), 200

        except Exception as e:
            # Set error state
            mongo.db.endpoints.update_one(
                {"uuid": uuid},
                {"$set": {"status": EndpointStatus.ERROR}}
            )
            raise

    except Exception as e:
        logger.error(f"Error stopping endpoint {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error stopping endpoint: {str(e)}'
        }), 500

@bp.route('/<uuid>/status', methods=['GET'])
@login_required
def get_endpoint_status(uuid):
    """Get endpoint status and metrics"""
    try:
        endpoint = mongo.db.endpoints.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid']
        })

        if not endpoint:
            return jsonify({
                'status': 'error',
                'message': 'Endpoint not found'
            }), 404

        # Get endpoint instance
        endpoint_instance = _get_endpoint_instance(endpoint)
        if not endpoint_instance:
            return jsonify({
                'status': 'error',
                'message': 'Failed to get endpoint status'
            }), 500

        # Get metrics and status
        metrics = endpoint_instance.get_metrics()

        status_data = {
            'uuid': endpoint['uuid'],
            'name': endpoint['name'],
            'status': endpoint.get('status', EndpointStatus.INACTIVE),
            'last_active': endpoint.get('last_active'),
            'metrics': metrics,
            'updated_at': endpoint.get('updated_at')
        }

        # Add test-specific status
        if endpoint.get('is_test'):
            if endpoint['message_type'] == 'HL7v2':
                from app.services.test_hl7_client import TestHL7Client
                client = TestHL7Client()
                try:
                    test_status = client.get_stream_status(endpoint['organization_uuid'])
                    if test_status and test_status.get('status') == 'success':
                        status_data['test_status'] = test_status.get('data', {})
                except Exception as e:
                    logger.error(f"Error fetching test status: {str(e)}")
                    status_data['test_status'] = {'error': 'Unable to fetch test status'}

        return jsonify(status_data), 200

    except Exception as e:
        logger.error(f"Error getting endpoint status {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error getting endpoint status: {str(e)}'
        }), 500

@bp.route('/<uuid>/delete', methods=['POST'])
@login_required
@admin_required
def delete_endpoint(uuid):
    """Delete endpoint"""
    try:
        endpoint = mongo.db.endpoints.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid'],
            "deleted": {"$ne": True}
        })

        if not endpoint:
            return jsonify({
                'status': 'error',
                'message': 'Endpoint not found or already deleted'
            }), 404

        # Handle test endpoints
        if endpoint.get('is_test'):
            if endpoint['message_type'] == 'HL7v2':
                try:
                    from app.services.test_hl7_client import TestHL7Client
                    client = TestHL7Client()
                    result = client.delete_stream(endpoint['organization_uuid'])
                    
                    if not result or result.get('status') != 'success':
                        raise Exception('Failed to delete test stream')
                except Exception as e:
                    logger.error(f"Error deleting test stream {uuid}: {str(e)}")
                    return jsonify({
                        'status': 'error',
                        'message': 'Failed to delete test stream'
                    }), 500

        # Stop endpoint if active
        if endpoint.get('status') == EndpointStatus.ACTIVE:
            try:
                endpoint_instance = _get_endpoint_instance(endpoint)
                if endpoint_instance:
                    endpoint_instance.stop()
            except Exception as e:
                logger.warning(f"Error stopping endpoint during deletion: {str(e)}")

        # Mark endpoint as deleted
        try:
            result = mongo.db.endpoints.update_one(
                {"uuid": uuid},
                {
                    "$set": {
                        "deleted": True,
                        "deleted_at": datetime.utcnow(),
                        "status": EndpointStatus.DELETED,
                        "deleted_by": g.user['email']
                    }
                }
            )

            if result.modified_count > 0:
                return jsonify({
                    'status': 'success',
                    'message': 'Endpoint deleted successfully'
                }), 200
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'Failed to delete endpoint'
                }), 500

        except PyMongoError as e:
            logger.error(f"Database error deleting endpoint {uuid}: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Database error occurred while deleting endpoint'
            }), 500

    except Exception as e:
        logger.error(f"Error deleting endpoint {uuid}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'Error deleting endpoint: {str(e)}'
        }), 500

@bp.route('/ajax/endpoint_types/<mode>/<message_type>')
@login_required
def get_endpoint_types(mode, message_type):
    """Get valid endpoint types for mode and message type"""
    try:
        endpoint_types = []
        endpoint_mappings = current_app.config['STREAMSV2_ENDPOINT_MAPPINGS']
        
        if mode == 'source':
            # Get valid source endpoint types from mapping
            source_mappings = endpoint_mappings['source']
            for endpoint_type, config in source_mappings.items():
                if message_type in config['supported_types']:
                    # Format the label based on endpoint type
                    if endpoint_type == 'mllp':
                        endpoint_types.append({'value': 'mllp', 'label': 'MLLP'})
                    elif endpoint_type == 'sftp':
                        endpoint_types.append({'value': 'sftp', 'label': 'SFTP'})
                    elif endpoint_type == 'https':
                        endpoint_types.append({'value': 'https', 'label': 'HTTPS'})
                        
        elif mode == 'destination':
            # Get valid destination endpoint types from mapping
            dest_mappings = endpoint_mappings['destination']
            for endpoint_type, config in dest_mappings.items():
                if message_type in config['supported_types']:
                    if endpoint_type == 'aws_s3':
                        endpoint_types.append({'value': 'aws_s3', 'label': 'AWS S3'})
                    elif endpoint_type == 'gcp_storage':
                        endpoint_types.append({'value': 'gcp_storage', 'label': 'Google Cloud Storage'})

        # Log the results for debugging
        current_app.logger.debug(
            f"Endpoint types for {mode}/{message_type}: {endpoint_types}"
        )

        return jsonify({
            'status': 'success',
            'endpoint_types': endpoint_types
        })

    except Exception as e:
        current_app.logger.error(f"Error getting endpoint types: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'Error retrieving endpoint types'
        }), 500

@bp.route('/<uuid>/detail')
@login_required
def endpoint_detail(uuid):
    """Show endpoint details"""
    try:
        endpoint = mongo.db.endpoints.find_one({
            "uuid": uuid,
            "organization_uuid": g.organization['uuid']
        })

        if not endpoint:
            flash('Endpoint not found', 'error')
            return redirect(url_for('endpoints.view_endpoints'))

        # Get endpoint instance for metrics
        endpoint_instance = _get_endpoint_instance(endpoint)
        metrics = endpoint_instance.get_metrics() if endpoint_instance else {
            'messages_received': 0,
            'messages_sent': 0,
            'errors': 0
        }

        return render_template(
            'endpoint_detail.html',
            endpoint=endpoint,
            metrics=metrics
        )

    except Exception as e:
        current_app.logger.error(f"Error getting endpoint details: {str(e)}")
        flash('Error retrieving endpoint details', 'error')
        return redirect(url_for('endpoints.view_endpoints'))

@bp.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        # Check MongoDB connection
        mongo.db.command('ping')
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected'
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
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

# Context processors
@bp.context_processor
def utility_processor():
    """Add utility functions to template context"""
    def format_endpoint_status(status):
        """Format endpoint status for display"""
        status_map = {
            EndpointStatus.ACTIVE: 'Active',
            EndpointStatus.INACTIVE: 'Inactive',
            EndpointStatus.STARTING: 'Starting',
            EndpointStatus.STOPPING: 'Stopping',
            EndpointStatus.ERROR: 'Error',
            EndpointStatus.DELETED: 'Deleted'
        }
        return status_map.get(status, 'Unknown')

    return dict(
        format_endpoint_status=format_endpoint_status
    )