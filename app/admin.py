# app/admin.py

from flask import request, abort, url_for, redirect, flash, render_template, g, session
from flask_admin import Admin, AdminIndexView, expose
from flask_admin.contrib.pymongo import ModelView
from flask_admin.contrib.pymongo.filters import FilterEqual, FilterNotEqual, FilterLike, FilterGreater, FilterSmaller
from wtforms import Form, StringField, IntegerField, BooleanField, DateTimeField, TextAreaField, FloatField, SelectField
from wtforms.validators import Optional, DataRequired, Email, Length
from .extensions import mongo
import json
from datetime import datetime
from bson import ObjectId
import os

# Custom Filters
class BooleanEqualFilter(FilterEqual):
    def apply(self, query, value):
        if value == '1':
            query[self.column] = True
        else:
            query[self.column] = False
        return query

class SecureAdminIndexView(AdminIndexView):
    @expose('/')
    def index(self):
        if not self.is_accessible():
            return self._handle_view(None)
        
        try:
            # Gather statistics with error handling
            stats = {
                'streams': mongo.db.streams.count_documents({'deleted': False}),
                'messages': mongo.db.messages.count_documents({}),
                'fhir': mongo.db.fhir_messages.count_documents({}),
                'users': mongo.db.users.count_documents({})
            }
        except Exception as e:
            current_app.logger.error(f"Error gathering admin statistics: {str(e)}")
            stats = {
                'streams': 0,
                'messages': 0,
                'fhir': 0,
                'users': 0
            }
        
        # Check system status
        try:
            mongo_status = mongo.db.command('ping')['ok'] == 1.0
        except Exception:
            mongo_status = False
            current_app.logger.error("MongoDB connection check failed")
            
        try:
            from redis import Redis
            redis = Redis.from_url(current_app.config['REDIS_URL'])
            redis_status = redis.ping()
        except Exception:
            redis_status = False
            current_app.logger.error("Redis connection check failed")
            
        try:
            from celery.task.control import inspect
            insp = inspect()
            workers = insp.active()
            celery_status = workers is not None
        except Exception:
            celery_status = False
            current_app.logger.error("Celery status check failed")

        # Log access
        if hasattr(g, 'user') and g.user:
            current_app.logger.info(f"Admin dashboard accessed by {g.user.get('email')}")
        
        return self.render('admin/custom_index.html',
                          stats=stats,
                          mongo_status=mongo_status,
                          redis_status=redis_status,
                          celery_status=celery_status)

    def is_accessible(self):
        """Check if current user has admin access"""
        try:
            # Basic user check
            if not hasattr(g, 'user') or not g.user:
                current_app.logger.debug("Admin access denied: No user in context")
                return False

            # Get user email
            user_email = g.user.get('email')
            if not user_email:
                current_app.logger.debug("Admin access denied: No email in user data")
                return False

            # Check admin access
            admin_emails = current_app.config.get('ADMIN_PANEL_EMAILS', [])
            has_access = user_email in admin_emails
            
            # Log access attempt
            if not has_access:
                current_app.logger.warning(f"Admin access denied for user: {user_email}")
            else:
                current_app.logger.debug(f"Admin access granted for user: {user_email}")
                
            return has_access

        except Exception as e:
            current_app.logger.error(f"Error checking admin access: {str(e)}")
            return False

    def _handle_view(self, name, **kwargs):
        """Handle unauthorized access attempts"""
        if not self.is_accessible():
            # Store the requested URL for post-login redirect
            next_url = request.url
            session['next_url'] = next_url
            current_app.logger.info(f"Redirecting unauthorized admin access to login. Requested URL: {next_url}")
            
            # Redirect to login
            return redirect(url_for('auth.google_login'))
            
        return super(SecureAdminIndexView, self)._handle_view(name, **kwargs)

    def handle_view_exception(self, exc):
        """Handle exceptions in admin views"""
        if isinstance(exc, Exception):
            current_app.logger.error(f"Admin view error: {str(exc)}")
            flash(str(exc), 'error')
            return True
        return super().handle_view_exception(exc)

class SecureModelView(ModelView):
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50

    def is_accessible(self):
        """Check if current user has admin access"""
        try:
            # Basic user check
            if not hasattr(g, 'user') or not g.user:
                current_app.logger.debug(f"Model view access denied: No user in context. View: {self.name}")
                return False

            # Get user email
            user_email = g.user.get('email')
            if not user_email:
                current_app.logger.debug(f"Model view access denied: No email in user data. View: {self.name}")
                return False

            # Check admin access
            admin_emails = current_app.config.get('ADMIN_PANEL_EMAILS', [])
            has_access = user_email in admin_emails
            
            # Log access attempt
            if not has_access:
                current_app.logger.warning(f"Model view access denied for user: {user_email}. View: {self.name}")
            else:
                current_app.logger.debug(f"Model view access granted for user: {user_email}. View: {self.name}")
                
            return has_access

        except Exception as e:
            current_app.logger.error(f"Error checking model view access: {str(e)}")
            return False

    def _handle_view(self, name, **kwargs):
        """Handle unauthorized access attempts"""
        if not self.is_accessible():
            # Store the requested URL for post-login redirect
            next_url = request.url
            session['next_url'] = next_url
            current_app.logger.info(f"Redirecting unauthorized model view access to login. View: {self.name}, URL: {next_url}")
            
            # Redirect to login
            return redirect(url_for('auth.google_login'))
            
        return super(SecureModelView, self)._handle_view(name, **kwargs)

    def after_model_change(self, form, model, is_created):
        """Log model changes"""
        try:
            action = 'created' if is_created else 'edited'
            flash(f'Successfully {action} record.', 'success')
            
            # Log the change
            log_entry = {
                'user': g.user.get('email'),
                'action': action,
                'model': self.name,
                'record_id': str(model.get('_id')),
                'timestamp': datetime.utcnow(),
                'changes': {k: v for k, v in model.items() if k != '_id'}  # Store changed data
            }
            
            mongo.db.admin_logs.insert_one(log_entry)
            current_app.logger.info(f"Admin action logged: {action} on {self.name} by {g.user.get('email')}")

        except Exception as e:
            current_app.logger.error(f"Error logging model change: {str(e)}")
            flash('Action completed but logging failed.', 'warning')

    def after_model_delete(self, model):
        """Log model deletions"""
        try:
            flash('Successfully deleted record.', 'success')
            
            # Log the deletion
            log_entry = {
                'user': g.user.get('email'),
                'action': 'deleted',
                'model': self.name,
                'record_id': str(model.get('_id')),
                'timestamp': datetime.utcnow(),
                'deleted_data': {k: v for k, v in model.items() if k != '_id'}  # Store deleted data
            }
            
            mongo.db.admin_logs.insert_one(log_entry)
            current_app.logger.info(f"Admin deletion logged: {self.name} by {g.user.get('email')}")

        except Exception as e:
            current_app.logger.error(f"Error logging model deletion: {str(e)}")
            flash('Deletion completed but logging failed.', 'warning')

    def handle_view_exception(self, exc):
        """Handle exceptions in model views"""
        if isinstance(exc, Exception):
            current_app.logger.error(f"Model view error in {self.name}: {str(exc)}")
            flash(str(exc), 'error')
            return True
        return super().handle_view_exception(exc)

class StreamView(SecureModelView):
    column_list = ['uuid', 'name', 'message_type', 'host', 'port', 'active', 'created_at']
    column_details_list = column_list
    column_searchable_list = ['name', 'message_type', 'uuid']
    column_filters = [
        BooleanEqualFilter('active', 'Active Status'),
        FilterEqual('message_type', 'Message Type'),
        FilterEqual('organization_uuid', 'Organization')
    ]
    form_excluded_columns = ['created_at', 'updated_at']
    
    def scaffold_form(self):
        class StreamForm(Form):
            name = StringField('Name', validators=[DataRequired()])
            message_type = SelectField('Message Type', choices=[
                ('HL7v2', 'HL7v2'),
                ('FHIR', 'FHIR'),
                ('CCDA', 'CCDA'),
                ('X12', 'X12'),
                ('Clinical Notes', 'Clinical Notes')
            ])
            host = StringField('Host')
            port = IntegerField('Port', validators=[Optional()])
            active = BooleanField('Active', default=True)
            deleted = BooleanField('Deleted', default=False)
            
            def validate(self):
                if not super(StreamForm, self).validate():
                    return False
                if self.port.data and (self.port.data < 1 or self.port.data > 65535):
                    self.port.errors.append('Port must be between 1 and 65535')
                    return False
                return True
                
        return StreamForm

    def on_model_change(self, form, model, is_created):
        if is_created:
            model['created_at'] = datetime.utcnow()
            model['uuid'] = str(uuid.uuid4())
        model['updated_at'] = datetime.utcnow()
        return super().on_model_change(form, model, is_created)

class MessageView(SecureModelView):
    column_list = ['uuid', 'stream_uuid', 'type', 'conversion_status', 'timestamp', 'organization_uuid']
    column_details_list = column_list
    column_searchable_list = ['uuid', 'stream_uuid']
    column_filters = [
        FilterEqual('type', 'Message Type'),
        FilterEqual('conversion_status', 'Conversion Status'),
        FilterEqual('organization_uuid', 'Organization'),
        FilterGreater('timestamp', 'After Time'),
        FilterSmaller('timestamp', 'Before Time')
    ]
    can_create = False

    def scaffold_form(self):
        class MessageForm(Form):
            uuid = StringField('UUID', render_kw={'readonly': True})
            stream_uuid = StringField('Stream UUID', render_kw={'readonly': True})
            type = StringField('Type', render_kw={'readonly': True})
            conversion_status = StringField('Status', render_kw={'readonly': True})
            timestamp = DateTimeField('Timestamp', render_kw={'readonly': True})
            organization_uuid = StringField('Organization', render_kw={'readonly': True})
        return MessageForm

    def get_query(self):
        return super(MessageView, self).get_query().sort([('timestamp', -1)])

class FHIRMessageView(SecureModelView):
    column_list = [
        'original_message_uuid',
        'conversion_metadata.conversion_time',
        'fhir_validation_status',
        'organization_uuid'
    ]
    column_details_list = column_list
    column_filters = [
        FilterEqual('fhir_validation_status', 'Validation Status'),
        FilterEqual('organization_uuid', 'Organization')
    ]
    can_create = False

    def scaffold_form(self):
        class FHIRMessageForm(Form):
            original_message_uuid = StringField('Original Message UUID', render_kw={'readonly': True})
            conversion_time = FloatField('Conversion Time', render_kw={'readonly': True})
            fhir_validation_status = StringField('Validation Status', render_kw={'readonly': True})
            organization_uuid = StringField('Organization', render_kw={'readonly': True})
            fhir_content = TextAreaField('FHIR Content', render_kw={'readonly': True})
        return FHIRMessageForm

    def _format_fhir_content(self, view, context, model, name):
        try:
            return json.dumps(model.get('fhir_content', {}), indent=2)
        except Exception as e:
            return str(model.get('fhir_content', '{}'))

class LogView(SecureModelView):
    column_list = ['stream_uuid', 'level', 'message', 'timestamp', 'organization_uuid']
    column_details_list = column_list
    column_filters = [
        FilterEqual('level', 'Log Level'),
        FilterEqual('stream_uuid', 'Stream'),
        FilterEqual('organization_uuid', 'Organization'),
        FilterGreater('timestamp', 'After'),
        FilterSmaller('timestamp', 'Before')
    ]
    can_create = False
    can_edit = False
    can_delete = False

    def scaffold_form(self):
        class LogForm(Form):
            stream_uuid = StringField('Stream UUID', render_kw={'readonly': True})
            level = StringField('Level', render_kw={'readonly': True})
            message = TextAreaField('Message', render_kw={'readonly': True})
            timestamp = DateTimeField('Timestamp', render_kw={'readonly': True})
            organization_uuid = StringField('Organization', render_kw={'readonly': True})
        return LogForm

class ParsingLogView(SecureModelView):
    column_list = ['message_id', 'status', 'timestamp', 'organization_uuid']
    column_details_list = column_list
    column_filters = [
        FilterEqual('status', 'Status'),
        FilterEqual('message_id', 'Message ID'),
        FilterEqual('organization_uuid', 'Organization'),
        FilterGreater('timestamp', 'After'),
        FilterSmaller('timestamp', 'Before')
    ]
    can_create = False
    can_edit = False

    def scaffold_form(self):
        class ParsingLogForm(Form):
            message_id = StringField('Message ID', render_kw={'readonly': True})
            status = StringField('Status', render_kw={'readonly': True})
            timestamp = DateTimeField('Timestamp', render_kw={'readonly': True})
            organization_uuid = StringField('Organization', render_kw={'readonly': True})
        return ParsingLogForm

class ValidationLogView(SecureModelView):
    column_list = ['message_id', 'is_valid', 'error', 'timestamp', 'organization_uuid']
    column_details_list = column_list
    column_filters = [
        BooleanEqualFilter('is_valid', 'Is Valid'),
        FilterEqual('message_id', 'Message ID'),
        FilterEqual('organization_uuid', 'Organization')
    ]
    can_create = False
    can_edit = False
    can_delete = False

    def scaffold_form(self):
        class ValidationLogForm(Form):
            message_id = StringField('Message ID', render_kw={'readonly': True})
            is_valid = BooleanField('Is Valid', render_kw={'readonly': True})
            error = TextAreaField('Error', render_kw={'readonly': True})
            timestamp = DateTimeField('Timestamp', render_kw={'readonly': True})
            organization_uuid = StringField('Organization', render_kw={'readonly': True})
        return ValidationLogForm

class AdminLogView(SecureModelView):
    column_list = ['user', 'action', 'model', 'record_id', 'timestamp']
    column_details_list = column_list
    column_filters = [
        FilterEqual('user', 'User'),
        FilterEqual('action', 'Action'),
        FilterEqual('model', 'Model'),
        FilterGreater('timestamp', 'After'),
        FilterSmaller('timestamp', 'Before')
    ]
    can_create = False
    can_edit = False
    can_delete = False

    def scaffold_form(self):
        class AdminLogForm(Form):
            user = StringField('User', render_kw={'readonly': True})
            action = StringField('Action', render_kw={'readonly': True})
            model = StringField('Model', render_kw={'readonly': True})
            record_id = StringField('Record ID', render_kw={'readonly': True})
            timestamp = DateTimeField('Timestamp', render_kw={'readonly': True})
        return AdminLogForm

class UserView(SecureModelView):
    column_list = ['email', 'name', 'roles', 'organizations', 'created_at', 'last_login']
    column_details_list = column_list
    column_searchable_list = ['email', 'name']
    column_filters = [
        FilterEqual('roles', 'Role'),
        FilterEqual('organizations', 'Organization')
    ]

    def scaffold_form(self):
        class UserForm(Form):
            email = StringField('Email', validators=[DataRequired(), Email()])
            name = StringField('Name', validators=[DataRequired(), Length(min=2)])
            roles = SelectField('Role', choices=[
                ('admin', 'Admin'),
                ('viewer', 'Viewer')
            ])
            organizations = SelectField('Organization')
            created_at = DateTimeField('Created At', render_kw={'readonly': True})
            last_login = DateTimeField('Last Login', render_kw={'readonly': True})

            def __init__(self, *args, **kwargs):
                super(UserForm, self).__init__(*args, **kwargs)
                orgs = list(mongo.db.organizations.find({'active': True}, {'name': 1}))
                self.organizations.choices = [(str(org['_id']), org['name']) for org in orgs]

        return UserForm

    def on_model_change(self, form, model, is_created):
        if is_created:
            model['created_at'] = datetime.utcnow()
            model['last_login'] = None
        return super().on_model_change(form, model, is_created)

class OrganizationView(SecureModelView):
    column_list = ['name', 'type', 'created_at', 'active', 'description']
    column_details_list = column_list
    column_searchable_list = ['name']
    column_filters = [
        FilterEqual('type', 'Organization Type'),
        BooleanEqualFilter('active', 'Active Status')
    ]

    def scaffold_form(self):
        class OrgForm(Form):
            name = StringField('Name', validators=[DataRequired()])
            type = SelectField('Type', choices=[
                ('Healthcare Provider', 'Healthcare Provider'),
                ('Payer', 'Payer'),
                ('Health IT Vendor', 'Health IT Vendor'),
                ('Research Institution', 'Research Institution'),
                ('Government Agency', 'Government Agency'),
                ('Other', 'Other')
            ])
            active = BooleanField('Active', default=True)
            description = TextAreaField('Description')
            created_at = DateTimeField('Created At', render_kw={'readonly': True})

        return OrgForm

    def on_model_change(self, form, model, is_created):
        if is_created:
            model['created_at'] = datetime.utcnow()
        return super().on_model_change(form, model, is_created)

def init_admin(app):
    """Initialize Flask-Admin with OAuth authentication"""
    # Create admin instance
    admin = Admin(
        app,
        name='Rails Health Admin',
        template_mode='bootstrap3',
        index_view=SecureAdminIndexView(
            name='Dashboard',
            template='admin/custom_index.html',
            url='/admin'
        )
    )

    # Ensure admin template directory exists
    admin_template_dir = os.path.join(app.template_folder, 'admin')
    if not os.path.exists(admin_template_dir):
        os.makedirs(admin_template_dir)

    # Create custom index template if it doesn't exist
    index_template = os.path.join(admin_template_dir, 'custom_index.html')
    if not os.path.exists(index_template):
        with open(index_template, 'w') as f:
            f.write('''
{% extends 'admin/master.html' %}
{% block body %}
<div class="container">
    <h1>Rails Health Administration</h1>
    <div class="row">
        <div class="col-sm-6">
            <div class="panel panel-primary">
                <div class="panel-heading">
                    <h3 class="panel-title">Quick Stats</h3>
                </div>
                <div class="panel-body">
                    <ul class="list-group">
                        <li class="list-group-item">
                            <span class="badge">{{ stats.streams }}</span>
                            Active Streams
                        </li>
                        <li class="list-group-item">
                            <span class="badge">{{ stats.messages }}</span>
                            Total Messages
                        </li>
                        <li class="list-group-item">
                            <span class="badge">{{ stats.fhir }}</span>
                            FHIR Messages
                        </li>
                    </ul>
                </div>
            </div>
        </div>
        <div class="col-sm-6">
            <div class="panel panel-info">
                <div class="panel-heading">
                    <h3 class="panel-title">System Status</h3>
                </div>
                <div class="panel-body">
                    <ul class="list-group">
                        <li class="list-group-item">
                            <span class="label label-{{ 'success' if celery_status else 'danger' }} pull-right">
                                {{ 'Running' if celery_status else 'Stopped' }}
                            </span>
                            Celery Workers
                        </li>
                        <li class="list-group-item">
                            <span class="label label-{{ 'success' if redis_status else 'danger' }} pull-right">
                                {{ 'Connected' if redis_status else 'Disconnected' }}
                            </span>
                            Redis Connection
                        </li>
                        <li class="list-group-item">
                            <span class="label label-{{ 'success' if mongo_status else 'danger' }} pull-right">
                                {{ 'Connected' if mongo_status else 'Disconnected' }}
                            </span>
                            MongoDB Connection
                        </li>
                    </ul>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
''')

    # Add views
    admin.add_view(StreamView(
        mongo.db.streams,
        'Streams',
        endpoint='admin_streams',
        category='Data Management'
    ))

    admin.add_view(MessageView(
        mongo.db.messages,
        'Messages',
        endpoint='admin_messages',
        category='Data Management'
    ))

    admin.add_view(FHIRMessageView(
        mongo.db.fhir_messages,
        'FHIR Messages',
        endpoint='admin_fhir',
        category='Data Management'
    ))

    admin.add_view(LogView(
        mongo.db.logs,
        'System Logs',
        endpoint='admin_logs',
        category='Logs'
    ))

    admin.add_view(ParsingLogView(
        mongo.db.parsing_logs,
        'Parsing Logs',
        endpoint='admin_parsing_logs',
        category='Logs'
    ))

    admin.add_view(ValidationLogView(
        mongo.db.validation_logs,
        'Validation Logs',
        endpoint='admin_validation_logs',
        category='Logs'
    ))

    admin.add_view(AdminLogView(
        mongo.db.admin_logs,
        'Admin Logs',
        endpoint='admin_action_logs',
        category='Logs'
    ))

    admin.add_view(UserView(
        mongo.db.users,
        'Users',
        endpoint='admin_users',
        category='User Management'
    ))

    admin.add_view(OrganizationView(
        mongo.db.organizations,
        'Organizations',
        endpoint='admin_organizations',
        category='User Management'
    ))

    # Initialize indexes
    with app.app_context():
        try:
            # Message indexes
            mongo.db.messages.create_index([('uuid', 1)], unique=True)
            mongo.db.messages.create_index([('stream_uuid', 1)])
            mongo.db.messages.create_index([('organization_uuid', 1)])
            mongo.db.messages.create_index([('timestamp', -1)])
            mongo.db.messages.create_index([('conversion_status', 1)])

            # Stream indexes
            mongo.db.streams.create_index([('uuid', 1)], unique=True)
            mongo.db.streams.create_index([('organization_uuid', 1)])
            mongo.db.streams.create_index([('name', 1)])

            # Other indexes
            mongo.db.fhir_messages.create_index([
                ('original_message_uuid', 1),
                ('organization_uuid', 1)
            ], unique=True)

            # Log indexes
            mongo.db.logs.create_index([('timestamp', -1)])
            mongo.db.logs.create_index([('stream_uuid', 1)])
            mongo.db.logs.create_index([('organization_uuid', 1)])
            
            # User and org indexes
            mongo.db.users.create_index([('email', 1)], unique=True)
            mongo.db.users.create_index([('organizations', 1)])
            mongo.db.organizations.create_index([('name', 1)], unique=True)

            # Validation logs indexes
            mongo.db.validation_logs.create_index([('message_id', 1)])
            mongo.db.validation_logs.create_index([('timestamp', -1)])
            mongo.db.validation_logs.create_index([('organization_uuid', 1)])
            
            # Parsing logs indexes
            mongo.db.parsing_logs.create_index([('message_id', 1)])
            mongo.db.parsing_logs.create_index([('timestamp', -1)])
            mongo.db.parsing_logs.create_index([('organization_uuid', 1)])
            
            # Admin logs indexes
            mongo.db.admin_logs.create_index([('timestamp', -1)])
            mongo.db.admin_logs.create_index([('user', 1)])

            app.logger.info("Successfully initialized all admin indexes")
            
        except Exception as e:
            app.logger.error(f"Error creating indexes: {str(e)}")
            # Continue even if indexes fail - they might already exist

    # Add middleware to handle admin access
    @app.before_request
    def restrict_admin_access():
        if request.path.startswith('/admin'):
            if not hasattr(g, 'user') or not g.user:
                next_url = request.url
                return redirect(url_for('auth.google_login', next=next_url))
            
            if not isinstance(g.user, dict) or g.user.get('email') not in app.config['ADMIN_PANEL_EMAILS']:
                abort(403)

    return admin

# Add missing imports if not already at top of file
import uuid