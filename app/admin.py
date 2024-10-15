from flask_admin import Admin
from flask_admin.contrib.pymongo import ModelView
from wtforms import Form, StringField, IntegerField, BooleanField, DateTimeField, TextAreaField, FloatField
from wtforms.validators import Optional
from flask_admin.contrib.pymongo.filters import FilterEqual, FilterLike
import json
from .extensions import mongo

admin = Admin(template_mode='bootstrap3')

class CustomModelView(ModelView):
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True
    
    def __init__(self, collection, endpoint, columns, *args, **kwargs):
        self._column_list = columns
        super(CustomModelView, self).__init__(collection, endpoint=endpoint, *args, **kwargs)

    @property
    def column_list(self):
        return self._column_list

    def scaffold_list_columns(self):
        return self._column_list

    def scaffold_form(self):
        class ModelForm(Form):
            pass
        
        for col in self._column_list:
            if col.endswith('_at'):
                setattr(ModelForm, col, DateTimeField(col, validators=[Optional()]))
            elif col in ['port', 'timeout']:
                setattr(ModelForm, col, IntegerField(col, validators=[Optional()]))
            elif col in ['active', 'deleted', 'parsed']:
                setattr(ModelForm, col, BooleanField(col, validators=[Optional()]))
            else:
                setattr(ModelForm, col, StringField(col, validators=[Optional()]))
        
        return ModelForm

    def get_list(self, page, sort_field, sort_desc, search, filters, page_size=None):
        query = {}
        if search:
            query['$or'] = [{field: {'$regex': search, '$options': 'i'}} for field in self._column_list]

        sort = None
        if sort_field:
            sort = [(sort_field, -1 if sort_desc else 1)]

        total_count = self.coll.count_documents(query)
        page = max(1, page)
        results = self.coll.find(query)
        if sort:
            results = results.sort(sort)
        results = results.skip((page - 1) * self.page_size).limit(self.page_size)

        return total_count, results

    def create_model(self, form):
        try:
            model = form.data
            self.coll.insert_one(model)
        except Exception as ex:
            self.handle_view_exception(ex)
            return False
        return True

    def update_model(self, form, model):
        try:
            model.update(form.data)
            self.coll.replace_one({'_id': model['_id']}, model)
        except Exception as ex:
            self.handle_view_exception(ex)
            return False
        return True

    def delete_model(self, model):
        try:
            self.coll.delete_one({'_id': model['_id']})
        except Exception as ex:
            self.handle_view_exception(ex)
            return False
        return True

class FHIRMessageView(CustomModelView):
    def __init__(self, collection, endpoint, *args, **kwargs):
        columns = ['original_message_uuid', 'fhir_content', 'conversion_metadata.conversion_time']
        super(FHIRMessageView, self).__init__(collection, endpoint, columns, *args, **kwargs)

    column_labels = {
        'original_message_uuid': 'Original UUID',
        'fhir_content': 'FHIR Content',
        'conversion_metadata.conversion_time': 'Conversion Time'
    }
    column_filters = [
        FilterEqual(column='original_message_uuid', name='Original UUID'),
        FilterLike(column='fhir_content.resourceType', name='Resource Type'),
    ]

    def _format_fhir_content(self, context, model, name):
        fhir_content = model.get('fhir_content', {})
        return json.dumps(fhir_content, indent=2)

    def scaffold_form(self):
        class FHIRForm(Form):
            original_message_uuid = StringField('Original UUID')
            fhir_content = TextAreaField('FHIR Content')
            conversion_time = FloatField('Conversion Time')

        return FHIRForm

    def on_model_change(self, form, model, is_created):
        if 'fhir_content' in form.data and isinstance(form.data['fhir_content'], str):
            try:
                model['fhir_content'] = json.loads(form.data['fhir_content'])
            except json.JSONDecodeError:
                raise ValueError('Invalid JSON in FHIR Content')

        if 'conversion_time' in form.data:
            if 'conversion_metadata' not in model:
                model['conversion_metadata'] = {}
            model['conversion_metadata']['conversion_time'] = form.data['conversion_time']


def init_admin(app):
    if not admin.app:  # Only initialize if it hasn't been done already
        admin.init_app(app)

        # Add views
        admin.add_view(CustomModelView(
            mongo.db.streams, 
            endpoint='streams_view',
            name='Streams',
            columns=['uuid', 'name', 'message_type', 'host', 'port', 'timeout', 'active', 'created_at', 'updated_at', 'last_active', 'deleted']
        ))
        admin.add_view(CustomModelView(
            mongo.db.messages, 
            endpoint='messages_view',
            name='Messages',
            columns=['uuid', 'stream_uuid', 'message', 'parsed', 'timestamp', 'type', 'conversion_status']
        ))
        admin.add_view(CustomModelView(
            mongo.db.logs, 
            endpoint='logs_view',
            name='Logs',
            columns=['stream_uuid', 'level', 'message', 'timestamp']
        ))
        admin.add_view(CustomModelView(
            mongo.db.parsing_logs, 
            endpoint='parsing_logs_view',
            name='Parsing Logs',
            columns=['message_id', 'status', 'timestamp']
        ))
        admin.add_view(CustomModelView(
            mongo.db.validation_logs, 
            endpoint='validation_logs_view',
            name='Validation Logs',
            columns=['message_id', 'is_valid', 'timestamp']
        ))
        admin.add_view(FHIRMessageView(
            mongo.db.fhir_messages, 
            endpoint='fhir_messages_view',
            name='FHIR Messages'
        ))

    return admin