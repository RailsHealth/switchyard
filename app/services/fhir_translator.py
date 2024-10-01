import yaml
import json
from datetime import datetime, timezone
import re

class FHIRTranslator:
    def __init__(self, mapping):
        if isinstance(mapping, str):
            try:
                with open(mapping, 'r') as file:
                    self.mapping = yaml.safe_load(file)
            except FileNotFoundError:
                print(f"Warning: FHIR mapping file not found at {mapping}. Using empty mapping.")
                self.mapping = {}
        elif isinstance(mapping, dict):
            self.mapping = mapping
        else:
            print("Warning: Invalid mapping provided. Using empty mapping.")
            self.mapping = {}

    def translate(self, fhir_json):
        if isinstance(fhir_json, str):
            try:
                fhir_dict = json.loads(fhir_json)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string provided")
        elif isinstance(fhir_json, dict):
            fhir_dict = fhir_json
        else:
            raise ValueError("Input should be a JSON string or a dictionary")
        
        return self._process_fhir(fhir_dict)

    def _process_fhir(self, fhir_dict):
        bundle_info = self._extract_bundle_info(fhir_dict)
        resources = self._extract_resources(fhir_dict)
        patient_info = next((r for r in resources if r['resourceType'] == 'Patient'), None)
        
        if patient_info:
            resources.remove(patient_info)
        
        sorted_resources = sorted(resources, key=self._get_resource_date, reverse=True)
        
        html = self._generate_bundle_info_html(bundle_info)
        html += self._generate_patient_card_html(patient_info)
        html += self._generate_timeline_html(sorted_resources)
        
        return html

    def _extract_bundle_info(self, fhir_dict):
        if fhir_dict.get('resourceType') == 'Bundle':
            return {
                'id': fhir_dict.get('id'),
                'type': fhir_dict.get('type'),
                'total': fhir_dict.get('total'),
                'link': fhir_dict.get('link'),
                'meta': fhir_dict.get('meta')
            }
        return None

    def _extract_resources(self, json_data):
        resources = []
        if json_data.get('resourceType') == 'Bundle':
            for entry in json_data.get('entry', []):
                resources.extend(self._extract_resources(entry.get('resource', {})))
        elif 'resourceType' in json_data:
            resources.append(json_data)
        return resources

    def _get_resource_date(self, resource):
        date_fields = [
            'date', 'effectiveDateTime', 'recorded', 'period.start',
            'authored', 'authoredOn', 'issued', 'dateWritten', 'created'
        ]
        for field in date_fields:
            value = self._get_nested_value(resource, field)
            if value and self._is_date(value):
                return self._parse_date(value)
        return datetime.min.replace(tzinfo=timezone.utc)

    def _get_nested_value(self, dict_obj, key):
        keys = key.split('.')
        value = dict_obj
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return None
        return value

    def _is_date(self, string):
        date_pattern = r'^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$'
        return bool(re.match(date_pattern, string))

    def _parse_date(self, date_str):
        if not self._is_date(date_str):
            return None
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            try:
                dt = datetime.fromisoformat(date_str)
            except ValueError:
                return None
        
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _generate_bundle_info_html(self, bundle_info):
        if not bundle_info:
            return ''
        
        html = '<div class="bundle-info collapsible">'
        html += '<div class="bundle-header" onclick="toggleCollapsible(this)">'
        html += '<h3>Bundle Information</h3>'
        html += '<span class="toggle-icon">+</span>'
        html += '</div>'
        html += '<div class="bundle-content" style="display:none;">'
        for key, value in bundle_info.items():
            if key != 'meta':
                html += f'<p><strong>{key}:</strong> {value}</p>'
        html += '</div></div>'
        return html

    def _generate_patient_card_html(self, patient_info):
        if not patient_info:
            return '<div class="patient-card">No patient information available</div>'

        name = patient_info.get('name', [{}])[0]
        full_name = f"{name.get('given', [''])[0]} {name.get('family', '')}"
        
        html = f'''
        <div class="patient-card">
            <div class="patient-card-header">
                <span class="patient-name">{full_name}</span>
                <span class="patient-id">ID: {patient_info.get('id', 'N/A')}</span>
            </div>
            <div class="patient-details">
                <div class="patient-detail-item">
                    <span class="patient-detail-label">Gender:</span> {patient_info.get('gender', 'N/A')}
                </div>
                <div class="patient-detail-item">
                    <span class="patient-detail-label">Birth Date:</span> {patient_info.get('birthDate', 'N/A')}
                </div>
                <div class="patient-detail-item">
                    <span class="patient-detail-label">Address:</span> {self._format_address(patient_info.get('address', [{}])[0])}
                </div>
            </div>
        </div>
        '''
        return html

    def _format_address(self, address):
        if not address:
            return 'N/A'
        parts = [
            address.get('line', [''])[0],
            address.get('city', ''),
            address.get('state', ''),
            address.get('postalCode', ''),
            address.get('country', '')
        ]
        return ', '.join(filter(None, parts))

    def _generate_timeline_html(self, resources):
        html = '<div class="timeline">'
        for resource in resources:
            html += self._generate_resource_html(resource)
        html += '</div>'
        return html

    def _generate_resource_html(self, resource):
        resource_type = resource.get('resourceType', 'Unknown')
        created_time = self._get_resource_date(resource)
        formatted_date = self._get_formatted_date(created_time)
        
        html = f'<div class="timeline-item">'
        html += f'<div class="timeline-header" onclick="toggleResource(this)">'
        html += f'<span class="timeline-resource-type">{resource_type}</span>'
        html += f'<span class="timeline-timestamp">{formatted_date}</span>'
        html += '</div>'
        html += '<div class="timeline-content" style="display:none;">'
        html += self._format_resource_content(resource)
        html += '</div></div>'
        return html

    def _format_resource_content(self, resource, depth=0):
        html = '<div class="resource-content">'
        for key, value in resource.items():
            if key in ['resourceType', 'id']:
                continue
            html += self._format_key_value(key, value, depth)
        html += '</div>'
        return html

    def _format_key_value(self, key, value, depth):
        translated_key = self.translate_key(key)
        html = f'<div class="key-value-pair level-{depth}">'
        html += f'<span class="key" title="{key}">{translated_key}:</span> '
        
        if isinstance(value, dict):
            html += self._format_resource_content(value, depth + 1)
        elif isinstance(value, list):
            html += '<ul class="compact-list">'
            for item in value:
                html += '<li>'
                if isinstance(item, dict):
                    html += self._format_resource_content(item, depth + 1)
                else:
                    html += self._format_value(item)
                html += '</li>'
            html += '</ul>'
        else:
            html += self._format_value(value)
        
        html += '</div>'
        return html

    def _format_value(self, value):
        if isinstance(value, str):
            if value.startswith('http'):
                return f'<a href="{value}" target="_blank" class="fhir-link">{self.get_coding_system(value)}</a>'
            elif self._is_date(value):
                parsed_date = self._parse_date(value)
                if parsed_date:
                    return f'<span class="fhir-date">{self._get_formatted_date(parsed_date)}</span>'
                else:
                    return f'<span class="fhir-string">{value}</span>'
            elif '/' in value and any(ref in value for ref in ['Patient', 'Observation', 'Encounter']):
                return f'<a href="#" class="fhir-reference" onclick="showReferenceDetails(\'{value}\')">{value}</a>'
            else:
                return f'<span class="fhir-string">{value}</span>'
        elif isinstance(value, bool):
            return f'<span class="fhir-boolean">{str(value).lower()}</span>'
        elif isinstance(value, (int, float)):
            return f'<span class="fhir-number">{value}</span>'
        else:
            return f'<span class="fhir-value">{value}</span>'

    def _get_formatted_date(self, date):
        if date is None or date == datetime.min.replace(tzinfo=timezone.utc):
            return 'Unknown Date'
        return date.strftime('%d %b %Y, %I:%M:%S %p')

    def translate_key(self, key):
        return self.mapping.get(key, key)

    def get_coding_system(self, url):
        if 'hl7.org' in url:
            return 'HL7'
        elif 'snomed.info' in url:
            return 'SNOMED'
        else:
            return 'FHIR System'