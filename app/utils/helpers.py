import json
from bson import ObjectId
from datetime import datetime

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

def json_response(data):
    return json.dumps(data, cls=JSONEncoder)

def validate_stream_data(data):
    required_fields = ['name', 'host', 'port', 'timeout']
    for field in required_fields:
        if field not in data:
            return False, f"Missing required field: {field}"
    
    try:
        int(data['port'])
        int(data['timeout'])
    except ValueError:
        return False, "Port and timeout must be integers"

    return True, None

def format_hl7_message(message):
    """
    Format an HL7 message for display or logging.
    This is a simple implementation and might need to be adjusted based on your specific needs.
    """
    segments = message.split('\r')
    formatted = '\n'.join(segments)
    return formatted

def parse_hl7_datetime(hl7_datetime):
    """
    Parse HL7 datetime string into Python datetime object.
    Format: YYYYMMDDHHMMSS
    """
    return datetime.strptime(hl7_datetime, '%Y%m%d%H%M%S')