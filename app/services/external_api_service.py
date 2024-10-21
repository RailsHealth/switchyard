import requests
from flask import current_app
from app.utils.logging_utils import log_message_cycle
import time

def get_endpoint_config(message_type):
    flag_key = f"{message_type.upper()}_DUAL_ENDPOINTSWITCH"
    flag_value = current_app.config.get(flag_key, "DUAL").upper()
    
    new_endpoint_key = f"{message_type.upper()}_NEW_API_URL"
    current_endpoint_key = f"{message_type.upper()}_CURRENT_API_URL"
    
    new_endpoint = current_app.config.get(new_endpoint_key)
    current_endpoint = current_app.config.get(current_endpoint_key)
    
    if flag_value in ["1", "DUAL"]:
        return new_endpoint, current_endpoint
    elif flag_value in ["2", "DET"]:
        return new_endpoint, None
    elif flag_value in ["3", "PROB"]:
        return None, current_endpoint
    else:
        raise ValueError(f"Invalid flag value for {message_type}: {flag_value}")

def send_conversion_request(url, payload, attempt, endpoint_type):
    start_time = time.time()
    try:
        response = requests.post(url, json=payload, timeout=30)
        response_time = time.time() - start_time
        
        log_message_cycle(
            payload['UUID'],
            payload.get('organization_uuid'),
            "api_call",
            payload['OriginalDataType'],
            {
                "endpoint_type": endpoint_type,
                "attempt": attempt,
                "response_time": response_time,
                "status_code": response.status_code
            },
            "success" if response.status_code == 200 else "error"
        )
        
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        response_time = time.time() - start_time
        log_message_cycle(
            payload['UUID'],
            payload.get('organization_uuid'),
            "api_call_error",
            payload['OriginalDataType'],
            {
                "endpoint_type": endpoint_type,
                "attempt": attempt,
                "response_time": response_time,
                "error": str(e)
            },
            "error"
        )
        raise

def convert_message(message, endpoint_url, max_retries, endpoint_type):
    for attempt in range(max_retries):
        try:
            response = send_conversion_request(endpoint_url, message, attempt + 1, endpoint_type)
            return response.json()
        except Exception as e:
            log_message_cycle(
                message['UUID'],
                message.get('organization_uuid'),
                "conversion_error",
                message['OriginalDataType'],
                {
                    "endpoint_type": endpoint_type,
                    "attempt": attempt + 1,
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                "error"
            )
            if attempt == max_retries - 1:
                raise

def validate_api_response(response_data):
    required_keys = ['UUID', 'CreationTimestamp', 'ConversionTimestamp', 'OriginalDataType', 'MessageBody', 'Isvalidated']
    missing_keys = [key for key in required_keys if key not in response_data]
    if missing_keys:
        current_app.logger.error(f"API response is missing required fields: {', '.join(missing_keys)}")
        return False
    return True