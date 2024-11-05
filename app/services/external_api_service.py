from flask import current_app
import requests
from app.utils.logging_utils import log_message_cycle
import time
from tenacity import retry, stop_after_attempt, wait_fixed

def get_endpoint_url(message_type):
    """Get the conversion endpoint URL for a message type"""
    endpoint_key = f"{message_type.upper()}_API_URL"
    endpoint = current_app.config.get(endpoint_key)
    
    if not endpoint:
        raise ValueError(f"No endpoint configured for message type: {message_type}")
    
    return endpoint

@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def send_conversion_request(url, payload):
    """Send conversion request with retry logic"""
    start_time = time.time()
    try:
        response = requests.post(url, json=payload, timeout=30)
        response_time = time.time() - start_time
        
        log_message_cycle(
            payload['UUID'],
            payload.get('organization_uuid'),
            "api_call_attempt",
            payload['OriginalDataType'],
            {
                "response_time": response_time,
                "status_code": response.status_code,
                "url": url,
                "attempt": response.headers.get('X-Retry-Count', '1')
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
                "response_time": response_time,
                "error": str(e),
                "url": url
            },
            "error"
        )
        raise

def convert_message(message, message_type):
    """Convert a message using the configured endpoint"""
    endpoint_url = get_endpoint_url(message_type)
    
    log_message_cycle(
        message['UUID'],
        message.get('organization_uuid'),
        "conversion_attempt_started",
        message_type,
        {"url": endpoint_url},
        "in_progress"
    )
    
    try:
        response = send_conversion_request(endpoint_url, message)
        result = response.json()
        
        log_message_cycle(
            message['UUID'],
            message.get('organization_uuid'),
            "conversion_completed",
            message_type,
            {"url": endpoint_url},
            "success"
        )
        
        return result
    except Exception as e:
        log_message_cycle(
            message['UUID'],
            message.get('organization_uuid'),
            "conversion_failed",
            message_type,
            {
                "error": str(e),
                "error_type": type(e).__name__,
                "url": endpoint_url
            },
            "error"
        )
        raise

def validate_api_response(response_data):
    """Validate the API response structure"""
    required_keys = ['UUID', 'CreationTimestamp', 'ConversionTimestamp', 
                    'OriginalDataType', 'MessageBody', 'Isvalidated']
    
    return all(key in response_data for key in required_keys)