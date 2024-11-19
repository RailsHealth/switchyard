# app/services/test_hl7_client.py

import requests
from flask import current_app
import logging
from typing import Optional, Dict, Any

class TestHL7Client:
    """Client for communicating with Test HL7 Service"""
    
    def __init__(self):
        self.base_url = current_app.config['TEST_HL7_SERVICE_URL'].rstrip('/')
        self.api_key = current_app.config['TEST_HL7_SERVICE_API_KEY']
        self.logger = logging.getLogger(__name__)
        
    def _make_request(self, method: str, endpoint: str, json: Optional[Dict] = None) -> Dict[str, Any]:
        """Make HTTP request to test service"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {'X-API-Key': self.api_key}
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=json,
                timeout=10  # 10 second timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Test HL7 service request failed: {str(e)}")
            raise

    def create_test_stream(self, org_uuid: str) -> Dict[str, Any]:
        """Create a new test stream"""
        return self._make_request(
            'POST',
            '/api/streams',
            json={'organization_uuid': org_uuid}
        )

    def start_stream(self, org_uuid: str) -> Dict[str, Any]:
        """Start sending messages for a stream"""
        return self._make_request(
            'POST',
            f'/api/streams/{org_uuid}/start'
        )

    def stop_stream(self, org_uuid: str) -> Dict[str, Any]:
        """Stop sending messages for a stream"""
        return self._make_request(
            'POST',
            f'/api/streams/{org_uuid}/stop'
        )

    def delete_stream(self, org_uuid: str) -> Dict[str, Any]:
        """Delete a test stream"""
        return self._make_request(
            'DELETE',
            f'/api/streams/{org_uuid}'
        )

    def get_stream_status(self, org_uuid: str) -> Dict[str, Any]:
        """Get current status of a stream"""
        return self._make_request(
            'GET',
            f'/api/streams/{org_uuid}/status'
        )

    def check_health(self) -> bool:
        """Check if the test service is healthy"""
        try:
            response = self._make_request('GET', '/api/health')
            return response.get('status') == 'healthy'
        except Exception:
            return False