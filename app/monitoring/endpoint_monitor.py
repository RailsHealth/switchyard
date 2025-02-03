import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any
from flask import current_app

logger = logging.getLogger(__name__)

class EndpointMonitor:
    """Database-level monitor for endpoint operations"""

    def __init__(self, mongo_client):
        self.mongo_client = mongo_client
        self.monitor_thread = None
        self._setup_logging()

    def _setup_logging(self):
        """Setup logging for endpoint monitoring"""
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # File handler for permanent record
        file_handler = logging.FileHandler('logs/endpoint_duplicates.log')
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(file_handler)
        
        # Console handler with immediate visibility
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '\033[91m%(asctime)s - %(levelname)s - %(message)s\033[0m'  # Red color for visibility
        ))
        logger.addHandler(console_handler)
        
        logger.setLevel(logging.INFO)

    def check_for_duplicates(self):
        """Check for duplicate endpoints in database"""
        try:
            # Get all non-deleted endpoints
            endpoints = list(self.mongo_client.db.endpoints.find({
                "deleted": {"$ne": True}
            }))

            # Group by UUID
            uuid_groups = {}
            for endpoint in endpoints:
                uuid = endpoint.get('uuid')
                if uuid not in uuid_groups:
                    uuid_groups[uuid] = []
                uuid_groups[uuid].append(endpoint)

            # Check for duplicates
            for uuid, group in uuid_groups.items():
                if len(group) > 1:
                    # Get time range of creations
                    creation_times = sorted([ep.get('created_at') for ep in group])
                    time_span = creation_times[-1] - creation_times[0]
                    
                    logger.warning(
                        f"\n!!! DUPLICATE ENDPOINTS DETECTED !!!"
                        f"\nEndpoint Name: {group[0].get('name')}"
                        f"\nUUID: {uuid}"
                        f"\nCount: {len(group)}"
                        f"\nFirst Creation: {creation_times[0]}"
                        f"\nLast Creation: {creation_times[-1]}"
                        f"\nTime Span: {time_span.total_seconds()} seconds"
                        f"\nCreation Times:\n" + 
                        "\n".join([
                            f"  - {ep.get('created_at')} ({ep.get('_id')})"
                            for ep in sorted(group, key=lambda x: x.get('created_at'))
                        ])
                    )

                    # If creations happened very rapidly (within 5 minutes)
                    if time_span < timedelta(minutes=5):
                        logger.error(
                            f"\n!!! RAPID DUPLICATE CREATION DETECTED !!!"
                            f"\n{len(group)} duplicates of endpoint '{group[0].get('name')}'"
                            f" created within {time_span.total_seconds()} seconds"
                        )

        except Exception as e:
            logger.error(f"Error checking for duplicates: {str(e)}")

    def start_monitoring(self):
        """Start periodic duplicate checking"""
        self.check_for_duplicates()  # Initial check
        logger.info("Started endpoint duplicate monitoring")

    def stop_monitoring(self):
        """Stop monitoring"""
        logger.info("Stopped endpoint duplicate monitoring")