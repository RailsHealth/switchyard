"""
StreamsV2 Package
Enhanced stream processing implementation with improved workflow and monitoring.
"""

from typing import Dict, Any

# Version info
__version__ = '2.0.0'
__author__ = 'Rails Health'
__description__ = 'Enhanced stream processing implementation'

# Initialize configuration
DEFAULT_CONFIG: Dict[str, Any] = {
    'STREAMSV2_BATCH_SIZE': 100,
    'STREAMSV2_PROCESSING_INTERVAL': 60,
    'STREAMSV2_MAX_RETRIES': 3,
    'STREAMSV2_RETRY_DELAY': 5
}

def init_app(app):
    """Initialize StreamsV2 with app configuration"""
    # Merge default config with app config
    for key, value in DEFAULT_CONFIG.items():
        app.config.setdefault(key, value)

    # Import and register blueprint here to avoid circular imports
    from app.routes.streamsv2 import bp
    app.register_blueprint(bp)