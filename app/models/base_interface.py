import logging
from app.utils.logging_utils import log_message_cycle

class BaseInterface:
    def __init__(self, stream_uuid, organization_uuid):
        self.stream_uuid = stream_uuid
        self.organization_uuid = organization_uuid
        self.logger = logging.getLogger(f'{self.__class__.__name__}-{stream_uuid}')

    def _store_log(self, level, message):
        # This method should be implemented in the derived classes
        pass

    def log_message_cycle(self, message_uuid, event_type, message_type, details, status):
        log_message_cycle(message_uuid, self.organization_uuid, event_type, message_type, details, status)