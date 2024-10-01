import logging

class BaseInterface:
    def __init__(self, stream_uuid):
        self.stream_uuid = stream_uuid
        self.logger = logging.getLogger(f'{self.__class__.__name__}-{stream_uuid}')

    def _store_log(self, level, message):
        # This method should be implemented in the derived classes
        pass