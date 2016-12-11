import os

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class RemoveBackendStopFileCommand(BaseCommand):

    COMMAND = 'remove_backend_stop_file'
    # TODO: rename to stop_file
    REQUIRED_PARAMS = ('remove_stop_file',)

    def execute(self):
        stop_file = self.params['remove_stop_file']
        try:
            os.remove(stop_file)
        except Exception as e:
            logger.error('Failed to remove backend stop marker: {}'.format(e))
            raise
        logger.info('Successfully removed backend stop marker: {}'.format(stop_file))
