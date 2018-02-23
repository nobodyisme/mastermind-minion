import os

from tornado import gen

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class RemoveBackendStopFileCommand(BaseCommand):

    COMMAND = 'remove_backend_stop_file'
    # TODO: rename to stop_file
    REQUIRED_PARAMS = ('remove_stop_file',)

    @gen.coroutine
    def execute(self):
        stop_file = self.params['remove_stop_file']
        try:
            os.remove(stop_file)
        except Exception as e:
            cmd_logger.error('Failed to remove backend stop marker: {}'.format(e), extra=self.log_extra)
            raise
        cmd_logger.info('Successfully removed backend stop marker: {}'.format(stop_file), extra=self.log_extra)
