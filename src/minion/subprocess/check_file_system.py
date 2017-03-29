import os

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class CheckFileSystemCommand(BaseCommand):

    COMMAND = 'check_file_system'
    REQUIRED_PARAMS = ('backend_path',)

    def execute(self):
        path = self.params['backend_path']
        try:
            os.listdir(path)
        except Exception as e:
            cmd_logger.error('Failed to check path: {}'.format(e), extra=self.log_extra)
            raise
        cmd_logger.info('Successfully check path: {}'.format(path), extra=self.log_extra)
