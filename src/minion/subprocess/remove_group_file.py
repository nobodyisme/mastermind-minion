import os

from tornado import gen

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class RemoveGroupFileCommand(BaseCommand):

    COMMAND = 'remove_group_file'
    REQUIRED_PARAMS = ('remove_group_file',)

    @gen.coroutine
    def execute(self):
        cmd_logger.info('Removing group file {0}'.format(self.params['remove_group_file']), extra=self.log_extra)
        path = self.params['remove_group_file']
        try:
            os.remove(path)
        except Exception as e:
            cmd_logger.error('Failed to remove group file: {}'.format(e), extra=self.log_extra)
            marker = self.params.get('stop_backend')
            if marker:
                try:
                    with open(marker, 'w') as f:
                        f.write(path)
                except Exception as e:
                    cmd_logger.error('Failed to create backend stop file: {}'.format(e), extra=self.log_extra)
                    raise
            else:
                raise
        cmd_logger.info('Successfully removed group file {}'.format(self.params['remove_group_file']), extra=self.log_extra)
