import shutil

from tornado import gen

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class RemovePathCommand(BaseCommand):

    COMMAND = 'remove_path'
    REQUIRED_PARAMS = ('remove_path',)

    @gen.coroutine
    def execute(self):
        try:
            shutil.rmtree(self.params['remove_path'])
        except Exception:
            cmd_logger.exception('Failed to remove path {}'.format(self.params['remove_path']), extra=self.log_extra)
            raise
        cmd_logger.info('Successfully removed path {}'.format(self.params['remove_path']), extra=self.log_extra)
