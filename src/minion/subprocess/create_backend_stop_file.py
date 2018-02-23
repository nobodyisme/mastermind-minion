from tornado import gen

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class CreateBackendStopFileCommand(BaseCommand):

    COMMAND = 'create_backend_stop_file'
    # TODO: rename to stop_file
    REQUIRED_PARAMS = ('create_stop_file',)

    @gen.coroutine
    def execute(self):
        stop_file = self.params['create_stop_file']
        try:
            open(stop_file, 'w').close()
        except Exception as e:
            cmd_logger.error('Failed to create backend stop marker: {}'.format(e), extra=self.log_extra)
            raise
        cmd_logger.info('Successfully created backend stop marker: {}'.format(stop_file), extra=self.log_extra)
