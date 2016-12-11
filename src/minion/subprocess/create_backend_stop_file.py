from minion.logger import logger
from minion.subprocess.base import BaseCommand


class CreateBackendStopFileCommand(BaseCommand):

    COMMAND = 'create_backend_stop_file'
    # TODO: rename to stop_file
    REQUIRED_PARAMS = ('create_stop_file',)

    def execute(self):
        stop_file = self.params['create_stop_file']
        try:
            open(stop_file, 'w').close()
        except Exception as e:
            logger.error('Failed to create backend stop marker: {}'.format(e))
            raise
        logger.info('Successfully created backend stop marker: {}'.format(stop_file))
