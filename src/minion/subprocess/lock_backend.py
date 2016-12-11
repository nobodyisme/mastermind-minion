from minion.logger import logger
from minion.subprocess.base import BaseCommand


class LockBackendCommand(BaseCommand):

    COMMAND = 'lock_backend'
    REQUIRED_PARAMS = ('mark_backend',)

    def execute(self):
        marker = self.params['mark_backend']
        try:
            open(marker, 'w').close()
        except Exception as e:
            logger.error('Failed to create backend marker: {}'.format(e))
            raise
        logger.info('Successfully created backend marker: {}'.format(marker))
