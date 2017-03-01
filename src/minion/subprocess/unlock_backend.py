import os

from minion.logger import logger
from minion.subprocess.base import BaseCommand
import errno


class UnlockBackendCommand(BaseCommand):

    COMMAND = 'unlock_backend'
    REQUIRED_PARAMS = ('unmark_backend',)

    def execute(self):
        marker = self.params['unmark_backend']
        try:
            os.remove(marker)
        except OSError as e:
            if e.errno == errno.ENOENT:
                # errno == 2: No such file or directory
                pass
            else:
                raise
        except Exception as e:
            logger.error('Failed to remove backend marker: {}'.format(e))
            raise
        logger.info('Successfully removed backend marker: {}'.format(marker))
