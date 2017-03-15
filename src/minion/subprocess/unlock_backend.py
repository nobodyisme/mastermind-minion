import os

from minion.logger import cmd_logger
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
            cmd_logger.error('Failed to remove backend marker: {}'.format(e), extra=self.log_extra)
            raise
        cmd_logger.info('Successfully removed backend marker: {}'.format(marker), extra=self.log_extra)
