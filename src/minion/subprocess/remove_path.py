import shutil

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class RemovePathCommand(BaseCommand):

    COMMAND = 'remove_path'
    REQUIRED_PARAMS = ('remove_path',)

    def execute(self):
        try:
            shutil.rmtree(self.params['remove_path'])
        except Exception:
            logger.exception('Failed to remove path {}'.format(self.params['remove_path']))
            raise
        logger.info('Successfully removed path {}'.format(self.params['remove_path']))
