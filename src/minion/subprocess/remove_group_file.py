import os

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class RemoveGroupFileCommand(BaseCommand):

    COMMAND = 'remove_group_file'
    REQUIRED_PARAMS = ('remove_group_file',)

    def execute(self):
        logger.info('Removing group file {0}'.format(self.params['remove_group_file']))
        try:
            os.remove(self.params['remove_group_file'])
        except Exception as e:
            logger.error('Failed to remove group file: {}'.format(e))
            raise
        logger.info('Successfully removed group file {}'.format(self.params['remove_group_file']))
