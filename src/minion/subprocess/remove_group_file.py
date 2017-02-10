import os

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class RemoveGroupFileCommand(BaseCommand):

    COMMAND = 'remove_group_file'
    REQUIRED_PARAMS = ('remove_group_file',)

    def execute(self):
        logger.info('Removing group file {0}'.format(self.params['remove_group_file']))
        path = self.params['remove_group_file']
        try:
            os.remove(path)
        except Exception as e:
            logger.error('Failed to remove group file: {}'.format(e))
            marker = self.params.get('stop_backend')
            if marker:
                try:
                    with open(marker, 'w') as f:
                        f.write(path)
                except Exception as e:
                    logger.error('Failed to create backend stop file: {}'.format(e))
                    raise
            else:
                raise
        logger.info('Successfully removed group file {}'.format(self.params['remove_group_file']))
