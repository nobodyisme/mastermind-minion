import os

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class MovePathCommand(BaseCommand):

    COMMAND = 'move_path'
    REQUIRED_PARAMS = ('move_src', 'move_dst')

    def execute(self):
        try:
            os.rename(self.params['move_src'], self.params['move_dst'])
        except Exception as e:
            logger.error('Failed to execute move path command: {} to {}: {}'.format(
                self.params['move_src'],
                self.params['move_dst'],
                e
            ))
            marker = self.params.get('stop_backend')
            if marker:
                try:
                    open(marker, 'w').close()
                except Exception as e:
                    logger.error('Failed to create backend stop file: {}'.format(e))
                    raise
        logger.info('Successfully performed move task: {} to {}'.format(
            self.params['move_src'],
            self.params['move_dst']
        ))
