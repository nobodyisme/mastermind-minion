import os

from tornado import gen

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class MovePathCommand(BaseCommand):

    COMMAND = 'move_path'
    REQUIRED_PARAMS = ('move_src', 'move_dst')

    @gen.coroutine
    def execute(self):
        try:
            os.rename(self.params['move_src'], self.params['move_dst'])
        except Exception as e:
            cmd_logger.error(
                'Failed to execute move path command: {} to {}: {}'.format(
                    self.params['move_src'],
                    self.params['move_dst'],
                    e,
                ),
                extra=self.log_extra,
            )
            marker = self.params.get('stop_backend')
            if marker:
                try:
                    with open(marker, 'w') as f:
                        f.write(self.params['move_src'])
                except Exception as e:
                    cmd_logger.error('Failed to create backend stop file: {}'.format(e), extra=self.log_extra)
                    raise
        cmd_logger.info(
            'Successfully performed move task: {} to {}'.format(
                self.params['move_src'],
                self.params['move_dst'],
            ),
            extra=self.log_extra,
        )
