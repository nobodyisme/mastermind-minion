import os

from tornado import gen

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class LockBackendCommand(BaseCommand):

    COMMAND = 'lock_backend'
    REQUIRED_PARAMS = (
        'mark_backend',
        # 'holder_id',  # temporary not required, uncomment this param later
    )

    @gen.coroutine
    def execute(self):
        holder_id = self.params.get('holder_id', '')
        marker = self.params['mark_backend']

        try:
            dirname = os.path.dirname(marker)
            if not os.path.exists(dirname):
                os.makedirs(dirname, 0755)

            with open(marker, 'w') as f:
                f.write(holder_id)

        except Exception as e:
            cmd_logger.error('Failed to create backend marker: {}'.format(e), extra=self.log_extra)
            # stop_marker_on_errors option is used in tasks to restore group
            # if node backend is broken and currently in RO state, we cannot create lock file,
            # so we can only skip exception.
            if self.params.get('skip_errors'):
                pass
            else:
                raise

        cmd_logger.info('Successfully created backend marker: {}'.format(marker), extra=self.log_extra)
