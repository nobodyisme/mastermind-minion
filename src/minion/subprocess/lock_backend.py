import os

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class LockBackendCommand(BaseCommand):

    COMMAND = 'lock_backend'
    REQUIRED_PARAMS = (
        'mark_backend',
        # 'holder_id',  # temporary not required, uncomment this param later
    )

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
            raise
        cmd_logger.info('Successfully created backend marker: {}'.format(marker), extra=self.log_extra)
