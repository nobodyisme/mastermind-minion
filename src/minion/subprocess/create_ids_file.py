import os
import os.path

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class CreateIdsFileCommand(BaseCommand):

    COMMAND = 'create_ids_file'
    REQUIRED_PARAMS = ('ids',)

    def execute(self):
        ids_file = self.params['ids']
        cmd_logger.info('Generating ids file {}'.format(ids_file), extra=self.log_extra)
        if os.path.exists(ids_file):
            cmd_logger.info('Ids file {} already exists'.format(ids_file), extra=self.log_extra)
        else:
            try:
                with open(ids_file, 'wb') as f:
                    f.write(os.urandom(64))
            except Exception:
                cmd_logger.exception(
                    'Failed to create ids file {}'.format(ids_file),
                    extra=self.log_extra,
                )
                raise
        cmd_logger.info('Successfully created ids file {}'.format(ids_file), extra=self.log_extra)
