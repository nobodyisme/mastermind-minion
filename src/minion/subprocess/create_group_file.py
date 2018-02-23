import os
import os.path

from tornado import gen

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class CreateGroupFileCommand(BaseCommand):

    COMMAND = 'create_group_file'
    REQUIRED_PARAMS = ('group', 'group_file',)

    @gen.coroutine
    def execute(self):
        group = str(int(self.params['group']))
        path = self.params['group_file'].format(group_id=group)
        try:
            if os.path.exists(path):
                os.rename(path, path + '.bak')
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname, 0755)
            with open(path, 'w') as f:
                f.write(group)
        except Exception:
            cmd_logger.exception('Failed to create group file', extra=self.log_extra)
            raise
        cmd_logger.info('Successfully created group file {} for group {}'.format(path, group), extra=self.log_extra)
