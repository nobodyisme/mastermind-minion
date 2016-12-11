import os
import os.path

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class CreateGroupFileCommand(BaseCommand):

    COMMAND = 'create_group_file'
    REQUIRED_PARAMS = ('group', 'group_file',)

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
            logger.exception('Failed to create group file')
            raise
        logger.info('Successfully created group file {} for group {}'.format(path, group))
