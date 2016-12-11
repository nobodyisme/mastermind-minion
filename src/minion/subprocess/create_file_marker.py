import os
import os.path

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class CreateFileMarkerCommand(BaseCommand):

    COMMAND = 'create_group_file'
    REQUIRED_PARAMS = ('group_file_marker',)

    def execute(self):
        group = (
            str(self.params.get('group'))
            if 'group' in self.params.get('group') else
            ''
        )
        try:
            path = self.params['group_file_marker'].format(group_id=group)
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname, 0755)
            with open(path, 'w') as f:
                f.write(group)
        except Exception as e:
            logger.error('Failed to create group file marker: {}'.format(e))
            raise
        logger.info('Successfully created group file marker for group {}'.format(group))
