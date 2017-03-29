import os
import os.path

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class CreateFileMarkerCommand(BaseCommand):

    COMMAND = 'create_file_marker'
    REQUIRED_PARAMS = ('group_file_marker',)

    def execute(self):
        group = (
            str(self.params.get('group'))
            if 'group' in self.params.get('group') else
            ''
        )
        path = self.params['group_file_marker'].format(group_id=group)
        try:
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname, 0755)
            with open(path, 'w') as f:
                f.write(group)
        except Exception as e:
            cmd_logger.error('Failed to create group file marker: {}'.format(e), extra=self.log_extra)
            marker = self.params.get('stop_backend')
            if marker:
                try:
                    with open(marker, 'w') as f:
                        f.write(path)
                except Exception as e:
                    cmd_logger.error('Failed to create backend stop file: {}'.format(e), extra=self.log_extra)
                    raise
            else:
                raise
        cmd_logger.info('Successfully created group file marker for group {}'.format(group), extra=self.log_extra)
