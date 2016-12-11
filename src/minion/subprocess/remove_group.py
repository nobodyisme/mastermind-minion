import os
import os.path

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class RemoveGroupCommand(BaseCommand):

    COMMAND = 'remove_group'
    REQUIRED_PARAMS = ('group_base_path',)

    def removed_basename(self, basename):
        return 'removed_backend.{basename}'.format(basename=basename)

    def execute(self):
        group_base_path = self.params['group_base_path'].rstrip('/')

        if not os.path.exists(group_base_path):
            raise RuntimeError(
                'Group dir {path} does not exist'.format(
                    path=group_base_path,
                )
            )

        dst_base_path, basename = os.path.split(group_base_path)

        remove_path = os.path.join(
            dst_base_path,
            self.removed_basename(basename)
        )
        logger.info(
            'Renaming group base dir {tmp_dir} to destination dir {dest_dir}'.format(
                tmp_dir=group_base_path,
                dest_dir=remove_path,
            )
        )
        try:
            os.rename(group_base_path, remove_path)
        except OSError as e:
            if e.errno == 2:
                # errno == 2: No such file or directory
                if os.path.exists(remove_path):
                    # group_base_path was already renamed, not an error
                    pass
                else:
                    raise
        except Exception:
            logger.exception('Failed to rename tmp dir to dest dir')
            raise
