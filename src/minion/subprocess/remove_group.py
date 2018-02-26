import os
import os.path
import shutil

from tornado import gen

from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand


class RemoveGroupCommand(BaseCommand):

    COMMAND = 'remove_group'
    REQUIRED_PARAMS = ('group_base_path',)

    def removed_basename(self, basename):
        return 'removed_backend.{basename}.{group}'.format(
            basename=basename,
            group=self.params['group'],
        )

    @gen.coroutine
    def execute(self):
        group_base_path = self.params['group_base_path'].rstrip('/')

        if not os.path.exists(group_base_path):
            raise RuntimeError(
                'Group dir {path} does not exist'.format(
                    path=group_base_path,
                )
            )

        dst_base_path, basename = os.path.split(group_base_path)

        delete_backend_dir = self.params.get('delete_backend_dir', False)

        if delete_backend_dir:

            try:
                shutil.rmtree(group_base_path)
            except OSError as e:
                if e.errno == 2:
                    # errno == 2: No such file or directory
                    pass
                else:
                    raise
            except Exception:
                cmd_logger.exception(
                    'Failed to remove backend directory {}'.format(self.params['group_base_path']),
                    extra=self.log_extra,
                )
                raise

        else:
            remove_path = os.path.join(
                dst_base_path,
                self.removed_basename(basename)
            )
            cmd_logger.info(
                'Renaming group base dir {tmp_dir} to destination dir {dest_dir}'.format(
                    tmp_dir=group_base_path,
                    dest_dir=remove_path,
                ),
                extra=self.log_extra,
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
                else:
                    raise
            except Exception:
                cmd_logger.exception('Failed to rename tmp dir to dest dir', extra=self.log_extra)
                raise
