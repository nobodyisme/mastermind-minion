import os
import os.path

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class CreateGroupCommand(BaseCommand):

    COMMAND = 'create_group'
    REQUIRED_PARAMS = ('group_base_path_root_dir', 'files')

    def tmp_basename(self, basename):
        return 'new_backend.{basename}'.format(basename=basename)

    def get_vacant_basename(self, base_path):
        free_basename = 1
        while True:
            dest_dir = os.path.join(base_path, str(free_basename))

            tmp_basename = self.tmp_basename(str(free_basename))
            tmp_dir = os.path.join(base_path, tmp_basename)

            if any(map(os.path.exists, [dest_dir, tmp_dir])):
                free_basename += 1
                continue
            break
        return str(free_basename)

    def execute(self):
        group_base_path_root_dir = self.params['group_base_path_root_dir'].rstrip('/')

        basename = self.get_vacant_basename(group_base_path_root_dir)
        tmp_basename = self.tmp_basename(basename)

        tmp_dir = os.path.join(group_base_path_root_dir, tmp_basename)
        logger.info('Creating tmp dir for new group: {}'.format(tmp_dir))
        try:
            os.mkdir(tmp_dir, 0755)
        except Exception:
            logger.exception('Failed to create tmp dir for new group')
            raise

        logger.info('Adding group files')
        for filename, body in self.params['files'].iteritems():
            logger.info('Adding file {}'.format(filename))
            filename = os.path.join(
                tmp_dir,
                filename
            )
            dirname, basefname = os.path.split(filename)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(filename, 'wb') as f:
                f.write(body)

        dest_dir = os.path.join(group_base_path_root_dir, basename)
        logger.info(
            'Renaming tmp dir {tmp_dir} to destination dir {dest_dir}'.format(
                tmp_dir=tmp_dir,
                dest_dir=dest_dir,
            )
        )
        try:
            os.rename(tmp_dir, dest_dir)
        except Exception:
            logger.exception('Failed to rename tmp dir to dest dir')
            raise
