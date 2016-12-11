import os
import os.path

from minion.logger import logger
from minion.subprocess.base import BaseCommand


class CreateIdsFileCommand(BaseCommand):

    COMMAND = 'create_ids_file'
    REQUIRED_PARAMS = ('ids',)

    def execute(self):
        ids_file = self.params['ids']
        logger.info('Generating ids file {}'.format(ids_file))
        if os.path.exists(ids_file):
            logger.info('Ids file {} already exists'.format(ids_file))
        else:
            try:
                with open(ids_file, 'wb') as f:
                    f.write(os.urandom(64))
            except Exception:
                logger.exception('Failed to create ids file {}'.format(
                    ids_file)
                )
                raise
        logger.info('Successfully created ids file {}'.format(ids_file))
