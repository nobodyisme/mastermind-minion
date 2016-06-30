import os
import os.path
import shutil

from tornado.ioloop import IOLoop

from minion.config import config
from minion.logger import logger
from minion.subprocess.base import BaseSubprocess
from minion.watchers.rsync import RsyncWatcher


class RsyncSubprocess(BaseSubprocess):

    RSYNC_PASSWORD = config['auth'].get('rsync_password', '')

    def __init__(self, uid, cmd, params=None, env=None, success_codes=None, io_loop=IOLoop.instance()):
        super(RsyncSubprocess, self).__init__(
            uid,
            cmd,
            params=params,
            env=env,
            success_codes=success_codes,
            io_loop=io_loop,
        )
        if not 'group' in params:
            raise ValueError('Parameter "group" is required')
        self.group = params['group']
        if self.RSYNC_PASSWORD:
            self.env['RSYNC_PASSWORD'] = self.RSYNC_PASSWORD

    watcher_base = RsyncWatcher

    def run(self):
        super(RsyncSubprocess, self).run()
        self.watcher.on_success(self.create_group_file)

    def status(self):
        res = super(RsyncSubprocess, self).status()
        res['group'] = self.group
        return res

    def create_group_file(self):
        if self.params.get('group_file'):
            try:
                group = str(int(self.params.get('group')))
                path = self.params['group_file'].format(group_id=group)
                if os.path.exists(path):
                    os.rename(path, path + '.bak')
                dirname = os.path.dirname(path)
                if not os.path.exists(dirname):
                    os.makedirs(dirname, 0755)
                with open(path, 'w') as f:
                    f.write(group)
            except Exception as e:
                logger.exception('Failed to create group file: {0}'.format(e))
            else:
                logger.info('Successfully created group file '
                            'for group {0}'.format(group))
        else:
            logger.info('Group file creation was not requested for '
                'group {0}'.format(self.params.get('group')))

        if self.params.get('remove_path'):
            try:
                shutil.rmtree(self.params['remove_path'])
            except Exception as e:
                logger.exception('Failed to remove path {0}: {1}'.format(
                    self.params['remove_path'], e))
            else:
                logger.info('Successfully removed path {0} '
                    'for group {1}'.format(self.params['remove_path'], group))
        else:
            logger.info('Path removal was not requested for '
                'group {0}'.format(self.params.get('group')))

        if self.params.get('ids'):
            ids_file = self.params['ids']
            logger.info('Generating ids file {} required'.format(ids_file))
            if os.path.exists(ids_file):
                logger.info('Ids file {} already exists'.format(ids_file))
            else:
                try:
                    with open(ids_file, 'wb') as f:
                        f.write(os.urandom(64))
                except Exception as e:
                    logger.exception('Failed to create ids file {}'.format(
                        ids_file))
                else:
                    logger.info('Successfully created ids file {} '
                        'for group {1}'.format(ids_file, group))
