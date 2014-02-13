import os
import os.path

from tornado.ioloop import IOLoop

from minion.config import config
from minion.logger import logger
from minion.subprocess.base import BaseSubprocess
from minion.watchers.rsync import RsyncWatcher


class RsyncSubprocess(BaseSubprocess):

    RSYNC_PASSWORD = config['auth'].get('rsync_password', '')

    def __init__(self, cmd, params=None, io_loop=IOLoop.instance()):
        super(RsyncSubprocess, self).__init__(cmd, params=params, io_loop=io_loop)
        if self.RSYNC_PASSWORD:
            self.env['RSYNC_PASSWORD'] = self.RSYNC_PASSWORD

    def watch(self):
        return RsyncWatcher(self.process)

    def run(self):
        super(RsyncSubprocess, self).run()
        self.watcher.on_success(self.create_group_file)

    def create_group_file(self):
        if self.params.get('group'):
            try:
                group = str(int(self.params.get('group')))
                path = self.params.get('group_file')
                if os.path.exists(path):
                    os.rename(path, path + '.bak')
                dirname = os.path.dirname(path)
                if not os.path.exists(dirname):
                    os.makedirs(dirname, 0755)
                with open(path, 'w') as f:
                    f.write(group + '\n')
            except Exception as e:
                logger.exception('Failed to create group file: {0}'.format(e))
            else:
                logger.info('Successfully created group file '
                            'for group {0}'.format(group))
