from tornado.ioloop import IOLoop

from minion.subprocess.base import BaseSubprocess
from minion.watchers.base import ProgressWatcher


class UbicSubprocess(BaseSubprocess):

    def __init__(self, cmd, params=None, io_loop=IOLoop.instance()):
        super(UbicSubprocess, self).__init__(cmd, params=params, io_loop=io_loop)
        if not 'node' in params:
            raise ValueError('Parameter "node" is required')
        self.node = params['node']

    def watch(self):
        return ProgressWatcher(self.process)

    def run(self):
        super(UbicSubprocess, self).run()
        self.watcher.on_success(self.create_group_file_marker)

    def status(self):
        res = super(UbicSubprocess, self).status()
        res['node'] = self.node
        return res

    def create_group_file_marker(self):
        if self.params.get('group_file_marker'):
            try:
                group = str(int(self.params.get('group')))
                path = self.params.get('group_file_marker')
                dirname = os.path.dirname(path)
                if not os.path.exists(dirname):
                    os.makedirs(dirname, 0755)
                with open(path, 'w') as f:
                    f.write(group + '\n')
            except Exception as e:
                logger.exception('Failed to create group file marker: {0}'.format(e))
            else:
                logger.info('Successfully created group file marker '
                            'for group {0}'.format(group))
        else:
            logger.info('Group file marker creation was not requested for '
                'group {0}'.format(self.params.get('group')))
