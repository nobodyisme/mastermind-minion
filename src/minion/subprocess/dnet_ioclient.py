from tornado.ioloop import IOLoop

from minion.subprocess.base import BaseSubprocess
from minion.watchers.base import ProgressWatcher


class DnetIoclientSubprocess(BaseSubprocess):

    def __init__(self, cmd, params=None, io_loop=IOLoop.instance()):
        super(DnetIoclientSubprocess, self).__init__(cmd, params=params, io_loop=io_loop)
        if not 'node' in params and not 'node_backend' in params:
            raise ValueError('Parameter "node" or "node_backend" is required')
        self.node = params['node']
        self.node_backend = params['node_backend']

    def watch(self):
        return ProgressWatcher(self.process)

    def status(self):
        res = super(DnetIoclientSubprocess, self).status()
        if self.node:
            res['node'] = self.node
        if self.node_backend:
            res['node_backend'] = self.node_backend
        return res
