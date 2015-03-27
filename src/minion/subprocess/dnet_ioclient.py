from tornado.ioloop import IOLoop

from minion.subprocess.base import BaseSubprocess
from minion.watchers.base import ProgressWatcher


class DnetIoclientSubprocess(BaseSubprocess):

    def __init__(self, cmd, params=None, success_codes=None, io_loop=IOLoop.instance()):
        super(DnetIoclientSubprocess, self).__init__(cmd, params=params, success_codes=success_codes, io_loop=io_loop)
        if not 'node' in params and not 'node_backend' in params:
            raise ValueError('Parameter "node" or "node_backend" is required')
        self.node = params.get('node')
        self.node_backend = params.get('node_backend')

    def status(self):
        res = super(DnetIoclientSubprocess, self).status()
        if self.node:
            res['node'] = self.node
        if self.node_backend:
            res['node_backend'] = self.node_backend
        return res
