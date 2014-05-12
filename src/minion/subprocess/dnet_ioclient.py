from tornado.ioloop import IOLoop

from minion.subprocess.base import BaseSubprocess
from minion.watchers.base import ProgressWatcher


class DnetIoclientSubprocess(BaseSubprocess):

    def __init__(self, cmd, params=None, io_loop=IOLoop.instance()):
        super(DnetIoclientSubprocess, self).__init__(cmd, params=params, io_loop=io_loop)
        if not 'node' in params:
            raise ValueError('Parameter "node" is required')
        self.node = params['node']

    def watch(self):
        return ProgressWatcher(self.process)

    def status(self):
        res = super(DnetIoclientSubprocess, self).status()
        res['node'] = self.node
        return res
