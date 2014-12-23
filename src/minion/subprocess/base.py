import copy
import os

from tornado.ioloop import IOLoop
from tornado.process import Subprocess


class BaseSubprocess(object):

    def __init__(self, cmd, params=None, io_loop=IOLoop.instance()):
        self.cmd = cmd
        self.cmd_str = ' '.join(self.cmd)
        self.env = copy.copy(os.environ)
        self.process = None
        self.watcher = None
        self.params = params
        self.io_loop = io_loop

    def run(self):
        self.process = Subprocess(self.cmd,
                                  stdout=Subprocess.STREAM,
                                  stderr=Subprocess.STREAM,
                                  env=self.env,
                                  io_loop=self.io_loop)
        self.watcher = self.watch()

    def watch(self):
        raise NotImplemented('Use dedicated subprocess objects')

    @property
    def pid(self):
        assert self.process
        return self.process.pid

    def status(self):
        assert self.process
        assert self.watcher

        res = self.watcher.status()
        res.update({
            'pid': self.process.pid,
            'command': self.cmd_str,
        })

        return res

    def terminate(self):
        if self.process.returncode is None:
            self.process.terminate()
