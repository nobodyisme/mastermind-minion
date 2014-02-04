import collections
import shlex
import uuid

from tornado.ioloop import IOLoop
from tornado.process import Subprocess


class SubprocessManager(object):
    def __init__(self, ioloop=IOLoop.instance()):
        self.subprocesses = {}
        self.ioloop = ioloop

    def run(self, command, watcher):
        cmd = (shlex.split(command)
               if isinstance(command, basestring) else
               command)
        sub = Subprocess(cmd,
                         stdout=Subprocess.STREAM,
                         io_loop=self.ioloop)
        uid = uuid.uuid4().hex
        self.subprocesses[uid] = watcher(sub)
        return uid

    def status(self, uid):
        return self.subprocesses[uid].status()


manager = SubprocessManager()
