import copy
import os
import shlex
import signal

from tornado.ioloop import IOLoop
from tornado.process import Subprocess

from minion.watchers.base import ProgressWatcher


class BaseSubprocess(object):

    def __init__(self, cmd, params=None, success_codes=None, io_loop=IOLoop.instance()):
        self.cmd = cmd
        self.cmd_str = ' '.join(self.cmd)
        self.env = copy.copy(os.environ)
        self.process = None
        self.watcher = None
        self.params = params
        self.success_codes = success_codes
        self.io_loop = io_loop

    def run(self):
        self.process = Subprocess(self.cmd,
                                  stdout=Subprocess.STREAM,
                                  stderr=Subprocess.STREAM,
                                  env=self.env,
                                  io_loop=self.io_loop)
        self.watcher = self.watch()

    def watch(self):
        return self.watcher_base(self.process, success_codes=self.success_codes)

    @property
    def watcher_base(self):
        return ProgressWatcher

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
            'task_id': self.params.get('task_id'),
        })

        return res

    def terminate(self):
        if self.process.returncode is None:
            self.__children_pids(self.__terminate_pid_tree)

    def __children_pids(self, callback):
        cmd_str = 'pgrep -P {pid}'.format(pid=self.process.pid)
        sub = Subprocess(shlex.split(cmd_str),
                         stdout=Subprocess.STREAM,
                         stderr=Subprocess.STREAM,
                         io_loop=self.io_loop)
        sub.stdout.read_until_close(callback=callback)

    def __terminate_pid_tree(self, pids):
        tree_pids = pids.strip() + ' ' + str(self.process.pid)
        cmd_str = 'kill ' + tree_pids
        sub = Subprocess(shlex.split(cmd_str),
                         stdout=Subprocess.STREAM,
                         stderr=Subprocess.STREAM,
                         io_loop=self.io_loop)
