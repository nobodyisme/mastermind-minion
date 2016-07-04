import ConfigParser
import copy
import os
import shlex
import signal
import time

from tornado.ioloop import IOLoop
from tornado.process import Subprocess

from minion.config import config
from minion.db import Session
import minion.db.commands
from minion.watchers.base import ProgressWatcher


class BaseSubprocess(object):

    def __init__(self, uid, cmd, params=None, env=None, success_codes=None, io_loop=IOLoop.instance()):
        self.uid = uid
        self.cmd = cmd
        self.cmd_str = ' '.join(self.cmd)
        self.env = copy.copy(os.environ)
        config_env = self._get_config_env_vars(cmd[0])
        if config_env:
            self.env.update(config_env)
        if env:
            self.env.update(env)
        self.process = None
        self.watcher = None
        self.params = params
        self.success_codes = success_codes
        self.io_loop = io_loop

    def _get_config_env_vars(self, command):
        try:
            cfg_items = config.items('{command}_env'.format(command=command))
        except ConfigParser.NoSectionError:
            return {}

        # NOTE: ConfigParser.items lowercases option names
        return dict(
            (env_var_name.upper(), env_var_value)
            for env_var_name, env_var_value in cfg_items.iteritems()
        )

    def run(self):
        self.process = Subprocess(self.cmd,
                                  stdout=Subprocess.STREAM,
                                  stderr=Subprocess.STREAM,
                                  env=self.env,
                                  io_loop=self.io_loop)

        # create db record
        s = Session()
        s.begin()
        command = minion.db.commands.Command(uid=self.uid,
                          pid=self.process.pid,
                          command=self.cmd_str,
                          start_ts=int(time.time()),
                          task_id=self.params.get('task_id'))
        #TODO:
        #what about group_id, node, node_backend ?

        s.add(command)
        s.commit()

        self.watcher = self.watch(command)

    def watch(self, command):
        return self.watcher_base(self.process, command, success_codes=self.success_codes)

    watcher_base = ProgressWatcher

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
