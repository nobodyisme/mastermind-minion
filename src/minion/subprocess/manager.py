import collections
import copy
import os
import shlex
import uuid

from tornado.ioloop import IOLoop

from minion.logger import logger
from minion.subprocess.rsync import RsyncSubprocess
from minion.subprocess.dnet_ioclient import DnetIoclientSubprocess
from minion.subprocess.ubic import UbicSubprocess


class SubprocessManager(object):
    def __init__(self, ioloop=IOLoop.instance()):
        self.subprocesses = {}
        self.ioloop = ioloop

    def get_subprocess(self, cmd, params):
        if cmd[0] == 'rsync':
            Subprocess = RsyncSubprocess
            self.check(params.get('group', 0))
        elif cmd[0] == 'dnet_ioclient':
            Subprocess = DnetIoclientSubprocess
        elif cmd[0] == 'ubic':
            Subprocess = UbicSubprocess
        else:
            raise ValueError('Unsupported command: {0}'.format(cmd[0]))
        return Subprocess

    def run(self, command, params):
        logger.info('command to execute: {0}'.format(command))
        logger.info('parameters supplied: {0}'.format(params))
        if isinstance(command, unicode):
            command = command.encode('utf-8')
        cmd = (shlex.split(command)
               if isinstance(command, basestring) else
               command)

        Subprocess = self.get_subprocess(cmd, params)
        sub = Subprocess(cmd, params=params)
        sub.run()

        logger.info('command execution started successfully, pid: {0}'.format(sub.pid))
        uid = uuid.uuid4().hex
        self.subprocesses[uid] = sub
        return uid

    def status(self, uid):
        return self.subprocesses[uid].status()

    def keys(self):
        return self.subprocesses.keys()

    def check(self, group):
        for subprocess in self.subprocesses.itervalues():
            if not isinstance(subprocess, RsyncSubprocess):
                continue
            status = subprocess.status()
            if status['group'] == group and status['progress'] < 1.0:
                raise ValueError('Task for group {0} is already running, '
                    'pid: {1}'.format(group, status['pid']))


manager = SubprocessManager()
