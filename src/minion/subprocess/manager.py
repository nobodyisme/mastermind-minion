import collections
import copy
import os
import shlex
import uuid

from tornado.ioloop import IOLoop

from minion.logger import logger
from minion.subprocess.rsync import RsyncSubprocess
from minion.subprocess.dnet_ioclient import DnetIoclientSubprocess
from minion.subprocess.dnet_client import DnetClientSubprocess
from minion.subprocess.dnet_recovery import DnetRecoverySubprocess
from minion.subprocess.ubic import UbicSubprocess


class SubprocessManager(object):
    def __init__(self, ioloop=IOLoop.instance()):
        self.subprocesses = {}
        self.ioloop = ioloop

    def get_subprocess(self, cmd, params):
        if cmd[0] == 'rsync':
            Subprocess = RsyncSubprocess
            self.check_group(params.get('group', 0))
        elif cmd[0] == 'dnet_ioclient':
            Subprocess = DnetIoclientSubprocess
            self.check_node(params.get('node'), params.get('node_backend'))
        elif cmd[0] == 'dnet_client':
            Subprocess = DnetClientSubprocess
            self.check_node(params.get('node'), params.get('node_backend'))
        elif cmd[0] == 'dnet_recovery':
            Subprocess = DnetRecoverySubprocess
            self.check_node(params.get('node'), params.get('node_backend'))
        elif cmd[0] == 'ubic':
            Subprocess = UbicSubprocess
            self.check_node(params.get('node'), params.get('node_backend'))
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

        if params.get('task_id'):
            running_uid = self.try_find_running_subprocess(params['task_id'])
            if running_uid:
                running_sub = self.subprocesses[running_uid]
                logger.info('command execution is not required, '
                    'process for task {0} is already running: {1}'.format(
                        params['task_id'], running_sub.status()))
                return running_uid
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

    def terminate(self, uid):
        if not uid in self.subprocesses:
            raise ValueError('Unknown command uid: {0}'.format(uid))
        logger.info('terminating command {0}, pid: {1}'.format(uid,
            self.subprocesses[uid].process.pid))

        # result, error, sub = self.subprocesses[uid].terminate().result()
        code = self.subprocesses[uid].terminate()
        if code:
            raise RuntimeError('Failed to terminate command {0}, '
                'exit code: {1}'.format(uid, code))

    def check_group(self, group):
        for subprocess in self.subprocesses.itervalues():
            if not isinstance(subprocess, RsyncSubprocess):
                continue
            status = subprocess.status()
            if status['group'] == group and status['progress'] < 1.0:
                raise ValueError('Task for group {0} is already running, '
                    'pid: {1}'.format(group, status['pid']))

    def check_node(self, node, node_backend):
        for subprocess in self.subprocesses.itervalues():
            status = subprocess.status()
            if node and status.get('node') == node and status['progress'] < 1.0:
                raise ValueError('Task for node {0} is already running, '
                    'pid: {1}'.format(node, status['pid']))
            elif node_backend and status.get('node_backend') == node_backend and status['progress'] < 1.0:
                raise ValueError('Task for node backend {0} is already running, '
                    'pid: {1}'.format(node_backend, status['pid']))

    def try_find_running_subprocess(self, task_id):
        for uid, subprocess in self.subprocesses.iteritems():
            status = subprocess.status()
            if status.get('task_id') and status['task_id'] == task_id:
                return uid


manager = SubprocessManager()
