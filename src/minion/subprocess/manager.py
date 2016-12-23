import os
import shlex
import time
import uuid

from sqlalchemy import or_
from tornado.ioloop import IOLoop

from minion.logger import logger
from minion.db import Session
from minion.db.commands import Command
from minion.subprocess import subprocess_factory
from minion.subprocess.base_shell import BaseSubprocess


class SubprocessManager(object):
    def __init__(self, ioloop=IOLoop.instance()):
        self.subprocesses = {}
        self.ioloop = ioloop

    @staticmethod
    def pid_exists(pid):
        if not pid:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def update_broken_commands(self):
        s = Session()
        s.begin()
        try:
            for c in s.query(Command).filter_by(exit_code=None):
                if not self.pid_exists(c.pid):
                    c.progress = 1.0
                    c.exit_code = 666
                    c.finish_ts = int(time.time())
                    s.add(c)
                    logger.info(
                        'Command {}, pid {} is considered broken, will be marked as '
                        'finished'.format(
                            c.uid,
                            c.pid
                        )
                    )
                else:
                    logger.warn(
                        'Command {}, pid {} is considered broken, but process is running'.format(
                            c.uid,
                            c.pid
                        )
                    )
            s.commit()
        except Exception:
            logger.exception('Failed to update broken commands')
            s.rollback()
            raise

    def get_subprocess(self, cmd, params):
        Subprocess = subprocess_factory(cmd)
        if 'group' in params:
            self.check_group(params['group'])
        if 'node_backend' in params:
            self.check_node_backend(params['node_backend'])
        return Subprocess

    def run(self, command, params, env=None, success_codes=None):
        logger.info('command to execute: {0}'.format(command))
        logger.info('parameters supplied: {params}, env variables: {env}'.format(
            params=params,
            env=env,
        ))
        if isinstance(command, unicode):
            command = command.encode('utf-8')
        cmd = (shlex.split(command)
               if isinstance(command, basestring) else
               command)

        if params.get('task_id'):
            running_uid = self.try_find_running_subprocess(params['task_id'])
            if running_uid:
                running_sub = self.subprocesses[running_uid]
                logger.info(
                    'command execution is not required, process for task {} is already running: '
                    '{}'.format(
                        params['task_id'],
                        running_sub.status()
                    )
                )
                return running_uid
        Subprocess = self.get_subprocess(cmd, params)
        uid = uuid.uuid4().hex
        if issubclass(Subprocess, BaseSubprocess):
            sub = Subprocess(uid, cmd, params=params, env=env, success_codes=success_codes)
        else:
            sub = Subprocess(uid, params=params)
        sub.run()
        logger.info('command execution started successfully: {}'.format(sub))

        self.subprocesses[uid] = sub
        return uid

    def status(self, uid):
        if uid in self.subprocesses:
            return self.subprocesses[uid].status()

        s = Session()
        command = s.query(Command).get(uid)
        if command is None:
            raise ValueError('Unknown command uid: {0}'.format(uid))
        return command.status()

    def unfinished_commands(self, finish_ts_gte=None):
        res = {}

        # 1. Getting unfinished commands from local db
        s = Session()
        criterias = []
        if finish_ts_gte:
            criterias.append(Command.finish_ts == None)
            criterias.append(Command.finish_ts >= finish_ts_gte)
        for c in s.query(Command).filter(or_(*criterias)):
            res[c.uid] = c.status()

        # 2. Updating with in-memory commands
        for uid, sp in self.subprocesses.iteritems():
            cmd_status = sp.status()
            if (
                finish_ts_gte and
                cmd_status['finish_ts'] and
                cmd_status['finish_ts'] < finish_ts_gte
            ):
                continue
            res[uid] = cmd_status
        return res

    def terminate(self, uid):
        if uid not in self.subprocesses:
            raise ValueError('Unknown command uid: {0}'.format(uid))
        logger.info(
            'terminating command {}, pid: {}'.format(uid, self.subprocesses[uid].process.pid)
        )

        # result, error, sub = self.subprocesses[uid].terminate().result()
        code = self.subprocesses[uid].terminate()
        if code:
            raise RuntimeError('Failed to terminate command {}, exit code: {}'.format(uid, code))

    def check_group(self, group):
        for subprocess in self.subprocesses.itervalues():
            if subprocess.params.get('group') == group and subprocess.finish_ts is None:
                raise ValueError(
                    'Task for group {} is already running, {}'.format(group, subprocess)
                )

    def check_node_backend(self, node_backend):
        for subprocess in self.subprocesses.itervalues():
            if subprocess.params.get('node_backend') == node_backend and subprocess.finish_ts is None:
                raise ValueError(
                    'Task for node backend {} is already running: {}'.format(
                        node_backend,
                        subprocess
                    )
                )

    def try_find_running_subprocess(self, task_id):
        for uid, subprocess in self.subprocesses.iteritems():
            status = subprocess.status()
            if status.get('task_id', None) == task_id:
                return uid


manager = SubprocessManager()
