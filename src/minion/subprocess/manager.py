import os
import shlex
import time
import uuid

from sqlalchemy import desc
from sqlalchemy import or_
from tornado.ioloop import IOLoop

from minion.logger import logger
from minion.logger import cmd_logger
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
                log_extra = {'task_id': c.task_id, 'job_id': c.job_id}
                if not self.pid_exists(c.pid):
                    c.progress = 1.0
                    c.exit_code = 666
                    c.finish_ts = int(time.time())
                    s.add(c)
                    cmd_logger.info(
                        'Command {}, pid {} is considered broken, will be marked as '
                        'finished'.format(
                            c.uid,
                            c.pid
                        ),
                        extra=log_extra,
                    )
                else:
                    cmd_logger.warn(
                        'Command {}, pid {} is considered broken, but process is running'.format(
                            c.uid,
                            c.pid
                        ),
                        extra=log_extra,
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
        log_extra = {'task_id': params.get('task_id'), 'job_id': params.get('job_id')}
        cmd_logger.info('command to execute: {0}'.format(command), extra=log_extra)
        cmd_logger.info(
            'parameters supplied: {params}, env variables: {env}'.format(
                params=params,
                env=env,
            ),
            extra=log_extra,
        )
        if isinstance(command, unicode):
            command = command.encode('utf-8')
        cmd = (shlex.split(command)
               if isinstance(command, basestring) else
               command)

        subprocess_uid, status = self.try_find_nonfailed_subprocess_and_status(params.get('job_id'), params.get('task_id'))
        if subprocess_uid:
            cmd_logger.info(
                'command execution is not required, process for task {} is already running: '
                '{}'.format(
                    params['task_id'],
                    status,
                ),
                extra=log_extra,
            )
            return subprocess_uid

        Subprocess = self.get_subprocess(cmd, params)
        uid = uuid.uuid4().hex
        if issubclass(Subprocess, BaseSubprocess):
            sub = Subprocess(uid, cmd, params=params, env=env, success_codes=success_codes)
        else:
            sub = Subprocess(uid, params=params)
        sub.run()
        if sub.error:
            cmd_logger.info('command execution failed: {}: {}'.format(sub, sub.error), extra=log_extra)
        else:
            cmd_logger.info('command execution started successfully: {}'.format(sub), extra=log_extra)

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
        sub = self.subprocesses[uid]
        cmd_logger.info(
            'terminating command {}, pid: {}'.format(uid, sub.process.pid),
            extra=sub.log_extra,
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

    def try_find_nonfailed_subprocess_and_status(self, job_id, task_id):
        if not task_id:
            return None, None

        for uid, subprocess in self.subprocesses.iteritems():
            status = subprocess.status()
            # it is enough to check only task_id and ignore job_id
            if status.get('task_id') == task_id and (status['exit_code'] == 0 or status['progress'] < 1.0):
                return uid, status

        s = Session()
        # it is enough to check only task_id and ignore job_id
        query_start_ts = time.time()
        command = s.query(Command).filter(Command.task_id == task_id).order_by(desc(Command.start_ts)).first()
        logger.debug('find_nonfailed_subprocess query completed in {:.3f} seconds'.format(time.time() - query_start_ts))
        if not command:
            return None, None

        if command.progress != 1.0:
            cmd_logger.warning(
                "Command {} ({}) was interrupted by minion restart, but not marked as finished".format(
                    command.uid,
                    command.command,
                ),
                extra={
                    'job_id': job_id,
                    'task_id': task_id,
                },
            )
            return None, None

        if command.exit_code == 0:
            return command.uid, command.status

        return None, None


manager = SubprocessManager()
