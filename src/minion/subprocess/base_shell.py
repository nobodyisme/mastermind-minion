import copy
import json
import os
import shlex
import time
import uuid

from tornado.ioloop import IOLoop
from tornado.process import Subprocess

from minion.config import config
from minion.db import Session
import minion.db.commands
from minion.logger import cmd_logger
from minion.subprocess.base import BaseCommand
from minion.watchers.base import ProgressWatcher


class BaseSubprocess(BaseCommand):

    POST_PROCESSORS = ()

    def __init__(self, uid, cmd, params=None, env=None, success_codes=None, io_loop=IOLoop.instance()):
        super(BaseSubprocess, self).__init__(
            uid,
            params=params,
            io_loop=io_loop
        )
        self.cmd = self._prepare_command(cmd)
        self.cmd_str = ' '.join(self.cmd)
        self.env = copy.copy(os.environ)
        config_env = self._get_config_env_vars(cmd[0])
        if config_env:
            self.env.update(config_env)
        if env:
            self.env.update(env)
        self.command = None
        self.process = None
        self.watcher = None

        self.artifacts = {}

        self.success_codes = success_codes

    def _prepare_command(self, cmd):
        return cmd

    def _get_config_env_vars(self, command):
        env_dict = config.get('{command}_env'.format(command=command), {})

        # NOTE: ConfigParser.items lowercases option names
        # NOTE: Section's dict stores the section name itself under '__name__'
        # key, it should be skipped
        return dict(
            (env_var_name.upper(), env_var_value)
            for env_var_name, env_var_value in env_dict.iteritems()
            if not env_var_name.startswith('__')
        )

    def run(self):

        self.start_ts = int(time.time())

        self.process = Subprocess(self.cmd,
                                  stdout=Subprocess.STREAM,
                                  stderr=Subprocess.STREAM,
                                  env=self.env,
                                  io_loop=self.io_loop)

        # create db record
        s = Session()
        s.begin()
        command = minion.db.commands.Command(
            uid=self.uid,
            pid=self.process.pid,
            command=self.cmd_str,
            start_ts=int(time.time()),
            task_id=self.params.get('task_id'),
            job_id=self.params.get('job_id'),
        )

        s.update_ts = int(time.time())
        s.add(command)
        s.commit()

        self.watcher = self.watch(command)

        self.command = command

    def watch(self, command):
        # TODO: watcher should not know about success_codes
        return self.watcher_base(self, self.process, success_codes=self.success_codes)

    watcher_base = ProgressWatcher

    @property
    def pid(self):
        assert self.process
        return self.process.pid

    def status(self):
        assert self.process
        assert self.watcher

        return {
            'progress': self.watcher.progress,
            'exit_code': self.watcher.exit_code,
            'exit_message': self.watcher.exit_message,
            'command_code': self.command_code,
            'start_ts': self.start_ts,
            'finish_ts': self.finish_ts,
            'output': self.watcher.get_stdout(),
            'error_output': self.watcher.get_stderr(),
            'pid': self.pid,
            'command': self.cmd_str,
            'task_id': self.params.get('task_id'),
            'job_id': self.params.get('job_id'),
            'artifacts': self.artifacts,
        }

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
        Subprocess(shlex.split(cmd_str),
                   stdout=Subprocess.STREAM,
                   stderr=Subprocess.STREAM,
                   io_loop=self.io_loop)

    def collect_artifacts(self):
        pass

    # watcher callbacks

    def on_update_progress(self):
        s = Session()
        s.begin()
        try:
            command = self.command
            command.progress = self.watcher.progress
            command.exit_code = self.watcher.exit_code
            command.command_code = self.command_code
            command.stdout = self.watcher.get_stdout()
            command.stderr = self.watcher.get_stderr()
            if command.exit_code is not None:
                command.artifacts = json.dumps(self.artifacts)
                command.finish_ts = self.finish_ts
            s.add(command)
            s.commit()
        except Exception:
            cmd_logger.exception('Failed to update db command', extra=self.log_extra)
            s.rollback()

    @property
    def command_code(self):
        return self.watcher.exit_code

    def _apply_postprocessors(self):
        if self.watcher.exit_code == 0:
            return True

        cmd_logger.info(
            'Checking success codes: command code {}, success codes {}'.format(
                self.command_code,
                self.success_codes,
            ),
            extra=self.log_extra,
        )

        if self.success_codes and self.command_code in self.success_codes:
            return True

        return False

    def on_command_completed(self):

        self.finish_ts = int(time.time())
        self.artifacts = self.collect_artifacts()

        self.on_update_progress()

        if not self._apply_postprocessors():
            # TODO: add status codes
            cmd_logger.info('Command failed, no post processors will be applied', extra=self.log_extra)
            return

        for post_processor in self.POST_PROCESSORS:
            params_supplied = all(
                param in self.params
                for param in post_processor.REQUIRED_PARAMS
            )
            if params_supplied:
                # TODO: replace by required params? possibly not
                cmd_logger.info('Running post processor {}'.format(post_processor.__name__), extra=self.log_extra)
                uid = uuid.uuid4().hex
                command = post_processor(uid, params=self.params)
                try:
                    # NOTE: when running as a post processor command is not
                    # dumped to database, therefore 'execute' method is called
                    # instead of 'run'
                    command.execute()
                except:
                    cmd_logger.exception(
                        'Post processor {} failed, skipped'.format(
                            post_processor.__name__
                        ),
                        extra=self.log_extra,
                    )
                    continue

    def __str__(self):
        return '<{}: {}, pid {}>'.format(type(self).__name__, self.cmd_str, self.pid)
