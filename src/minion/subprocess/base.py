import json
import time

from tornado import gen
from tornado.ioloop import IOLoop

from minion.db import Session
import minion.db.commands
from minion.logger import cmd_logger


class BaseCommand(object):

    # TODO: make it a proprty to throw not implemented exception?
    COMMAND = 'command'
    REQUIRED_PARAMS = ()

    def __init__(self, uid, params=None, io_loop=IOLoop.instance()):
        self.uid = uid
        self.params = params or {}
        self.io_loop = io_loop

        self.error = None
        self.start_ts = None
        self.finish_ts = None

        self.artifacts = {}

        self.log_extra = {
            'task_id': self.params.get('task_id'),
            'job_id': self.params.get('job_id'),
        }

        for param_name in self.REQUIRED_PARAMS:
            if param_name not in params:
                raise ValueError('Parameter "{}" is required'.format(param_name))

    @gen.coroutine
    def execute(self):
        raise NotImplemented("Command should implement 'execute' method")

    @gen.coroutine
    def run(self):

        self.start_ts = int(time.time())

        # create db record
        s = Session()
        s.begin()
        command = minion.db.commands.Command(
            uid=self.uid,
            pid=None,
            command=self.COMMAND,
            start_ts=self.start_ts,
            task_id=self.params.get('task_id'),
            job_id=self.params.get('job_id'),
        )

        s.update_ts = int(time.time())
        s.add(command)
        s.commit()

        try:
            yield self.execute()
        except Exception as e:
            cmd_logger.exception('Command execution failed', extra=self.log_extra)
            self.error = e

        self.finish_ts = int(time.time())

        s.begin()
        try:
            command.progress = 1.0
            command.exit_code = 1 if self.error else 0
            command.command_code = 1 if self.error else 0
            command.finish_ts = self.finish_ts
            command.artifacts = json.dumps(self.artifacts)
            s.add(command)
            s.commit()
        except Exception as e:
            cmd_logger.exception('Failed to update db command', extra=self.log_extra)
            s.rollback()

    def status(self):
        return {
            'progress': 1.0,
            'exit_code': 1 if self.error else 0,
            'exit_message': str(self.error) if self.error else 'Success',
            'command_code': 1 if self.error else 0,
            'start_ts': self.start_ts,
            'finish_ts': self.finish_ts,
            'output': '',
            'error_output': '',
            'pid': None,
            'task_id': self.params.get('task_id'),
            'job_id': self.params.get('job_id'),
            'command': self.COMMAND,
            'artifacts': self.artifacts,
        }

    def __str__(self):
        return '<{}: {}>'.format(type(self).__name__, self.COMMAND)
