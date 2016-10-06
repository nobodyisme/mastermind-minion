import shlex

from sqlalchemy import Column, Integer, Float, String

from minion.db import Base
from minion import subprocess


class Command(Base):
    __tablename__ = 'commands'

    uid = Column(String, primary_key=True)
    pid = Column(Integer, nullable=False)

    command = Column(String, nullable=False)
    progress = Column(Float, nullable=False, default=0.0)

    exit_code = Column(Integer)
    command_code = Column(Integer)

    start_ts = Column(Integer, nullable=False)
    finish_ts = Column(Integer)

    task_id = Column(String)
    group_id = Column(String)
    node = Column(String)
    node_backend = Column(String)

    stdout = Column(String, default='')
    stderr = Column(String, default='')

    def status(self):
        cmd = (shlex.split(self.command.encode('utf-8'))
               if isinstance(self.command, basestring) else
               self.command)
        Subprocess = subprocess.subprocess_factory(cmd)
        Watcher = Subprocess.watcher_base
        return {
            'pid': self.pid,
            'command': self.command,
            'task_id': self.task_id,
            'progress': self.progress,
            'exit_code': self.exit_code,
            'exit_message': Watcher.exit_messages.get(self.exit_code, 'Unknown'),
            'command_code': self.command_code,
            'start_ts': self.start_ts,
            'finish_ts': self.finish_ts,
            'output': self.stdout,
            'error_output': self.stderr,
        }
