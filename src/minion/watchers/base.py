import datetime
import time

from minion.db import Session
from minion.logger import logger


class ProgressWatcher(object):
    def __init__(self, subprocess, command, success_codes=None):
        self.progress = 0.0
        self.start_ts = int(time.time())
        self.finish_ts = None
        self.success_codes = success_codes or []

        self.command = command

        subprocess.stdout.read_until_close(self.feed,
                                           streaming_callback=self.feed)
        subprocess.stderr.read_until_close(self.feed_error,
                                           streaming_callback=self.feed_error)

        subprocess.stdout.set_close_callback(self.ensure_exit_cb)
        subprocess.set_exit_callback(self.exit_cb)

        self.subprocess = subprocess

        self.success_cb = None
        self.finish_cb = None
        self.exit_cb_timeout = None

        self.exit = False
        self.exit_code = None
        self.command_code = None

        self.output = []
        self.error_output = []

    def feed(self, s):
        prev_progress = self.progress
        self.output.append(s)
        if abs(self.progress - prev_progress) > 0.01:
            try:
                s = Session()
                s.begin()
                self.command.progress = self.progress
                s.add(self.command)
                s.commit()
            except Exception as e:
                logger.exception('pid {0}: failed to update db command'.format(
                    self.subprocess.pid))
                pass

    def feed_error(self, s):
        self.error_output.append(s)

    def exit_cb(self, code):
        self.exit = True
        logger.info('pid {0}: exit callback'.format(self.subprocess.pid))
        if self.exit_cb_timeout:
            logger.info('pid {0}: removing false exit callback'.format(
                self.subprocess.pid))
            self.subprocess.io_loop.remove_timeout(self.exit_cb_timeout)
            self.exit_cb_timeout = None
        self.exit_code = code
        self.progress = 1.0
        self.finish_ts = int(time.time())
        self.set_command_code()

        logger.info('pid {0}: exit code {1}, command code {2}'.format(
            self.subprocess.pid, self.exit_code, self.command_code))

        self.update_db_command()

        if self.success_cb:
            if self.exit_code == 0 or self.command_code in self.success_codes:
                logger.info('pid {0}: executing success callback'.format(
                    self.subprocess.pid))
                self.success_cb()

        if self.finish_cb:
            logger.info('pid {0}: executing finish callback'.format(
                self.subprocess.pid))
            self.finish_cb()


    def set_command_code(self):
        self.command_code = self.exit_code

    def update_db_command(self):
        s = Session()
        s.begin()
        try:
            command = self.command
            command.progress = self.progress
            command.exit_code = self.exit_code
            command.command_code = self.command_code
            command.finish_ts = self.finish_ts
            s.add(command)
            s.commit()
        except Exception as e:
            logger.exception('Failed to update db command: {0}'.format(e))
            s.rollback()

    def ensure_exit_cb(self):

        def set_false_exit_code():
            logger.warn('pid {0}: executing false exit callback'.format(
                self.subprocess.pid))
            self.exit_cb_timeout = None
            if self.exit:
                return
            logger.warn('pid {0}: setting exit code to 999'.format(
                self.subprocess.pid))
            self.exit = True
            self.exit_code = 999
            self.progress = 1.0
            self.finish_ts = int(time.time())

            self.update_db_command()

        if self.exit:
            return
        logger.info('pid {0}: setting false exit callback'.format(
            self.subprocess.pid))
        self.exit_cb_timeout = self.subprocess.io_loop.add_timeout(
            datetime.timedelta(seconds=10), set_false_exit_code)

    @property
    def exit_message(self):
        if self.exit_code is None:
            return ''
        return self.exit_messages.get(self.exit_code, 'Unknown')

    exit_messages = {
        0: 'Success',
        666: 'Process was interrupted by minion restart',
        999: 'Child\'s stdout was closed, but exit code was not received',
    }

    def status(self):
        return {
            'progress': self.progress,
            'exit_code': self.exit_code,
            'exit_message': self.exit_message,
            'command_code': self.command_code,
            'start_ts': self.start_ts,
            'finish_ts': self.finish_ts,
            'output': ''.join(self.output),
            'error_output': ''.join(self.error_output)
        }

    def on_success(self, cb):
        self.success_cb = cb

    def on_finish(self, cb):
        self.finish_cb = cb
