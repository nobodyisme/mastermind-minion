import datetime
import time

from minion.logger import logger


class ProgressWatcher(object):
    def __init__(self, subprocess, success_codes=None):
        self.progress = 0.0
        self.start_ts = int(time.time())
        self.finish_ts = None
        self.success_codes = success_codes or []

        subprocess.stdout.read_until_close(self.feed,
                                           streaming_callback=self.feed)
        subprocess.stderr.read_until_close(self.feed_error,
                                           streaming_callback=self.feed_error)

        subprocess.stdout.set_close_callback(self.ensure_exit_cb)
        subprocess.set_exit_callback(self.exit_cb)

        self.subprocess = subprocess

        self.success_cb = None
        self.exit_cb_timeout = None

        self.exit = False
        self.exit_code = None
        self.command_code = None

        self.output = []
        self.error_output = []

    def feed(self, s):
        self.output.append(s)

    def feed_error(self, s):
        self.error_output.append(s)

    def exit_cb(self, code):
        logger.info('pid {0}: exit callback'.format(self.subprocess.pid))
        self.exit = True
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
        if self.success_cb:
            if self.exit_code == 0 or self.command_code in self.success_codes:
                logger.info('pid {0}: executing success callback'.format(
                    self.subprocess.pid))
                self.success_cb()

    def is_success(self):
        success = (self.exit_code == 0 or
                   (self.success_codes and self.exit_code in self.success_codes or False)
                  )
        logger.info('pid {0}: exit code {1}, considered success: {2}'.format(
            self.subprocess.pid, self.exit_code, success))
        return success

    def set_command_code(self):
        self.command_code = self.exit_code

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

        logger.info('pid {0}: setting false exit callback'.format(
            self.subprocess.pid))
        self.exit_cb_timeout = self.subprocess.io_loop.add_timeout(
            datetime.timedelta(seconds=10), set_false_exit_code)

    @property
    def exit_message(self):
        if self.exit_code is None:
            return ''
        return self.exit_messages.get(self.exit_code, 'Unknown')

    @property
    def exit_messages(self):
        return {
            0: 'Success',
            999: 'Child\'s stdout was closed, but exit code was not received'
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
