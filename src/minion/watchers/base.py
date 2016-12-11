import datetime
import time

from minion.logger import logger


class ProgressWatcher(object):

    OUTPUT_WINDOW_SIZE = 1024 ** 2  # 1Mb
    OUTPUT_UPDATE_PERIOD = 60

    def __init__(self, command, subprocess, success_codes=None):
        self.progress = 0.0
        self.command = command

        # timestamp of the last time when command was dumped to db
        # (required to limit the rate of db dumps when stdout and stderr is updated)
        self.update_ts = int(time.time())

        subprocess.stdout.read_until_close(self._feed,
                                           streaming_callback=self._feed)
        subprocess.stderr.read_until_close(self._feed_error,
                                           streaming_callback=self._feed_error)

        subprocess.stdout.set_close_callback(self._ensure_exit_cb)
        subprocess.set_exit_callback(self._exit_cb)

        self.subprocess = subprocess

        self._exit = False
        self._exit_cb_timeout = None
        self.exit_code = None
        self.command_code = None

        self.output = memoryview(bytearray(' ' * self.OUTPUT_WINDOW_SIZE))
        self.output_size = 0
        self.error_output = memoryview(bytearray(' ' * self.OUTPUT_WINDOW_SIZE))
        self.error_output_size = 0

    def _append_chunk_to_window(self, window, chunk):
        if len(chunk) == 0:
            return
        if len(chunk) >= self.OUTPUT_WINDOW_SIZE:
            window[:] = chunk[-self.OUTPUT_WINDOW_SIZE:]
            return
        chars_left = self.OUTPUT_WINDOW_SIZE - len(chunk)
        window[:chars_left] = window[-chars_left:]
        window[-len(chunk):] = chunk

    def _update_db_dump(self):
        if time.time() - self.update_ts >= self.OUTPUT_UPDATE_PERIOD:
            self.command.on_update_progress()
            self.update_ts = int(time.time())

    def _feed(self, s):
        self._append_chunk_to_window(self.output, s)
        self.output_size = min(self.OUTPUT_WINDOW_SIZE, self.output_size + len(s))
        self._update_db_dump()

    def _feed_error(self, s):
        self._append_chunk_to_window(self.error_output, s)
        self.error_output_size = min(self.OUTPUT_WINDOW_SIZE, self.error_output_size + len(s))
        self._update_db_dump()

    def _exit_cb(self, code):
        self._exit = True
        logger.info('pid {0}: exit callback'.format(self.subprocess.pid))
        if self._exit_cb_timeout:
            logger.info('pid {0}: removing false exit callback'.format(
                self.subprocess.pid))
            self.subprocess.io_loop.remove_timeout(self._exit_cb_timeout)
            self._exit_cb_timeout = None
        self.exit_code = code
        self.progress = 1.0
        self.set_command_code()

        logger.info('pid {0}: exit code {1}, command code {2}'.format(
            self.subprocess.pid, self.exit_code, self.command_code))

        self.command.on_command_completed()

    def _ensure_exit_cb(self):

        def set_false_exit_code():
            logger.warn('pid {0}: executing false exit callback'.format(
                self.subprocess.pid))
            self._exit_cb_timeout = None
            if self._exit:
                return
            logger.warn('pid {0}: setting exit code to 999'.format(
                self.subprocess.pid))
            self._exit = True
            self.exit_code = 999
            self.progress = 1.0
            self.set_command_code()

            self.command.on_command_completed()

        if self._exit:
            return
        logger.info('pid {0}: setting false exit callback'.format(
            self.subprocess.pid))
        self._exit_cb_timeout = self.subprocess.io_loop.add_timeout(
            datetime.timedelta(seconds=10), set_false_exit_code)

    def set_command_code(self):
        self.command_code = self.exit_code

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

    def get_stdout(self):
        if self.output_size == 0:
            return ''
        return self.output[-self.output_size:].tobytes()

    def get_stderr(self):
        if self.error_output_size == 0:
            return ''
        return self.error_output[-self.error_output_size:].tobytes()
