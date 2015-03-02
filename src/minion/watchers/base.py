import datetime
import time


class ProgressWatcher(object):
    def __init__(self, subprocess):
        self.progress = 0.0
        self.start_ts = int(time.time())
        self.finish_ts = None

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

        self.output = []
        self.error_output = []

    def feed(self, s):
        self.output.append(s)

    def feed_error(self, s):
        self.error_output.append(s)

    def exit_cb(self, code):
        self.exit = True
        if self.exit_cb_timeout:
            self.subprocess.io_loop.remove_timeout(self.exit_cb_timeout)
            self.exit_cb_timeout = None
        self.exit_code = code
        self.progress = 1.0
        self.finish_ts = int(time.time())

        if self.success_cb and self.exit_code == 0:
            self.success_cb()

    def ensure_exit_cb(self):

        def set_false_exit_code():
            self.exit_cb_timeout = None
            if self.exit:
                return
            self.exit = True
            self.exit_code = 999
            self.progress = 1.0
            self.finish_ts = int(time.time())

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
            'start_ts': self.start_ts,
            'finish_ts': self.finish_ts,
            'output': ''.join(self.output),
            'error_output': ''.join(self.error_output)
        }

    def on_success(self, cb):
        self.success_cb = cb
