from tornado.process import Subprocess


class ProgressWatcher(object):
    def __init__(self, subprocess):
        self.progress = 0.0

        subprocess.stdout.read_until_close(self.feed,
                                           streaming_callback=self.feed)
        subprocess.set_exit_callback(self.exit)
        self.subprocess = subprocess

        self.exit = False
        self.exit_code = None

    def feed(self):
        raise NotImplemented

    def exit(self, code):
        self.exit = True
        self.exit_code = code
        self.progress = 1.0

    @property
    def exit_message(self):
        if self.exit_code is None:
            return ''
        return self.exit_messages.get(self.exit_code, 'Unknown')

    @property
    def exit_messages(self):
        return {
            0: 'Success',
        }

    def status(self):
        return {
            'pid': self.subprocess.pid,
            'progress': self.progress,
            'exit_code': self.exit_code,
            'exit_message': self.exit_message,
        }
