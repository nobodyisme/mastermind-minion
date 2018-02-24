from tornado import gen

from minion.subprocess.base import BaseCommand


class PingCommand(BaseCommand):

    COMMAND = 'ping'

    @gen.coroutine
    def execute(self):
        if 'sleep' in self.params:
            yield gen.sleep(float(self.params['sleep']))
        raise gen.Return('ok')
