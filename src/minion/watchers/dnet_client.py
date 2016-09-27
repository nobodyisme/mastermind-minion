import json

from minion.logger import logger
from minion.watchers.base import ProgressWatcher


class DnetClientWatcher(ProgressWatcher):

    def _command_code(self):
        if self.exit_code == 0:
            return self.exit_code

        output = ''.join(self.output)
        try:
            data = json.loads(output)
        except Exception as e:
            logger.error('pid {0}: failed to parse output json: {1}'.format(
                self.subprocess.pid, output))
            return self.exit_code

        if not 'error' in data:
            logger.error('pid {0}: no "error" key in response data'.format(
                self.subprocess.pid))
            return self.exit_code
        if not 'code' in data['error']:
            logger.error('pid {0}: no "code" key in response error data'.format(
                self.subprocess.pid))
            return self.exit_code

        logger.info('pid {0}: operation error code {1}'.format(
            self.subprocess.pid, data['error']['code']))

        return data['error']['code']

    def set_command_code(self):
        self.command_code = self._command_code()
