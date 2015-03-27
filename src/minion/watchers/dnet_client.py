import json

from minion.logger import logger
from minion.watchers.base import ProgressWatcher


class DnetClientWatcher(ProgressWatcher):

    def is_success(self):
        output = ''.join(self.output)
        try:
            data = json.loads(output)
        except Exception as e:
            logger.error('pid {0}: failed to parse output json: {1}'.format(
                self.subprocess.pid, output))
            return False

        if not 'error' in data:
            logger.error('pid {0}: no "error" key in response data')
            return False
        if not 'code' in data['error']:
            logger.error('pid {0}: no "code" key in response error data')
            return False

        logger.info('pid {0}: operation error code {1}'.format(
            self.subprocess.pid, data['error']['code']))

        return data['error']['code'] in self.success_codes
