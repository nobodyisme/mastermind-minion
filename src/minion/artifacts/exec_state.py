import json
import os.path

from minion.logger import logger


class ExecStateArtifactsMixin(object):

    def collect_artifacts(self):

        tmp_dir = None
        for i, cmd_part in enumerate(self.cmd):
            if cmd_part in ('--tmp', '-t'):
                tmp_dir = self.cmd[i + 1]
                break

        if not tmp_dir:
            logger.info('Failed to determine tmp directory')
            return {}

        exec_state_path = os.path.join(tmp_dir, 'exec_state')

        logger.info('Parsing exec state: {}'.format(exec_state_path))

        exec_state = {}

        try:
            with open(exec_state, 'rb') as f:
                exec_state = json.load(f).get('status', {})
        except Exception:
            logger.exception(
                'Failed to parse exec state file {}'.format(exec_state_path)
            )
            pass

        return exec_state
