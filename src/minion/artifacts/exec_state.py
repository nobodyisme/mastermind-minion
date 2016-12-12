import json

from minion.logger import logger


class ExecStateArtifactsMixin(object):

    def collect_artifacts(self):

        exec_state_path = self.params.get('exec_state_path')
        if not exec_state_path:
            logger.info('Exec state path was not supplied')
            return {}

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
