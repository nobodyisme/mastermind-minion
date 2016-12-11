import json

from minion.logger import logger
from minion.subprocess.base_shell import BaseSubprocess


class DnetRecoverySubprocess(BaseSubprocess):

    COMMAND = 'dnet_recovery'
    REQUIRED_PARAMS = ('node_backend',)

    def __init__(self, *args, **kwargs):
        super(DnetRecoverySubprocess, self).__init__(*args, **kwargs)
        # NOTE: temporary backward compatibility
        self.commands_stats = {}

    def status(self):
        # NOTE: temporary backward compatibility
        res = super(DnetRecoverySubprocess, self).status()
        res['commands_statuses'] = self.commands_stats
        return res

    def collect_artifacts(self):

        commands_stats_path = self.params.get('commands_stats_path')
        if not commands_stats_path:
            logger.info('Commands stats path was not supplied')
            return {}

        logger.info('Parsing commands stats path: {}'.format(commands_stats_path))

        commands_stats = {}

        try:
            with open(commands_stats_path, 'rb') as f:
                commands_stats = json.load(f).get('commands', {})
        except Exception:
            logger.exception(
                'Failed to parse commands stats file {}'.format(commands_stats_path)
            )

        # NOTE: temporary backward compatibility
        self.commands_stats = commands_stats

        return commands_stats
