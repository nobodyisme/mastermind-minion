import json

from minion.logger import logger
from minion.subprocess.dnet_ioclient import DnetIoclientSubprocess
from minion.watchers.dnet_recovery import DnetRecoveryWatcher


class DnetRecoverySubprocess(DnetIoclientSubprocess):

    watcher_base = DnetRecoveryWatcher

    def run(self):
        super(DnetRecoverySubprocess, self).run()
        self.watcher.on_finish(self.process_commands_stats)

    def process_commands_stats(self):

        commands_stats_path = self.params.get('commands_stats_path')
        if commands_stats_path:
            logger.info('Parsing commands stats path: {}'.format(commands_stats_path))

            try:
                with open(commands_stats_path, 'rb') as f:
                    commands_stats = json.load(f).get('commands', {})

                self._set_commands_stats(commands_stats)

            except Exception as e:
                logger.exception(
                    'Failed to parse commands stats file {}'.format(commands_stats_path)
                )
                pass
        else:
            logger.info('Commands stats path was not supplied')

    def _set_commands_stats(self, commands_stats):
        op_statuses_count = {}
        for operation_status, count in commands_stats.iteritems():
            operation, status = operation_status.split('.', 1)
            statuses_count = op_statuses_count.setdefault(operation, {})
            statuses_count.setdefault(status, 0)
            statuses_count[status] += count

        logger.info('Parsed command statuses: {}'.format(statuses_count))

        self.watcher.set_commands_stats(op_statuses_count)
