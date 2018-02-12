import json

from minion.logger import cmd_logger
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
            cmd_logger.info('Commands stats path was not supplied', extra=self.log_extra)
            return {}

        cmd_logger.info('Parsing commands stats path: {}'.format(commands_stats_path), extra=self.log_extra)

        job_stats = {}

        try:
            with open(commands_stats_path, 'rb') as f:
                job_stats = json.load(f)
        except Exception:
            cmd_logger.exception(
                'Failed to parse commands stats file {}'.format(commands_stats_path),
                extra=self.log_extra,
            )

        parsed_stats = self._parse_job_stats(job_stats)

        # NOTE: temporary backward compatibility
        self.commands_stats = parsed_stats
        return parsed_stats

    def _parse_job_stats(self, job_stats):
        op_statuses_count = {}

        commands_stats = job_stats.get('commands', {})
        for operation_status, count in commands_stats.iteritems():
            operation, status = operation_status.split('.', 1)
            statuses_count = op_statuses_count.setdefault(operation, {})
            statuses_count.setdefault(status, 0)
            statuses_count[status] += count

        unavailable_groups = job_stats.get('unavailable_groups')
        if unavailable_groups is not None:
            # we imitate artifacts format for such errors
            op_statuses_count.setdefault('unavailable_groups', {})["-6"] = unavailable_groups

        cmd_logger.info('Parsed command statuses: {}'.format(op_statuses_count), extra=self.log_extra)

        return op_statuses_count
