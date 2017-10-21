import json

from minion.logger import cmd_logger


class DnetClientArtifactsMixin(object):

    def collect_artifacts(self):

        result = {}

        cmd_logger.info('Parsing output json for artifacts', extra=self.log_extra)
        output = self.watcher.get_stdout()
        try:
            result = json.loads(output)
        except Exception as e:
            result = {
                'error': {
                    'message': 'failed to parse dnet_client output, see logs',
                    'code': -666,
                }
            }
            cmd_logger.error(
                'Failed to parse dnet_client output: error {}, stdout "{}"'.format(e, output),
                extra=self.log_extra,
            )
            pass

        if 'error' not in result:
            # do not save artifacts for successfully executed commands
            return {}

        return result
