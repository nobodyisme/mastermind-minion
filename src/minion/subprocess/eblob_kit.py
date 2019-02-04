"""Support for 'eblob_kit' utility.

    1. Fix command.

To start job:

$ curl -v -XPOST -d command=eblob_kit  -d destination=here -d path=/some/path/elliptics/eblob/data
 -d subcommand=fix -sS 0.0.0.0:8081/command/start/

where:

    destination=<path> - local path, where `fix` results should be written.
    path=<path> - source blob path.
    json-file=<path> (optional) - save extendend job stats to file @ path (so called artifact).

"""
import json

from minion.logger import cmd_logger
from minion.subprocess.base_shell import BaseSubprocess


OUTPUT_PARAM = 'json-file'
LOG_PARAM = 'log-file'


class ResultFields(object):
    """Fields of artifacts dictionary."""

    SUB_CMD = 'subcommand'
    OUTPUT = 'report'
    ERR_MSG = 'error_message'


class EblobKitSubprocess(BaseSubprocess):
    """eblob_kit command launcher."""

    COMMAND = 'eblob_kit'

    REQUIRED_PARAMS = (
        ResultFields.SUB_CMD,
    )

    def _prepare_command(self, cmd):
        # NOTE: self.params are already checked at this point.
        cmd = super(EblobKitSubprocess, self)._prepare_command(cmd)

        if OUTPUT_PARAM in self.params:
            cmd.extend(['--json-file', self.params[OUTPUT_PARAM]])

        if LOG_PARAM in self.params:
            cmd.extend(['--log-file', self.params[LOG_PARAM]])

        subcommand = self.params[ResultFields.SUB_CMD]
        if subcommand == 'fix':
            self._check_fix_params()

            cmd.extend([
                'fix',
                self.params['path'],
                '--destination',
                self.params['destination'],
            ])
        else:
            raise NotImplemented("subcommand {} not yet supported, stay tuned".format(subcommand))

        return cmd

    def _check_fix_params(self):
        if 'path' not in self.params:
            raise ValueError(
                "'path' parameter must be provided "
                "for 'fix' subcommand"
            )

        if 'destination' not in self.params:
            raise ValueError(
                "'destination' parameter must be provided "
                "for 'fix' subcommand"
            )

    def collect_artifacts(self):
        """Collect job artifacts either from stdout or file."""
        result = {
            ResultFields.SUB_CMD: self.params[ResultFields.SUB_CMD]
        }

        if OUTPUT_PARAM in self.params:
            file_name = self.params[OUTPUT_PARAM]
            try:
                cmd_logger.info('Collecting artifacts from: {}'.format(file_name), extra=self.log_extra)

                with open(file_name, 'rb') as f:
                    result[ResultFields.OUTPUT] = json.load(f)
                    return result

            except Exception as e:
                cmd_logger.error(
                    'Failed to load eblob_kit artifact: error {}, file name "{}"'.format(e, file_name),
                    extra=self.log_extra,
                )
                result[ResultFields.ERR_MSG] = 'error: {}, file {}'.format(e, file_name)

                return result

        if self.watcher is None:
            result[ResultFields.ERR_MSG] = 'no watcher and no resulting json'
            return result

        cmd_logger.info('Collecting artifacts from stdout', extra=self.log_extra)

        try:
            output = self.watcher.get_stdout()
            result[ResultFields.OUTPUT] = json.loads(output)

            return result
        except Exception as e:
            cmd_logger.error('Failed to parse stdout as json: error {}, output "{}"'.format(e, output))

            result[ResultFields.ERR_MSG] = 'failed to load json from stdout {}'.format(e)

            return result
