import json
import re
import shlex

from minion.config import EllipticsConfig
from minion.logger import cmd_logger
from minion.subprocess.base_shell import BaseSubprocess
from minion.subprocess.create_file_marker import CreateFileMarkerCommand
from minion.subprocess.remove_group_file import RemoveGroupFileCommand
from minion.subprocess.move_path import MovePathCommand
from minion.subprocess.lock_backend import LockBackendCommand
from minion.subprocess.unlock_backend import UnlockBackendCommand


class DnetClientSubprocess(BaseSubprocess):

    COMMAND = 'dnet_client'
    POST_PROCESSORS = (
        CreateFileMarkerCommand,
        RemoveGroupFileCommand,
        MovePathCommand,
        LockBackendCommand,
        UnlockBackendCommand,
    )

    def __init__(self, *args, **kwargs):
        super(DnetClientSubprocess, self).__init__(*args, **kwargs)
        self._command_code = None

    PLACEHOLDERS_TPL = re.compile('{((?:[^}])+)}')

    def _prepare_command(self, cmd):
        cmd_str = ' '.join(cmd)

        if 'cmd_tpl' in self.params:
            # TODO: remove this backward-compatibility hack
            cmd_str = self.params['cmd_tpl']

        missing_parameters = self.PLACEHOLDERS_TPL.findall(cmd_str)

        for param in missing_parameters:
            if param in self.params:
                continue
            elif param == 'backend_id':
                if 'group' not in self.params:
                    raise ValueError('Parameter "group" is required')
                config_path = self.params['config_path']
                cmd_logger.info(
                    'backend id not found in request params, '
                    'getting backend id from elliptics config {config}, '
                    'group {group}'.format(
                        config=config_path,
                        group=self.params['group'],
                    ),
                    extra=self.log_extra,
                )
                config = EllipticsConfig(self.params['config_path'])
                self.params['backend_id'] = config.get_group_backend(int(self.params['group']))
            else:
                raise ValueError('Cannot process command: unknown parameter "{}"'.format(param))

        cmd_str = cmd_str.format(**self.params)
        cmd_logger.info('Prepared command: {}'.format(cmd_str), extra=self.log_extra)
        return shlex.split(cmd_str)

    def _parse_command_code(self):
        if self.watcher.exit_code == 0:
            return self.watcher.exit_code

        output = self.watcher.get_stdout()
        try:
            data = json.loads(output)
        except Exception:
            cmd_logger.error(
                'pid {}: failed to parse output json: {}'.format(
                    self.pid,
                    output,
                ),
                extra=self.log_extra,
            )
            return self.watcher.exit_code

        if 'error' not in data:
            cmd_logger.error(
                'pid {}: no "error" key in response data'.format(self.pid),
                extra=self.log_extra,
            )
            return self.watcher.exit_code
        if 'code' not in data['error']:
            cmd_logger.error(
                'pid {}: no "code" key in response error data'.format(self.pid),
                extra=self.log_extra,
            )
            return self.watcher.exit_code

        cmd_logger.info(
            'pid {}: operation error code {}'.format(
                self.pid,
                data['error']['code'],
            ),
            extra=self.log_extra,
        )

        return data['error']['code']

    @property
    def command_code(self):
        if self.watcher.exit_code is None:
            return None

        if self._command_code is None:
            self._command_code = self._parse_command_code()
        return self._command_code
