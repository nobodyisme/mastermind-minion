import shlex

from tornado.ioloop import IOLoop

from minion.config import EllipticsConfig
from minion.logger import logger
from minion.subprocess.base import BaseSubprocess
from minion.watchers.dnet_client import DnetClientWatcher


class DnetClientCmd(BaseSubprocess):

    watcher_base = DnetClientWatcher

    def __init__(self, uid, cmd=None, params=None, env=None, success_codes=None, io_loop=IOLoop.instance()):
        cmd = self._make_command(params)
        logger.info('Constructed command: {}'.format(cmd))
        super(DnetClientCmd, self).__init__(
            uid,
            cmd,
            params=params,
            env=env,
            success_codes=success_codes,
            io_loop=io_loop
        )

    def _make_command(self, params):
        cmd_tpl = params['cmd_tpl']
        if 'backend_id' not in params:
            # need to get backend id from config by group id
            config_path = params['config_path']
            logger.info(
                'backend id not found in request params, '
                'getting backend id from elliptics config {config}, '
                'group {group}'.format(
                    config=config_path,
                    group=params['group'],
                )
            )
            config = EllipticsConfig(params['config_path'])
            params['backend_id'] = config.get_group_backend(int(params['group']))

        return shlex.split(cmd_tpl.format(**params))
