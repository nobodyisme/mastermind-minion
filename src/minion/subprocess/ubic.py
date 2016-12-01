import os
import os.path

from tornado.ioloop import IOLoop

from minion.logger import logger
from minion.subprocess.base import BaseSubprocess


class UbicSubprocess(BaseSubprocess):

    def __init__(self, uid, cmd, params=None, env=None, success_codes=None, io_loop=IOLoop.instance()):
        super(UbicSubprocess, self).__init__(
            uid,
            cmd,
            params=params,
            env=env,
            success_codes=success_codes,
            io_loop=io_loop,
        )
        if 'node' not in params and 'node_backend' not in params:
            raise ValueError('Parameter "node" or "node_backend" is required')
        self.node = params.get('node')
        self.node_backend = params.get('node_backend')

    def run(self):
        super(UbicSubprocess, self).run()
        self.watcher.on_success(self.create_group_file_marker)

    def status(self):
        res = super(UbicSubprocess, self).status()
        if self.node:
            res['node'] = self.node
        if self.node_backend:
            res['node_backend'] = self.node_backend
        return res

    def stop_file(self):
        if self.params.get('create_stop_file'):
            stop_file = self.params['create_stop_file']
            try:
                open(stop_file, 'w').close()
            except Exception as e:
                logger.error('Failed to create backend stop marker: {0}'.format(e))
            else:
                logger.info('Successfully created backend stop marker: {0}'.format(
                    stop_file))

        if self.params.get('remove_stop_file'):
            stop_file = self.params['remove_stop_file']
            try:
                os.remove(stop_file)
            except Exception as e:
                logger.error('Failed to remove backend stop marker: {0}'.format(e))
            else:
                logger.info('Successfully removed backend stop marker: {0}'.format(
                    stop_file))

    def create_group_file_marker(self):
        if self.params.get('group_file_marker'):
            try:
                group = (str(int(self.params.get('group')))
                         if self.params.get('group') else
                         '')
                path = self.params['group_file_marker'].format(group_id=group)
                dirname = os.path.dirname(path)
                if not os.path.exists(dirname):
                    os.makedirs(dirname, 0755)
                with open(path, 'w') as f:
                    f.write(group)
            except Exception as e:
                logger.exception('Failed to create group file marker: {0}'.format(e))
                self.stop_file()
            else:
                logger.info('Successfully created group file marker '
                            'for group {0}'.format(group))
        else:
            logger.info('Group file marker creation was not requested for '
                        'group {0}'.format(self.params.get('group')))

        if self.params.get('remove_group_file'):
            logger.info('Removing group file {0}'.format(self.params['remove_group_file']))
            try:
                os.remove(self.params['remove_group_file'])
            except Exception as e:
                logger.exception('Failed to remove group file: {0}'.format(e))
                self.stop_file()
            else:
                logger.info('Successfully removed group '
                            'file {0}'.format(self.params['remove_group_file']))
        else:
            logger.info('Group file removal was not requested for '
                        'group {0}'.format(self.params.get('group')))

        if self.params.get('move_dst') and self.params.get('move_src'):
            try:
                os.rename(self.params['move_src'], self.params['move_dst'])
            except Exception as e:
                logger.exception('Failed to execute move task: {0} to {1}'.format(
                    self.params['move_src'], self.params['move_dst']))
                self.stop_file()
            else:
                logger.info('Successfully performed move task: {0} to {1}'.format(
                    self.params['move_src'], self.params['move_dst']))

        if self.params.get('mark_backend'):
            marker = self.params['mark_backend']
            try:
                open(marker, 'w').close()
            except Exception as e:
                logger.error('Failed to create backend down marker: {0}'.format(e))
                self.stop_file()
            else:
                logger.info('Successfully created backend down marker: {0}'.format(
                    marker))

        if self.params.get('unmark_backend'):
            marker = self.params['unmark_backend']
            try:
                os.remove(marker)
            except Exception as e:
                logger.error('Failed to remove backend down marker: {0}'.format(e))
                self.stop_file()
            else:
                logger.info('Successfully removed backend down marker: {0}'.format(
                    marker))

        if self.params.get('force_stop_file'):
            self.stop_file()
