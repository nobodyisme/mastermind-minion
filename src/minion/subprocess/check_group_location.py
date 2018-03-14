import json
import os.path
import gzip

from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from minion.config import config
from minion.subprocess.base import BaseCommand


class CheckGroupLocation(BaseCommand):

    COMMAND = 'check_group_location'
    REQUIRED_PARAMS = ('group', 'backend')

    DNET_MONITOR_BACKEND = 16
    MONITOR_URL_TPL = 'http://localhost:{port}/?categories={categories}&backends={backends}'

    @staticmethod
    def _check_backend_group_id(backend_id, backend_config, group):
        backend_group_id = backend_config.get('group')
        if not backend_group_id:
            raise ValueError('Group for backend {} is not set in config'.format(backend_id))

        if backend_group_id != group:
            raise ValueError('Backend {} belongs to group {}, expected group {}'.format(
                backend_id,
                backend_group_id,
                group,
            ))

    @staticmethod
    def _check_base_path(backend_id, backend_config, base_path):
        backend_data_path = backend_config.get('data')
        if not backend_data_path:
            raise ValueError('Base path is not set in config for backend {}'.format(backend_id))

        backend_base_path = os.path.dirname(backend_data_path) + '/'
        if base_path != backend_base_path:
            raise ValueError('Backend {}: config backend path is {}, expected {}'.format(
                backend_id,
                backend_base_path,
                base_path,
            ))

    @staticmethod
    def _check_group_file(group_file, group):
        try:
            with open(group_file) as f:
                group_file_content = f.read()
        except Exception as e:
            raise RuntimeError(
                'Failed to read contents of group id file {}: {}'.format(group_file, e)
            )

        group_file_group_id = int(group_file_content.strip())
        if group != group_file_group_id:
            raise ValueError(
                'Group file {} contains group id {}, expected {}'.format(
                    group_file,
                    group_file_group_id,
                    group,
                )
            )

    @gen.coroutine
    def execute(self):
        backend_id = self.params['backend']

        http_client = AsyncHTTPClient()
        monitor_port = self.params.get('monitor_port', config['elliptics']['monitor_port'])
        request = HTTPRequest(
            self.MONITOR_URL_TPL.format(
                port=monitor_port,
                categories=self.DNET_MONITOR_BACKEND,
                backends=backend_id,
            ),
            allow_ipv6=True,
            connect_timeout=float(config['elliptics']['monitor_port_connect_timeout']),
            request_timeout=float(config['elliptics']['monitor_port_request_timeout']),
        )
        response = yield http_client.fetch(request, raise_error=False)
        if response.error:
            raise RuntimeError(
                'Local elliptics monitor port request failed: {}'.format(response.error)
            )

        try:
            monitor_data_json = gzip.zlib.decompress(response.body)
        except Exception as e:
            raise RuntimeError(
                'Failed to uncompress local elliptics node response: {}'.format(e)
            )

        try:
            monitor_data = json.loads(monitor_data_json)
        except ValueError as e:
            raise RuntimeError('Failed to parse json from local elliptics node: {}'.format(e))

        backend_data = monitor_data['backends'].get(str(backend_id))
        if not backend_data:
            raise ValueError('Backend {} is not found'.format(backend_id))

        backend_config = backend_data.get('backend', {}).get('config', {})

        group = int(self.params['group'])
        self._check_backend_group_id(backend_id, backend_config, group)

        base_path = self.params.get('base_path')
        if base_path:
            self._check_base_path(backend_id, backend_config, base_path)

        group_file = self.params.get('group_file')
        if group_file:
            self._check_group_file(group_file, group)
