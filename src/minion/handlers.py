from functools import wraps
import json
import os.path
import traceback

import tornado.web

from minion.app import app
from minion.config import config
import minion.helpers as h
from minion.logger import logger
from minion.subprocess.manager import manager
from minion.templates import loader
from minion.subprocess.executor import GroupCreator, GroupRemover


@h.route(app, r'/ping/')
class PingHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('OK')


class AuthenticationRequestHandler(tornado.web.RequestHandler):
    def is_authenticated(self):
        if config['common']['debug'] == 'True':
            return True
        return (self.request.headers.get('X-Auth', '') ==
                config['auth']['key'])

    @staticmethod
    def auth_required(method):
        @wraps(method)
        def wrapped(self, *args, **kwargs):
            if not self.is_authenticated():
                self.set_status(403)
                return
            return method(self, *args, **kwargs)
        return wrapped


def api_response(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        try:
            self.add_header('Content-Type', 'text/json')
            try:
                res = method(self, *args, **kwargs)
            except Exception as e:
                logger.error('{0}: {1}'.format(e, traceback.format_exc(e)))
                response = {'status': 'error',
                            'error': str(e)}
            else:
                response = {'status': 'success',
                            'response': res}
            try:
                self.write(json.dumps(response))
            except Exception as e:
                logger.error('Failed to dump json response: {0}\n{1}'.format(e,
                    traceback.format_exc(e)))
                response = {'status': 'error',
                            'error': 'failed to construct response, see log file'}
                self.write(json.dumps(response))

        finally:
            self.finish()

    return wrapped


@h.route(app, r'/command/start/', name='start')
@h.route(app, r'/rsync/start/')
class RsyncStartHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def post(self):
        cmd = self.get_argument('command')
        success_codes = [int(c) for c in self.get_arguments('success_code')]
        params = dict((k, v[0]) for k, v in self.request.arguments.iteritems()
                                if k not in ('success_code',))
        uid = manager.run(cmd, params, success_codes=success_codes)
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))


@h.route(app, r'/rsync/manual/')
class RsyncManualHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(loader.load('manual.html').generate())


@h.route(app, r'/command/terminate/', name='terminate')
class CommandTerminateHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def post(self):
        uid = self.get_argument('cmd_uid')
        manager.terminate(uid)
        return {uid: manager.status(uid)}


@h.route(app, r'/command/status/([0-9a-f]+)/', name='status')
@h.route(app, r'/rsync/status/([0-9a-f]+)/')
class RsyncStatusHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def get(self, uid):
        return {uid: manager.status(uid)}


@h.route(app, r'/node/shutdown/', name='node_shutdown')
class NodeShutdownHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def post(self):
        cmd = self.get_argument('command')
        params = dict((k, v[0]) for k, v in self.request.arguments.iteritems())
        uid = manager.run(cmd, params)
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))


@h.route(app, r'/command/list/')
@h.route(app, r'/rsync/list/')
class RsyncListHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def get(self):
        finish_ts_gte = int(self.get_argument('finish_ts_gte', default=0)) or None
        return manager.unfinished_commands(finish_ts_gte=finish_ts_gte)


@h.route(app, r'/command/create_group/')
class CreateGroupHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def post(self):
        if config['common'].get('base_path') is None:
            logger.error('base path is not set, create group cannot be performed')
            self.set_status(500)
            raise RuntimeError('group creation is not allowed')
        files = {}
        for filename, http_files in self.request.files.iteritems():
            norm_filename = os.path.normpath(filename)
            if norm_filename.startswith('..') or norm_filename.startswith('/'):
                logger.error(
                    'Cannot create file {filename}, '
                    'normalized path {norm_filename} is not allowed'.format(
                        filename=filename,
                        norm_filename=norm_filename,
                    )
                )
                self.set_status(403)
                raise RuntimeError(
                    'File {filename} is forbidden, path should be relative '
                    'to group base directory'.format(filename=filename)
                )
            http_file = http_files[0]
            files[norm_filename] = http_file.body

        params = {
            k: v[0]
            for k, v in self.request.arguments.iteritems()
        }
        params['group_base_path'] = os.path.normpath(params['group_base_path'])
        if not params['group_base_path'].startswith(config['common']['base_path']):
            self.set_status(403)
            raise RuntimeError(
                'Group base path {path} is not under common base path'.format(
                    path=params['group_base_path'],
                )
            )
        params['files'] = files
        uid = manager.execute(GroupCreator, cmd='create_group', params=params)
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))


@h.route(app, r'/command/remove_group/')
class RemoveGroupHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def post(self):
        if config['common'].get('base_path') is None:
            logger.error('base path is not set, remove group cannot be performed')
            self.set_status(500)
            raise RuntimeError('group creation is not allowed')
        params = {
            k: v[0]
            for k, v in self.request.arguments.iteritems()
        }
        params['group_path'] = os.path.normpath(params['group_path'])
        if not params['group_path'].startswith(config['common']['base_path']):
            self.set_status(403)
            raise RuntimeError(
                'Group path {path} is not under common base path'.format(
                    path=params['group_path'],
                )
            )
        uid = manager.execute(GroupRemover, cmd='remove_group', params=params)
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))
