from functools import wraps
import json
import traceback

import tornado.web

from minion.app import app
from minion.config import config
import minion.helpers as h
from minion.logger import logger
from minion.subprocess.manager import manager
from minion.templates import loader
from minion.watchers import RsyncWatcher


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
