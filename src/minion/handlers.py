from functools import wraps
import json

import tornado.web

from minion.app import app
from minion.config import config
import minion.helpers as h
from minion.subprocess import manager
from minion.templates import loader
from minion.watchers import RsyncWatcher


@h.route(app, r'/')
class SomeHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('Whoa...')


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
        self.add_header('Content-Type', 'text/json')
        try:
            res = method(self, *args, **kwargs)
        except Exception as e:
            response = {
                'status': 'error',
                'error': str(e),
            }
        else:
            response = {
                'status': 'success',
                'response': res,
            }
        self.write(json.dumps(response))
    return wrapped


@h.route(app, r'/rsync/start/', name='start')
class RsyncStartHandler(AuthenticationRequestHandler):
    def post(self):
        cmd = self.get_argument('command')
        uid = manager.run(cmd, RsyncWatcher)
        self.set_status(302)
        self.add_header('Location', self.reverse_url('status', uid))


@h.route(app, r'/rsync/manual/')
class RsyncManualHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(loader.load('manual.html').generate())


@h.route(app, r'/rsync/status/([0-9a-f]+)/', name='status')
class RsyncStatusHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def get(self, uid):
        return manager.status(uid)


@h.route(app, r'/rsync/list/')
class RsyncListHandler(AuthenticationRequestHandler):
    @AuthenticationRequestHandler.auth_required
    @api_response
    def get(self):
        self.add_header('Content-Type', 'text/json')
        res = {}
        for uid in manager.keys():
            res[uid] = manager.status(uid)
        return res
