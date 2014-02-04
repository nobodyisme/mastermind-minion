from functools import wraps
import json

import tornado.web

from minion.app import app
import minion.helpers as h
from minion.subprocess import manager
from minion.watchers import RsyncWatcher


config = {'debug': False,
          'authkey': 'qwerty'}

@h.route(app, r'/')
class SomeHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('Whoa...')


class AuthenticationRequestHandler(tornado.web.RequestHandler):
    def is_authenticated(self):
        if config['debug']:
            return True
        return self.request.headers.get('X-Auth', '') == config['authkey']

    @staticmethod
    def auth_required(method):
        @wraps(method)
        def wrapped(self, *args, **kwargs):
            if not self.is_authenticated():
                self.set_status(403)
                return
            return method(self, *args, **kwargs)
        return wrapped


@h.route(app, r'/rsync/start/')
class RsyncStartHandler(AuthenticationRequestHandler):
    def get(self):
        uid = manager.run('rsync -rlHpogDtvv --progress --stats /home/indigo/rsyncsrv/ /home/indigo/rsyncsrv2/', RsyncWatcher)
        print "=" * 20
        print "STARTING %s" % uid
        print "=" * 20
        self.write(uid)


@h.route(app, r'/rsync/status/([0-9a-f]+)/')
class RsyncStartHandler(AuthenticationRequestHandler):
    @RsyncStartHandler.auth_required
    def get(self, uid):
        self.add_header('Content-Type', 'text/json')
        self.write(json.dumps(manager.status(uid)))

