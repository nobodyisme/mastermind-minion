from tornado.web import URLSpec

from functools import wraps


def route(app, route, name=None):
    def wrapper(obj):
        app.add_handlers('', [URLSpec(route, obj, name=name)])
        return obj
    return wrapper
