from functools import wraps


def route(app, route):
    def wrapper(obj):
        app.add_handlers('', [(route, obj)])
        return obj
    return wrapper
