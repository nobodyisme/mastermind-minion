#!/usr/bin/env python
import tornado

from minion.app import app
import minion.handlers


if __name__ == '__main__':
    app.listen(8080)
    tornado.ioloop.IOLoop.instance().start()
