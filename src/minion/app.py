import tornado.web

from minion.config import config


app = tornado.web.Application(debug=config['common']['debug'] == 'True', gzip=True)
