import tornado.web

from minion.config import config
import minion.logger


app = tornado.web.Application(debug=config['common']['debug'] == 'True')
app.listen(int(config['common']['port']))
