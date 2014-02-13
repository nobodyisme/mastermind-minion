from ConfigParser import ConfigParser
import json
import os.path

from minion.logger import setup_logger


__all__ = ['config']


class JsonConfig(dict):
    def __init__(self, filename):
        self.update(json.load(open(filename)))


DEFAULT_LOGGING_CONFIG_PATH = '/etc/elliptics/mastermind-minion/logging-default.conf'
LOGGING_CONFIG_PATH = '/etc/elliptics/mastermind-minion/logging.conf'

log_config_path = DEFAULT_LOGGING_CONFIG_PATH
if os.path.exists(LOGGING_CONFIG_PATH):
    log_config_path = LOGGING_CONFIG_PATH

logging_config = JsonConfig(log_config_path)
setup_logger(logging_config['logging'])

_cp = ConfigParser()
_cp.read(['/etc/elliptics/mastermind-minion/mastermind-minion-default.conf',
          '/etc/elliptics/mastermind-minion/mastermind-minion.conf'])

config = _cp._sections
