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


class EllipticsConfig(dict):
    def __init__(self, config_path):
        if not os.path.exists(config_path):
            raise ValueError('Config {config} does not exist'.format(config=config_path))
        try:
            config = json.load(open(config_path, 'r'))
        except (ValueError, TypeError):
            raise ValueError('File {config} is not valid elliptics config'.format(
                config=config_path,
            ))
        super(EllipticsConfig, self).__init__(config)
        self.config_path = config_path

    def get_group_backend(self, group):
        for backend in self['backends']:
            if backend['group'] == group:
                return backend['backend_id']
        raise ValueError('Group {group} is not found in config {config}'.format(
            group=group,
            config=self.config_path,
        ))
