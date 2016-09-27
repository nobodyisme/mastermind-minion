import json

from minion.logger import logger
from minion.watchers.base import ProgressWatcher


class DnetRecoveryWatcher(ProgressWatcher):

    def __init__(self, *args, **kwargs):
        super(DnetRecoveryWatcher, self).__init__(*args, **kwargs)
        self.commands_statuses = {}

    def status(self):
        status = super(DnetRecoveryWatcher, self).status()
        status['commands_statuses'] = self.commands_statuses
        return status

    def set_commands_stats(self, commands_statuses):
        self.commands_statuses = commands_statuses
