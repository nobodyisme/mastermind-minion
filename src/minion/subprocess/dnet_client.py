from minion.subprocess.ubic import UbicSubprocess
from minion.watchers.dnet_client import DnetClientWatcher


class DnetClientSubprocess(UbicSubprocess):

    @property
    def watcher_base(self):
        return DnetClientWatcher
