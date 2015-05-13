from minion.subprocess.ubic import UbicSubprocess
from minion.watchers.dnet_client import DnetClientWatcher


class DnetClientSubprocess(UbicSubprocess):

    watcher_base = DnetClientWatcher
