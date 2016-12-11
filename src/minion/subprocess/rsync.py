from minion.subprocess.base_shell import BaseSubprocess
from minion.subprocess.create_group_file import CreateGroupFileCommand
from minion.subprocess.remove_path import RemovePathCommand
from minion.subprocess.create_ids_file import CreateIdsFileCommand
from minion.watchers.rsync import RsyncWatcher


class RsyncSubprocess(BaseSubprocess):

    COMMAND = 'rsync'
    watcher_base = RsyncWatcher
    REQUIRED_PARAMS = ('group',)
    POST_PROCESSORS = (
        CreateGroupFileCommand,
        RemovePathCommand,
        CreateIdsFileCommand,
    )
