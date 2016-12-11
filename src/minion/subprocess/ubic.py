from minion.subprocess.base_shell import BaseSubprocess
from minion.subprocess.create_file_marker import CreateFileMarkerCommand
from minion.subprocess.remove_group_file import RemoveGroupFileCommand
from minion.subprocess.move_path import MovePathCommand
from minion.subprocess.lock_backend import LockBackendCommand
from minion.subprocess.unlock_backend import UnlockBackendCommand


class UbicSubprocess(BaseSubprocess):

    COMMAND = 'ubic'
    REQUIRED_PARAMS = ('node_backend',)
    POST_PROCESSORS = (
        CreateFileMarkerCommand,
        RemoveGroupFileCommand,
        MovePathCommand,
        LockBackendCommand,
        UnlockBackendCommand,
    )
