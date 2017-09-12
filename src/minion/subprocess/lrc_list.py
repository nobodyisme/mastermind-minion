from minion.subprocess.base_shell import BaseSubprocess
from minion.artifacts.exec_state import ExecStateArtifactsMixin


class LrcListSubprocess(ExecStateArtifactsMixin, BaseSubprocess):

    COMMAND = 'lrc_list'
