from minion.subprocess.base_shell import BaseSubprocess
from minion.artifacts.exec_state import ExecStateArtifactsMixin


class LrcConvertSubprocess(BaseSubprocess, ExecStateArtifactsMixin):

    COMMAND = 'lrc_convert'
