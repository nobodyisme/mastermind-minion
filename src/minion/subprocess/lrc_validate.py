from minion.subprocess.base_shell import BaseSubprocess
from minion.artifacts.exec_state import ExecStateArtifactsMixin


class LrcValidateSubprocess(BaseSubprocess, ExecStateArtifactsMixin):

    COMMAND = 'lrc_validate'
