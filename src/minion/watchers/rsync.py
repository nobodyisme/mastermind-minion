#!/usr/bin/env python
import re

from minion.watchers.base import ProgressWatcher


class RsyncWatcher(ProgressWatcher):

    SPLIT_RE = re.compile(r'\r|\n')
    FILES_CNT_RE = re.compile(r'(\d+)/(\d+)')

    def __init__(self, subprocess):
        super(RsyncWatcher, self).__init__(subprocess)
        self.current_file = 0
        self.total_files = 100  # first estimation of total files num

        self.buf = ''

    def feed(self, s):
        if self.exit:
            return
        data = self.SPLIT_RE.split(s)
        if not data:
            self._process_line(self.buf)
            return
        data[0] = self.buf + data[0]
        for line in data[:-1]:
            self._process_line(line)
        self.buf = data[-1]

    def _process_line(self, s):
        parts = filter(None, s.split(' '))
        if len(parts) < 2:
            return
        if not '%' in parts[1]:
            return

        percentage = int(parts[1][:-1])

        self.progress = (percentage / 100.0 / self.total_files + self.current_file / self.total_files)

        cnt_match = self.FILES_CNT_RE.findall(parts[-1])
        if cnt_match:
            left, total = cnt_match[0]
            self.total_files = float(total)
            self.current_file = self.total_files - int(left)

        return True

    @property
    def exit_messages(self):
        return RSYNC_EXIT_CODES


RSYNC_EXIT_CODES = {
    0: 'Success',
    1: 'Syntax or usage error',
    2: 'Protocol incompatibility',
    3: 'Errors selecting input/output files, dirs',
    4: 'Requested action not supported: an attempt was made to manipulate 64-bit files on a '
       'platform that cannot support them; or an option was specified that is  supported  by '
       'the client and not by the server',
    5: 'Error starting client-server protocol',
    6: 'Daemon unable to append to log-file',
    10: 'Error in socket I/O',
    11: 'Error in file I/O',
    12: 'Error in rsync protocol data stream',
    13: 'Errors with program diagnostics',
    14: 'Error in IPC code',
    20: 'Received SIGUSR1 or SIGINT',
    21: 'Some error returned by waitpid()',
    22: 'Error allocating core memory buffers',
    23: 'Partial transfer due to error',
    24: 'Partial transfer due to vanished source files',
    25: 'The --max-delete limit stopped deletions',
    30: 'Timeout in data send/receive',
    35: 'Timeout waiting for daemon connection',
}
