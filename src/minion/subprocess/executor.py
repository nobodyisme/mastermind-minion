import os
import os.path
import time

from tornado.ioloop import IOLoop

from minion.db import Session
import minion.db.commands
from minion.logger import logger
from minion.subprocess.base import BaseSubprocess


class CodeExecutor(BaseSubprocess):

    def __init__(self, uid, cmd, params=None, env=None, success_codes=None, io_loop=IOLoop.instance()):
        super(CodeExecutor, self).__init__(
            uid,
            cmd,
            params=params,
            env=env,
            success_codes=success_codes,
            io_loop=io_loop,
        )
        self.error = None
        self.start_ts = int(time.time())
        self.command = None

    def removed_basename(self, basename):
        return 'removed_backend.{basename}'.format(basename=basename)

    def tmp_basename(self, basename):
        return 'new_backend.{basename}'.format(basename=basename)

    def run(self):

        # create db record
        s = Session()
        s.begin()
        command = minion.db.commands.Command(
            uid=self.uid,
            pid=None,
            command=self.cmd_str,
            start_ts=int(time.time()),
            task_id=self.params.get('task_id')
        )
        # TODO: what about group_id, node, node_backend ?

        s.add(command)
        s.commit()

        try:
            self.execute()
        except Exception as e:
            self.error = e
            # TODO: raise?
        self.finish_ts = int(time.time())

        s.begin()
        try:
            command.progress = 1.0
            command.exit_code = 1 if self.error else 0
            command.command_code = 1 if self.error else 0
            command.finish_ts = self.finish_ts
            s.add(command)
            s.commit()
        except Exception as e:
            logger.exception('Failed to update db command')
            s.rollback()

    def status(self):
        return {
            'progress': 1.0,
            'exit_code': 1 if self.error else 0,
            'exit_message': str(self.error) if self.error else 'Success',
            'command_code': 1 if self.error else 0,
            'start_ts': self.start_ts,
            'finish_ts': self.finish_ts,
            'output': '',
            'error_output': '',
            'pid': None,
            'task_id': self.params.get('task_id'),
            'command': self.cmd_str,
        }


class GroupCreator(CodeExecutor):

    def execute(self):
        group_base_path_root_dir = self.params['group_base_path_root_dir'].rstrip('/')

        basename = self.get_vacant_basename(group_base_path_root_dir)
        tmp_basename = self.tmp_basename(basename)

        tmp_dir = os.path.join(group_base_path_root_dir, tmp_basename)
        logger.info('Creating tmp dir for new group: {}'.format(tmp_dir))
        try:
            os.mkdir(tmp_dir, 0755)
        except Exception:
            logger.exception('Failed to create tmp dir for new group')
            raise

        logger.info('Adding group files')
        for filename, body in self.params['files'].iteritems():
            logger.info('Adding file {}'.format(filename))
            filename = os.path.join(
                tmp_dir,
                filename
            )
            dirname, basefname = os.path.split(filename)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(filename, 'wb') as f:
                f.write(body)

        dest_dir = os.path.join(group_base_path_root_dir, basename)
        logger.info(
            'Renaming tmp dir {tmp_dir} to destination dir {dest_dir}'.format(
                tmp_dir=tmp_dir,
                dest_dir=dest_dir,
            )
        )
        try:
            os.rename(tmp_dir, dest_dir)
        except Exception:
            logger.exception('Failed to rename tmp dir to dest dir')
            raise

    def get_vacant_basename(self, base_path):
        free_basename = 1
        while True:
            dest_dir = os.path.join(base_path, str(free_basename))

            tmp_basename = self.tmp_basename(str(free_basename))
            tmp_dir = os.path.join(base_path, tmp_basename)

            removed_basename = self.removed_basename(str(free_basename))
            removed_dir = os.path.join(base_path, removed_basename)

            if any(map(os.path.exists, [dest_dir, tmp_dir, removed_dir])):
                free_basename += 1
                continue
            break
        return str(free_basename)


class GroupRemover(CodeExecutor):

    def execute(self):
        group_base_path = self.params['group_base_path'].rstrip('/')

        if not os.path.exists(group_base_path):
            raise RuntimeError(
                'Group dir {path} does not exist'.format(
                    path=group_base_path,
                )
            )

        dst_base_path, basename = os.path.split(group_base_path)

        remove_path = os.path.join(
            dst_base_path,
            self.removed_basename(basename)
        )
        logger.info(
            'Renaming group base dir {tmp_dir} to destination dir {dest_dir}'.format(
                tmp_dir=group_base_path,
                dest_dir=remove_path,
            )
        )
        try:
            os.rename(group_base_path, remove_path)
        except OSError as e:
            if e.errno == 2:
                # errno == 2: No such file or directory
                if os.path.exists(remove_path):
                    # group_base_path was already renamed, not an error
                    pass
                else:
                    raise
        except Exception:
            logger.exception('Failed to rename tmp dir to dest dir')
            raise
