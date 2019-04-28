"""Virtual file systems.
"""
from pathlib import Path, PurePosixPath
from datetime import datetime
from pytz import utc

import s3fs


class AbstractFileSystem:

    def exists(self, path):
        raise NotImplementedError('Abstract file system.')

    def open(self, path, mode='r'):
        raise NotImplementedError('Abstract file system.')

    def mkdir(self, path):
        raise NotImplementedError('Abstract file system.')

    def last_update_time(self, path):
        raise NotImplementedError('Abstract file system.')

    def full_path(self, path):
        raise NotImplementedError('Abstract file system.')


class LocalFileSystem(AbstractFileSystem):
    def __init__(self, root):
        self.root = Path(root)

    def exists(self, path):
        return (self.root/path).exists()

    def open(self, path, mode='r'):
        virtual_path = Path(path)
        if not self.exists(virtual_path.parent):
            self.mkdir(virtual_path.parent)
        return (self.root/virtual_path).open(mode=mode)

    def mkdir(self, path):
        return (self.root/path).mkdir(parents=True, exist_ok=True)

    def last_update_time(self, path):
        if self.exists(path):
            return (self.root/path).stat().st_mtime
        else:
            return 0

    def full_path(self, path):
        return self.root/path


class S3FileSystem(AbstractFileSystem):
    def __init__(self, root, **s3fs_kwargs):
        self.root = PurePosixPath(root)
        self.file_system = s3fs.S3FileSystem(**s3fs_kwargs)

    def exists(self, path):
        return self.file_system.exists(self.full_path(path))

    def open(self, path, mode='r'):
        return self.file_system.open(self.full_path(path), mode)

    def mkdir(self, path):
        return self.file_system.mkdir(self.full_path(path))

    def last_update_time(self, path):
        if self.exists(path):
            return self.file_system.info(self.full_path(path))['LastModified']
        else:
            return datetime(1, 1, 1, tzinfo=utc)

    def full_path(self, path):
        return (self.root/path).as_posix()


def create_filesystem_from_path(root_path, **kwargs):
    if root_path.startswith('s3://'):
        s3_root_path = root_path[5:]
        return S3FileSystem(s3_root_path, **kwargs)
    else:
        return LocalFileSystem(root_path)
