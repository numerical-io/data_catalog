"""Virtual file systems.
"""
from pathlib import Path, PurePosixPath
from datetime import datetime
import urllib.parse as parse

from pytz import utc
import s3fs

from .abc import ABCFileSystem


class AbstractFileSystem(ABCFileSystem):

    def exists(self, path):
        raise NotImplementedError('Abstract file system.')

    def open(self, path, mode='r', **kwargs):
        raise NotImplementedError('Abstract file system.')

    def mkdir(self, path):
        raise NotImplementedError('Abstract file system.')

    def last_update_time(self, path):
        raise NotImplementedError('Abstract file system.')

    def full_path(self, path):
        raise NotImplementedError('Abstract file system.')

    def uri(self, path):
        raise NotImplementedError('Abstract file system.')

    def listdir(self, path, with_hidden_files=False):
        raise NotImplementedError('Abstract file system.')


class LocalFileSystem(AbstractFileSystem):
    def __init__(self, root):
        self.root = Path(root)

    def exists(self, path):
        return (self.root/path).exists()

    def open(self, path, mode='r', **kwargs):
        virtual_path = Path(path)
        if not self.exists(virtual_path.parent):
            self.mkdir(virtual_path.parent)
        return (self.root/virtual_path).open(mode=mode, **kwargs)

    def mkdir(self, path):
        return (self.root/path).mkdir(parents=True, exist_ok=True)

    def last_update_time(self, path):
        if self.exists(path):
            return datetime.fromtimestamp(
                (self.root/path).stat().st_mtime
            ).astimezone()
        else:
            return datetime.fromtimestamp(0).astimezone()

    def full_path(self, path):
        return self.root/path

    def uri(self, path):
        return self.full_path(path).absolute().as_uri()

    def listdir(self, path, with_hidden_files=False):
        prefix = self.full_path(path).absolute()
        filenames = []
        for filepath in prefix.iterdir():
            filename = str(filepath.relative_to(prefix))
            if with_hidden_files or not filename.startswith("."):
                filenames.append(filename)
        return filenames


class S3FileSystem(AbstractFileSystem):
    def __init__(self, root, **s3fs_kwargs):
        self.root = PurePosixPath(root)
        self.file_system = s3fs.S3FileSystem(**s3fs_kwargs)

    def exists(self, path):
        return self.file_system.exists(self.full_path(path))

    def open(self, path, mode='r', **kwargs):
        return self.file_system.open(self.full_path(path), mode, **kwargs)

    def mkdir(self, path):
        return self.file_system.mkdir(self.full_path(path))

    def last_update_time(self, path):
        if self.exists(path):
            return self.file_system.info(self.full_path(path))['LastModified']
        else:
            return datetime.fromtimestamp(0, tz=utc)

    def full_path(self, path):
        return (self.root/path).as_posix()

    def uri(self, path):
        return "s3://" + self.full_path(path)

    def listdir(self, path, with_hidden_files=False):
        prefix = self.full_path(path)
        filenames = []
        for file_desc in self.file_system.listdir(prefix):
            filename = str(PurePosixPath(file_desc["name"]).relative_to(prefix))
            if with_hidden_files or not filename.startswith("."):
                filenames.append(filename)
        return filenames


def create_filesystem_from_uri(uri, **kwargs):
    parsed_uri = parse.urlparse(uri)
    if parsed_uri.scheme == "s3":
        return S3FileSystem(f"{parsed_uri.netloc}/{parsed_uri.path}", **kwargs)

    elif parsed_uri.scheme == "file":
        return LocalFileSystem(parse.unquote(parsed_uri.path))
    else:
        raise ValueError(f"Unknown URI scheme {parsed_uri.scheme}.")
