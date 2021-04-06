import errno, os, stat
from datetime import datetime
from typing import Union

import pyfuse3
from canvasapi import canvas

NANOSECONDS = 1_000_000_000
INT64_MAX = 1 << 64 - 1


# These functions used to create an integer `inode` from a course number and a file id
# Other alternatives:
# - https://math.stackexchange.com/questions/2781594/bijective-function-from-n-to-n-x-n
# - https://math.stackexchange.com/questions/444447/bijection-between-mathbbn-and-mathbbn-times-mathbbn


def bijection_forwards(i, j) -> int:
    """
    Some bijection f: N x N -> N. No guarantee that this will be consistent between versions. 
    """
    assert 0 <= i < (1 << 15)  # surely a maximum of 32_000 courses is enough
    assert 0 <= j < (1 << 48)  # quite likely file ids will stay below 281_474_976_710_656
    return i * (1 << 48) + j


def bijection_backwards(n) -> (int, int):
    """
    Inverse of `bijection_forwards`
    """
    return divmod(n, 1 << 48)


def datetime_to_fstime(t: datetime) -> int:
    return int(t.timestamp() * NANOSECONDS)


def default_inode_entry(inode) -> pyfuse3.EntryAttributes:
    """
    Caller must set `st_mode` and `st_size` on returned object before it is considered valid. Name?
    """
    entry = pyfuse3.EntryAttributes()

    # seconds --> nanoseconds
    stamp = datetime_to_fstime(datetime.now())

    entry.st_atime_ns = stamp  # accessed
    entry.st_ctime_ns = stamp  # created
    entry.st_mtime_ns = stamp  # modified
    entry.st_gid = os.getgid()
    entry.st_uid = os.getuid()
    entry.st_ino = inode

    # entry.attr_timeout = 10000
    # entry.entry_timeout = 10000

    return entry


def default_folder_entry(inode) -> pyfuse3.EntryAttributes:
    folder_entry = default_inode_entry(inode)
    folder_entry.st_mode = (stat.S_IFDIR | 0o755)
    folder_entry.st_size = 0
    return folder_entry


def fuse_assert(test: bool):
    """
    Will throw the FUSE equivalent of a FileNotFoundError if `test` is falsey.
    """
    if not test:
        raise pyfuse3.FUSEError(errno.ENOENT)


class LocalFolder:
    def __init__(self, inode, name):
        self.inode = inode
        self.name = name
        self.attributes = default_folder_entry(inode)


def fs_attributes(obj: Union[canvas.File, canvas.Folder, LocalFolder], inode: int) -> pyfuse3.EntryAttributes:
    if isinstance(obj, canvas.File):
        file_attr = default_inode_entry(inode)
        file_attr.st_mode = stat.S_IFREG | 0o644
        file_attr.st_size = obj.size
        file_attr.st_ctime_ns = datetime_to_fstime(obj.created_at_date)
        file_attr.st_mtime_ns = datetime_to_fstime(obj.modified_at_date)
        return file_attr

    elif isinstance(obj, canvas.Folder):
        folder_attr = default_inode_entry(inode)
        folder_attr.st_mode = stat.S_IFDIR | 0o755
        folder_attr.st_size = 0
        folder_attr.st_mtime_ns = datetime_to_fstime(obj.updated_at_date)
        return folder_attr

    elif isinstance(obj, LocalFolder):
        return obj.attributes

    else:
        raise TypeError("expected a `File` or `Folder` instance. Got:", obj)


def fs_name(obj: Union[canvas.File, canvas.Folder, LocalFolder]) -> str:
    if isinstance(obj, canvas.File):
        # this field is URL encoded. %20...
        # return obj.filename
        return obj.display_name
    elif isinstance(obj, canvas.Folder):
        return obj.name
    elif isinstance(obj, LocalFolder):
        return obj.name
    else:
        raise TypeError("expected a `File` or `Folder` instance. Got:", obj)


def fs_parent(obj: Union[canvas.File, canvas.Folder]) -> int:
    if isinstance(obj, canvas.File):
        return obj.folder_id
    elif isinstance(obj, canvas.Folder):
        return obj.parent_folder_id
    else:
        raise TypeError("expected a `File` or `Folder` instance. Got:", obj)


class DirectoryListing:
    def __init__(self, contents: [(int, str)]):
        self.contents = contents

    def add(self, inode, name):
        self.contents.append((inode, name))

    def name_to_inode(self, lookup_name: str) -> int:
        """
        Returns None if does not exist
        """
        assert type(lookup_name) == str

        for inode, name in self.contents:
            if name == lookup_name:
                return inode

    def list_from(self, from_inode=0):
        """
        An iterator over all items in this directory with inode >= from_inode, in increasing order of inode
        TODO: [Faster listing](https://github.com/grantjenks/python-sortedcontainers)
        """
        for inode, name in sorted(self.contents, key=lambda x: x[0]):
            if inode < from_inode:
                continue
            else:
                yield inode, name
