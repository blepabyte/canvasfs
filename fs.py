import errno, os, stat
from datetime import datetime

import canvasapi
import pyfuse3

NANOSECONDS = 1_000_000_000
INT64_MAX = 1 << 64 - 1


# These functions used to create an integer `inode` from a course number and a file id


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


class Node:
    def __init__(self, inode, obj, course_num):
        # TODO: Set parent course num correctly during build()
        self.parent_course_num = course_num
        self.inode = inode
        self.obj = obj
        self.id = obj.id

        if self.is_file():
            self.name = self.obj.filename
        elif self.is_folder():
            self.name = self.obj.name
        else:
            assert False, self.obj

    def is_file(self) -> bool:
        return isinstance(self.obj, canvasapi.file.File)

    def is_folder(self) -> bool:
        return isinstance(self.obj, canvasapi.folder.Folder)

    def parent(self):
        """
        Return the id of the parent/containing folder converted to an inode
        """
        if self.is_file():
            return self.obj.folder_id
        if self.is_folder():
            return self.obj.parent_folder_id

        assert False, self.obj

    def attributes(self) -> pyfuse3.EntryAttributes:
        if self.is_file():
            file_attr = default_inode_entry(self.inode)
            file_attr.st_mode = stat.S_IFREG | 0o644
            file_attr.st_size = self.obj.size
            file_attr.st_ctime_ns = datetime_to_fstime(self.obj.created_at_date)
            file_attr.st_mtime_ns = datetime_to_fstime(self.obj.modified_at_date)
            return file_attr

        if self.is_folder():
            folder_attr = default_inode_entry(self.inode)
            folder_attr.st_mode = stat.S_IFDIR | 0o755
            folder_attr.st_size = 0
            folder_attr.st_mtime_ns = datetime_to_fstime(self.obj.updated_at_date)
            return folder_attr

        assert False, self.obj


class OpenNode(Node):
    ...


class Subdirectory(Node):
    def __init__(self, self_node: Node, contents: [Node]):
        """
        The folder nodes in `contents` **could** implement Subdirectory but they don't - otherwise we would have a tree of objects stored
        """
        assert type(self_node) == Node
        super().__init__(self_node.inode, self_node.obj, self_node.parent_course_num)
        self.contents = contents

    def with_name(self, lookup_name: str) -> Node:
        # Try to avoid past issue where names were in bytes so all lookups failed
        assert type(lookup_name) == str

        # TODO: Proper O(1) lookup
        for f in self.contents:
            if f.name == lookup_name:
                return f

    def list_files(self, lower_bound_inode=0):
        """
        An iterator, in ascending order of inode, over all entries in `self.contents` with inode greater than the given `lower_bound`
        # TODO: [Faster listing](https://github.com/grantjenks/python-sortedcontainers)
        """
        for node in sorted(self.contents, key=lambda n: n.inode):
            if node.inode < lower_bound_inode:
                continue
            yield node
