import json
import functools
import os
import argparse
from datetime import datetime
import trio
import stat
import errno
import pyfuse3
from canvasapi import Canvas
from canvasapi.file import File
from canvasapi.folder import Folder

NANOSECONDS = 1e9


def default_inode_entry(inode):
    """
    Caller must set `st_mode` and `st_size` on returned object before it is considered "valid"
    """
    entry = pyfuse3.EntryAttributes()

    # Seconds --> nanoseconds
    stamp = datetime.now().timestamp() * NANOSECONDS

    entry.st_atime_ns = stamp
    entry.st_ctime_ns = stamp
    entry.st_mtime_ns = stamp
    entry.st_gid = os.getgid()
    entry.st_uid = os.getuid()
    entry.st_ino = inode
    return entry


class FileAccessWrapper:
    """
    Handle folder structure, file names/ids, and caching
    """

    def __init__(self, course):
        with open("/mnt/files/UoA/Canvas/token.json", "r") as f:
            token = json.load(f)["token"]
        self.canvas = Canvas("https://canvas.auckland.ac.nz", token)
        self.course = self.canvas.get_course(course)

        # TODO!
        self.inodes = {}
        self.name_index = {} # { name => inode }

        self.build_fs_tree()

    def build_fs_tree(self):
        """
        Call to this function is only necessary if remote files on Canvas have been changed
        Maybe call this every 30 minutes? Notify on change -- would need to comapre equality of FS trees
        """
        folders = list(self.course.get_folders())
        files: [File] = list(self.course.get_files())

        for f in files:
            self.inodes[f.id] = f
            self.name_index[f.filename] = f.id
        for f in folders:
            self.inodes[f.id] = f
            self.name_index[f.name] = f.id

    def lookup_inode(self, parent_inode, name):
        # TODO: Currently ignoring `parent_inode` assuming all files in same dir
        return self.name_index[name]

    @functools.lru_cache(maxsize=None)
    def file_contents(self, inode):
        assert isinstance(self.inodes[inode], File)
        return self.inodes[inode].get_contents()

    def read_inode_file(self, fh, off, size):
        # TODO: Put into ~/.cache - storing in memory impractical - check id and name, also date modified for sanity
        return self.file_contents(fh)[off:off + size]

"""
Separate class for courses that manages its files/folders independently? Need to assume IDs don't clash
Way to notify/upwards for updating?
"""

class CanvasFS(pyfuse3.Operations):
    def __init__(self, course):
        super().__init__()

        self.root_attr = default_inode_entry(pyfuse3.ROOT_INODE)
        self.root_attr.st_mode = (stat.S_IFDIR | 0o755)
        self.root_attr.st_size = 0

        # Initialise wrapper to Canvas API
        self.wrapper = FileAccessWrapper(course)

    """
    Implement filesystem operations
    """

    async def getattr(self, inode, ctx=None):
        if inode == pyfuse3.ROOT_INODE:
            return self.root_attr

        if inode not in self.wrapper.inodes:
            raise pyfuse3.FUSEError(errno.ENOENT)

        """
        FILE/INODE PERMISSIONS
        4 is read
        5 is read/execute (execute is always needed for directories)
        6 is read/write
        7 is read/write/execute 
        """

        node = self.wrapper.inodes[inode]
        if isinstance(node, Folder):
            entry = default_inode_entry(inode)
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_size = 0

            entry.st_mtime_ns = node.updated_at_date.timestamp() * NANOSECONDS
            return entry

        if isinstance(node, File):
            entry = default_inode_entry(inode)
            entry.st_mode = (stat.S_IFREG | 0o644)
            # I assume the Canvas API returns the size in bytes...
            entry.st_size = node.size

            entry.st_ctime_ns = node.created_at_date.timestamp() * NANOSECONDS
            entry.st_mtime_ns = node.modified_at_date.timestamp() * NANOSECONDS
            return entry

        raise RuntimeError("Some weird thing going on with the inode tree...")

    async def lookup(self, parent_inode, name, ctx=None):
        try:
            inode_of_target = self.wrapper.lookup_inode(parent_inode, name)
            return await self.getattr(inode_of_target)
        except:
            raise pyfuse3.FUSEError(errno.ENOENT)

    async def opendir(self, inode, ctx):
        # Forgot this: Results in opening mount point giving "does not exist"
        if inode == pyfuse3.ROOT_INODE:
            return pyfuse3.ROOT_INODE

        # TODO: Please don't try to open a file as a directory
        # TODO: When FileAccessWrapper rebuilt, need to invalidate returned handles

        if inode not in self.wrapper.inodes:
            raise pyfuse3.FUSEError(errno.ENOENT)

        return inode

    async def readdir(self, fh, start_id, token):

        # Traversal order struct/record

        file_order = list(filter(lambda f: isinstance(f, File), self.wrapper.inodes.values()))
        file_order.sort(key=lambda f: f.id)

        # Discard start
        it = enumerate(file_order)
        for _ in range(start_id):
            next(it)

        # TODO: Filter by whether is parent node

        # todo: rotating queue?

        for num, file in it:
            # Expects bytes as name
            if not pyfuse3.readdir_reply(token, file.filename.encode('utf-8'), await self.getattr(file.id), num + 1):
                break

    async def open(self, inode, flags, ctx):
        # TODO: Check is actually a file
        if inode not in self.wrapper.inodes:
            raise pyfuse3.FUSEError(errno.ENOENT)

        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        # TODO: Is streaming read possible?
        # Maybe cache in blocks of 10MB?
        # Synchronous read because why not
        return self.wrapper.read_inode_file(fh, off, size)


if __name__ == "__main__":
    par = argparse.ArgumentParser()
    par.add_argument("mount_point", type=str)
    par.add_argument("course_id", type=int)
    args = par.parse_args()

    filesystem = CanvasFS(args.course_id)
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=canvas_fs')

    # TODO: Be able to configure display/courses at runtime? Dynamically change directory listings.

    # TODO: Easy way to terminate and automatically unmount

    # TODO: Get course list and find courses supporting `get_files()` automatically and set up dirs

    astro = 48353 # ASTRO 200G
    ra = 47211 # MATHS 332: Real Analysis
    if False:
        fuse_options.add('debug')

    pyfuse3.init(filesystem, args.mount_point, fuse_options)
    print("Starting filesystem...")
    try:
        trio.run(pyfuse3.main)
    except KeyboardInterrupt:
        print("Stopping filesystem...")
    finally:
        pyfuse3.close(unmount=True)
        
