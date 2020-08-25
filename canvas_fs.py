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
from canvasapi.course import Course
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


def default_folder_entry(inode):
    folder_entry = default_inode_entry(inode)
    folder_entry.st_mode = (stat.S_IFDIR | 0o755)
    folder_entry.st_size = 0
    return folder_entry


class DoubleDict:
    def __init__(self):
        self.left_entries = {}
        self.right_entries = {}

    def insert(self, left, right):
        self.left_entries[left] = right
        self.right_entries[right] = left

    def get_l(self, left):
        return self.left_entries[left]

    def get_r(self, right):
        return self.right_entries[right]


class FileAccessWrapper:
    """
    Handle folder structure, file names/ids, and caching
    """

    def __init__(self, course_obj):
        self.course = course_obj

        self.inodes = {}
        self.name_index = {} # { name => inode }
        self.build_fs_tree()

    def build_fs_tree(self):
        """
        Call to this function is only necessary if remote files on Canvas have been changed
        Maybe call this every 30 minutes? Notify on change -- would need to comapre equality of FS trees

        UPDATE: Only call if directory is actually accessed / lookup call made. Limit server refreshes to 15 minutes. 
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

    def getattr(self, inode):
        node = self.inodes[inode]

        """
        FILE/INODE PERMISSIONS
        4 is read
        5 is read/execute (execute is always needed for directories)
        6 is read/write
        7 is read/write/execute 
        """

        if isinstance(node, Folder):
            entry = default_inode_entry(inode)
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_size = 0

            entry.st_mtime_ns = node.updated_at_date.timestamp() * NANOSECONDS
            return entry

        if isinstance(node, File):
            entry = default_inode_entry(inode)
            entry.st_mode = (stat.S_IFREG | 0o644)
            entry.st_size = node.size # Assume the Canvas API returns size in bytes

            entry.st_ctime_ns = node.created_at_date.timestamp() * NANOSECONDS
            entry.st_mtime_ns = node.modified_at_date.timestamp() * NANOSECONDS
            return entry

        raise RuntimeError("Some weird thing going on with the inode tree...")

"""
Separate class for courses that manages its files/folders independently? Need to assume IDs don't clash
Way to notify/upwards for updating?
"""

class CanvasFS(pyfuse3.Operations):
    def __init__(self, course_file_wrappers: {str: FileAccessWrapper}):

        # pass folder name for course?

        super().__init__()

        self.root_attr = default_inode_entry(pyfuse3.ROOT_INODE)
        self.root_attr.st_mode = (stat.S_IFDIR | 0o755)
        self.root_attr.st_size = 0

        self.courses = course_file_wrappers

        self.course_name_lookup: {str: int} = {}
        self.course_inode_lookup: {int: FileAccessWrapper} = {}

        # self.course_folders: {int: (pyfuse3.EntryAttributes, FileAccessWrapper)} = {}

        # Setup inodes for each course folder
        offset = 1
        for name, wrapper in course_file_wrappers.items():
            inode_no = pyfuse3.ROOT_INODE + offset

            self.course_name_lookup[name] = inode_no
            self.course_inode_lookup[inode_no] = wrapper

            offset += 1

        # TODO: how to get registered inodes to each course? when do inodes change? ideally we would pass a reference to the internal inode list but this is Python

    def get_course_with_inode(self, inode) -> FileAccessWrapper:
        """
        Does a sanity check to ensure the same inode does not belong to multiple courses (this could theoretically happen)
        """
        found = None
        for file_wrapper in self.course_inode_lookup.values():
            if inode in file_wrapper.inodes:
                assert found is not None
                found = file_wrapper.getattr(inode)

        if found is not None:
            return found
        else:
            raise pyfuse3.FUSEError(errno.ENOENT)

    """
    Implement filesystem operations
    """

    async def getattr(self, inode, ctx=None):
        if inode == pyfuse3.ROOT_INODE:
            return self.root_attr

        if inode in self.course_inode_lookup:
            return default_folder_entry(inode)

        return self.get_course_with_inode(inode).getattr(inode)

    async def lookup(self, parent_inode, name, ctx=None):
        if parent_inode == pyfuse3.ROOT_INODE:
            if name in self.course_name_lookup:
                return await self.getattr(self.course_name_lookup[name])
            else:
                raise pyfuse3.FUSEError(errno.ENOENT)

        if parent_inode in self.course_inode_lookup:
            return await self.getattr(self.course_inode_lookup[parent_inode].lookup_inode(parent_inode, name))

        return await self.getattr(self.get_course_with_inode(parent_inode).lookup_inode(parent_inode, name))

    async def opendir(self, inode, ctx):
        # TODO: Please don't try to open a file as a directory

        if inode == pyfuse3.ROOT_INODE:
            # Without this `if` statement opening mount point gives "does not exist"
            return pyfuse3.ROOT_INODE

        if inode in self.course_inode_lookup:
            return inode

        # Check that `inode` exists in one of the courses, otherwise this function will throw ENOENT
        _ = self.get_course_with_inode(inode)

        return inode

    async def readdir(self, fh, start_id, token):

        """
        We need same name in same directory to be mapped to same `start_id` consistently
        Accept updated list of files. Merge all courses files together rather than separate struct
        """

        # Root directory contains only course folders
        if fh == pyfuse3.ROOT_INODE:
            # Order in dicts is preserved (for recent Python versions)
            for num, items in enumerate(self.course_name_lookup.items()):
                if num < start_id:
                    continue

                name, inode = items
                if not pyfuse3.readdir_reply(token, name.encode("utf-8"), await self.getattr(inode), num + 1):
                    break
            return

        # Traversal order struct/record

        file_order = list(filter(lambda f: isinstance(f, File), self.wrapper.inodes.values()))
        file_order.sort(key=lambda f: f.id)

        # Discard start
        # TODO: We need to only enumerate the current folder
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
        # Check `inode` exists
        _ = self.get_course_with_inode(inode)

        # Maybe should also check if inode is actually a file?
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        # TODO: Is streaming read possible? (for large files like lectures)
        # Maybe cache in blocks of 10MB?
        # Synchronous read because why not
        return self.get_course_with_inode(fh).read_inode_file(fh, off, size)


class CanvasFSSetup:
    def __init__(self, mount_point):
        self.mount_point = mount_point
        self.courses = []

    def add_course(self, cid, cname):
        # TODO: swap argument order
        self.courses.append((cid, cname))
    
    def start(self, debug=False):

        # Initialise wrapper to Canvas API
        with open("/mnt/files/UoA/Canvas/token.json", "r") as f:
            token = json.load(f)["token"]
        canvas_obj = Canvas("https://canvas.auckland.ac.nz", token)

        # XXX: SETUP ALL THE COURSES
        filesystem = CanvasFS({course_name: FileAccessWrapper(canvas_obj.get_course(course_id)) for course_id, course_name in self.courses})

        fuse_options = set(pyfuse3.default_options)
        fuse_options.add('fsname=canvas_fs')

        if debug:
            fuse_options.add('debug')

        pyfuse3.init(filesystem, self.mount_point, fuse_options)

        try:
            print("Preparing to run filesystem...")
            trio.run(pyfuse3.main)
        except KeyboardInterrupt:
            print("Stopping filesystem...")
        finally:
            pyfuse3.close(unmount=True)



if __name__ == "__main__":
    par = argparse.ArgumentParser()
    par.add_argument("mount_point", type=str)
    args = par.parse_args()

    fs_config = CanvasFSSetup(args.mount_point)
    fs_config.add_course(47152, "MATHS320")
    fs_config.add_course(47211, "MATHS332")
    fs_config.add_course(46253, "COMPSCI320")

    fs_config.start()

    """
    # TODO: When FileAccessWrapper rebuilt, need to invalidate returned handles
    # TODO: Be able to configure display/courses at runtime? Dynamically change directory listings.
    # TODO: Easy way to terminate and automatically unmount
    # TODO: Get course list and find courses supporting `get_files()` automatically and set up dirs
    # TODO: Configurable option whether to keep Canvas folder structure or flatten into file list
    #       per-course configurable; do later
    
    efficient merge operation?
    
    FUSE API is really inflexible
    
    Compose directories, assert mutually exclusive
    
    Cache inode number in .cache / assume persists across sessions
    """