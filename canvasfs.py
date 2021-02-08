import json
import argparse
import functools
from time import perf_counter
import os
import sys
import stat
import errno
import pathlib
from datetime import datetime
import trio
import pyfuse3
import canvasapi
from canvasapi import Canvas
from canvasapi.course import Course
from canvasapi.file import File
from canvasapi.folder import Folder
import logging
from loguru import logger
import weakref

from fs import Node, OpenNode, Subdirectory
from fs import bijection_forwards, bijection_backwards, default_inode_entry, datetime_to_fstime, fuse_assert

NANOSECONDS = 1e9
CACHE_LOCATION = "/mnt/storage/.canvas_fs"

PYFUSE_ROOT_ATTR = default_inode_entry(pyfuse3.ROOT_INODE)
PYFUSE_ROOT_ATTR.st_mode = (stat.S_IFDIR | 0o755)
PYFUSE_ROOT_ATTR.st_size = 0


class ConfigError(Exception):
    pass


def config() -> dict:
    if config.CONFIG is not None:
        return config.CONFIG

    # Load config file for the first time
    if len(sys.argv) == 1:
        config_location = "config.json"
    elif len(sys.argv) == 2:
        config_location = sys.argv[1]
    else:
        raise ConfigError(
            "Too many command-line arguments: Expecting either none or path to a configuration file")

    with open(config_location) as f:
        config.CONFIG = json.load(f)

    return config()


config.CONFIG = None


def canvas() -> canvasapi.canvas.Canvas:
    if canvas.CANVAS is not None:
        return canvas.CANVAS

    token = config()["token"]
    domain = config()["domain"]
    canvas.CANVAS = Canvas(domain, token)
    return canvas()


canvas.CANVAS = None


def cache_size() -> int:
    """Computes total size of cache in bytes"""
    cache_location = config()["cache_dir"]
    return sum(os.stat(f"{cache_location}/{node}").st_size for node in os.listdir(cache_location))


# class DoubleDict:
#     def __init__(self):
#         self.left_entries = {}
#         self.right_entries = {}
#
#     def insert(self, left, right):
#         self.left_entries[left] = right
#         self.right_entries[right] = left
#
#     def get_l(self, left):
#         return self.left_entries[left]
#
#     def get_r(self, right):
#         return self.right_entries[right]


# TODO: Move hacky inode indexing to INodeStore
class FileAccessWrapper:
    """
    Handle folder structure, file names/ids, and caching
    """

    def __init__(self, course_name, course_obj):
        self.course = course_obj

        self.name = course_name

        self.inodes = {}
        self.name_index = {}  # { name => inode }
        self.build_fs_tree()

    def build_fs_tree(self):
        """
        Call to this function is only necessary if remote files on Canvas have been changed
        Maybe call this every 30 minutes? Notify on change -- would need to comapre equality of FS trees

        UPDATE: Only call if directory is actually accessed / lookup call made. Limit server refreshes to 15 minutes. 

        Await both fixed interval and poll request
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
        logger.trace(f"{name}")
        # TODO: Currently ignoring `parent_inode` assuming all files in same dir
        try:
            return self.name_index[name]
        except KeyError as e:
            logger.debug(f"{name} does not exist in {self.name}")
            raise pyfuse3.FUSEError(errno.ENOENT)

    def file_contents(self, inode):
        assert isinstance(self.inodes[inode], File)
        return self.inodes[inode].get_contents(binary=True)

    def read_inode_file(self, fh, off, size):
        # TODO: Is streaming/asynchronous read from server possible? (for large files like lectures)

        try:
            with open(f"{CACHE_LOCATION}/{fh}", "rb") as f:
                f.seek(off)
                return f.read(size)
        except FileNotFoundError:
            # TODO: Check date modified and refresh from server if new

            ts = perf_counter()
            contents = self.file_contents(fh)
            logger.success(
                f"Fetched {self.inodes[fh].filename} from Canvas API in {perf_counter() - ts:.3f}s")

            with open(f"{CACHE_LOCATION}/{fh}", "wb") as f:
                f.write(contents)
            return contents[off:off + size]

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
            entry.st_size = node.size  # Assume the Canvas API returns size in bytes

            entry.st_ctime_ns = node.created_at_date.timestamp() * NANOSECONDS
            entry.st_mtime_ns = node.modified_at_date.timestamp() * NANOSECONDS
            return entry

        raise RuntimeError("Some weird thing going on with the inode tree...")

    def readdir(self, fh, start_id, token):
        assert fh == 0

        logger.trace(f"directory listing: {self.name} @ {start_id}")

        # TODO: Sub-directory support
        file_order = list(
            filter(lambda f: isinstance(f, File), self.inodes.values()))
        file_order.sort(key=lambda f: f.id)

        if start_id >= len(file_order):
            logger.debug(
                f"start_id {start_id} is past end of directory listing")
            # raise pyfuse3.FUSEError(errno.ENOENT)
            return

        # Discard start
        it = enumerate(file_order)
        for _ in range(start_id):
            next(it)

        # Return next node ID in reply directly rather than arbitrary count
        for num, file in it:
            # Expects bytes as name
            logger.trace(f"[{num}] {file.filename}")
            if not pyfuse3.readdir_reply(token, file.filename.encode('utf-8'), self.getattr(file.id), num + 1):
                break


class NullFS:
    def inode_belongs(self, _inode) -> bool:
        return False

    async def getattr(self, inode, ctx=None):
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def lookup(self, parent_inode, name, ctx=None):
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def opendir(self, inode, ctx):
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def open(self, inode, flags, ctx):
        raise pyfuse3.FUSEError(errno.ENOENT)


class SubFS:
    """
    Each SubFS is responsible only for the inodes for which its `inode_belongs` method returns True. It follows that inodes should be unique between different SubFS instances. 

    The following methods on inodes are directly passed through from the root FS instance and should be passed to its child if the target inode does not belong. 
    - getattr
    - lookup
    - opendir
    - open
    """

    def __init__(self, init_list, num=2):

        assert len(init_list) > 0
        this_course, *rest = init_list
        self.id = this_course["id"]
        self.number = num

        self.cache_root = pathlib.Path(config()["cache_dir"]) / str(self.id)
        self.cache_root.mkdir(parents=True, exist_ok=True)

        self.root_inode = bijection_forwards(self.number, 0)

        self.course = canvas().get_course(self.id)
        self.has_subdirectories = this_course.get("subdirectories", False)

        self.name = this_course["name"] if "name" in this_course else self.course.course_code

        if rest:
            self.child = SubFS(rest, num + 1)
        else:
            self.child = NullFS()

        # if not has_subdirectories this is just the root inode -> all files
        self.subdirectories: {int, Subdirectory} = {}  # { folder_inode -> Subdirectory <: Node }
        self.files: {int, Node} = {}  # { file_inode -> FileNode <: Node }

    def inode_belongs(self, inode) -> bool:
        return inode in self.files or inode in self.subdirectories or inode == self.root_inode

    async def poll(self):
        while True:
            await trio.sleep(60 * 10)
            await self.build()

    async def build(self):
        all_files = [Node(bijection_forwards(self.number, f.id), f) for f in self.course.get_files()]
        all_folders = [Node(bijection_forwards(self.number, f.id), f) for f in self.course.get_folders()]

        self.files = all_files
        self.subdirectories = {
            folder_node.inode: Subdirectory(
                list(filter(lambda n: n.parent() == folder_node.inode, all_files + all_folders))
            )
            for folder_node in all_folders
        }

    # Passthrough methods

    async def getattr(self, inode, ctx=None):
        if not self.inode_belongs(inode):
            return await self.child.getattr(inode, ctx)

        if inode == self.root_inode:
            entry = default_inode_entry(inode)
            entry.st_mode = (stat.S_IFDIR | 0o755)
            entry.st_size = 0

            entry.st_ctime_ns = datetime_to_fstime(self.course.created_at_date)
            # TODO: mtime should be set to most recent modification out of all contained files
            return entry

        if inode in self.files:
            return self.files[inode].attributes()

        if inode in self.subdirectories:
            return self.subdirectories[inode].attributes()

        assert False, inode

    async def lookup(self, parent_inode, name, ctx=None):
        found = self.subdirectories[parent_inode].with_name(name.decode('utf-8'))
        fuse_assert(found is not None)
        return found.attributes()

    async def opendir(self, inode, ctx) -> OpenNode:
        fuse_assert(inode in self.subdirectories)
        # TODO: Return `weakref` as it's possible Subdirectory will be dumped into garbage when rebuild called
        return self.subdirectories[inode]

    async def open(self, inode, flags, ctx) -> OpenNode:
        fuse_assert(inode not in self.files)
        return self.files[inode]

    # Methods on file handles

    async def readdir(self, fh, start_id, token):
        for entry in self.subdirectories[fh].list_files(start_id):
            if not pyfuse3.readdir_reply(token, entry.name.encode('utf-8'), entry.attributes(), entry.inode + 1):
                return

    async def read(self, fh, off, size):
        # try to return read from cache
        # todo: check if modified should happen during build() not here
        cache_path = self.cache_root / str(bijection_backwards(fh)[1])
        if cache_path.exists():
            with open(cache_path, "rb") as f:
                f.seek(off)
                return f.read(size)

        # not found in cache so fetch from network
        def fetch():
            ts = perf_counter()
            file = self.files[fh].obj
            data = file.get_contents(binary=True)
            logger.info(f"Fetched '{file.filename}' over network in {perf_counter() - ts:.3f}s")
            return data

        with open(cache_path, "wb") as f:
            f.write(await trio.to_thread.run_sync(fetch))
        # no point rewriting the cache read from above. also prevents indexing errors
        return await self.read(fh, off, size)


class FS(pyfuse3.Operations):
    supports_dot_lookup = False

    def __init__(self, courses_config):
        super().__init__()

        self.root_child = SubFS(courses_config)
        self.subsystem = {}  # { course_num -> SubFS }
        self.open_handles = {}  # { inode -> OpenNode }

        cur_child = self.root_child
        while not isinstance(cur_child, NullFS):
            self.subsystem[cur_child.number] = cur_child
            cur_child = cur_child.child

        self.total_bytes_read = 0

    @staticmethod
    def create():
        """
        Sets up an FS instance. If no courses are provided in the config this may take a while, as each course needs to be checked for an accessible Files tab synchronously
        # TODO: Parallelise checks via async threads
        """

        # TODO: In the case of non-accessible files, an alternative can be scraping all the module pages for links to files and just dumping them? (override build?)
        def has_accessible_files(course_config) -> bool:
            target_course = canvas().get_course(course_config["id"])
            try:
                _f = list(target_course.get_files())
            except canvasapi.exceptions.Unauthorized:
                logger.warning(f"The course '{target_course.name}' does not have an accessible files tab")
                return False
            return True

        if "courses" in config():
            courses_config = config()["courses"]
        else:
            # User did not specify which courses to mount, so try all of them.
            courses_config = []
            active_courses = list(canvas().get_courses())
            for c in active_courses:
                courses_config.append({
                    "id": c.id
                })

        return FS(list(filter(has_accessible_files, courses_config)))

    # Passthrough methods

    async def getattr(self, inode, ctx=None):
        if inode == pyfuse3.ROOT_INODE:
            return PYFUSE_ROOT_ATTR

        return await self.root_child.getattr(inode, ctx)

    async def lookup(self, parent_inode, name, ctx=None):
        if parent_inode == pyfuse3.ROOT_INODE:
            for sub in self.subsystem.values():
                if name == sub.name:
                    return await sub.getattr(sub.root_inode, ctx)
            raise pyfuse3.FUSEError(errno.ENOENT)

        return await self.root_child.lookup(parent_inode, name, ctx)

    async def opendir(self, inode, ctx):
        if inode == pyfuse3.ROOT_INODE:
            return inode

        handle = await self.root_child.opendir(inode, ctx)
        self.open_handles[inode] = handle
        assert handle.is_folder()
        return handle.inode

    async def open(self, inode, flags, ctx):
        handle = await self.root_child.open(inode, flags, ctx)
        self.open_handles[inode] = handle
        assert handle.is_file()
        return pyfuse3.FileInfo(fh=handle.inode)

    # Methods on file handles

    async def releasedir(self, fh):
        self.open_handles.pop(fh)

    async def release(self, fh):
        self.open_handles.pop(fh)

    async def readdir(self, fh, start_id, token):
        if fh == pyfuse3.ROOT_INODE:
            for num, sub in self.subsystem.items():
                if num < start_id:
                    continue
                if not pyfuse3.readdir_reply(token, sub.name.encode('utf-8'), await sub.getattr(sub.root_inode),
                                             num + 1):
                    return

        return await self.subsystem[self.open_handles[fh].parent_course_num].readdir(fh, start_id, token)

    async def read(self, fh, off, size):
        out = await self.subsystem[self.open_handles[fh].parent_course_num].read(fh, off, size)
        self.total_bytes_read += len(out)
        return out


"""
IDEAS/WIP

- Give FileAccessWrapper a `refresh` async method that can be called periodically

- Recursively initialise wrappers with each pointing to "remainder" node, with a final node that just NOPs. This is elegant, and not too inefficient since # courses will be low, but for lookup calls might add non-negligible overhead... Maybe a mix of both approaches depending on the method being called? 
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
                assert found is None
                found = file_wrapper

        if found is not None:
            return found
        else:
            logger.error(
                f"Tried to find inode {inode} in sub-courses but it does not exist")
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
        assert isinstance(name, bytes)

        name = name.decode()
        logger.trace(f"{parent_inode} -> {name}")

        if parent_inode == pyfuse3.ROOT_INODE:

            if name in self.course_name_lookup:
                return await self.getattr(self.course_name_lookup[name])
            else:
                logger.debug(f"{name} not found in root directory")
                raise pyfuse3.FUSEError(errno.ENOENT)

        if parent_inode in self.course_inode_lookup:
            return await self.getattr(self.course_inode_lookup[parent_inode].lookup_inode(parent_inode, name))

        # This shouldn't really even happen since there are no sub-directories
        logger.error(f"lookup of {name} with no known parent")
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

        # Otherwise delegate to separate course
        if fh in self.course_inode_lookup:
            # for now assume no sub-folders
            self.course_inode_lookup[fh].readdir(0, start_id, token)
            return

        _ = self.get_course_with_inode(fh)
        raise NotImplementedError(
            "Sub-directories within course folders are not supported yet!")

    async def open(self, inode, flags, ctx):
        # Check `inode` exists
        _ = self.get_course_with_inode(inode)

        # Maybe should also check if inode is actually a file?
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, off, size):
        logger.trace(f"({fh, off, size})")
        # Delegate to individual course
        return self.get_course_with_inode(fh).read_inode_file(fh, off, size)


# TODO: Deprecate
class CanvasFSSetup:
    def __init__(self, mount_point):
        self.mount_point = mount_point
        self.courses = []

    def add_course(self, cid, cname):
        self.courses.append((cid, cname))

    def start(self, debug=False):

        # Initialise wrapper to Canvas API
        with open("/mnt/files/UoA/Canvas/token.json", "r") as f:
            token = json.load(f)["token"]
        canvas_obj = Canvas("https://canvas.auckland.ac.nz", token)

        # XXX: SETUP ALL THE COURSES
        # TODO: shared references to wrappers
        access_wrappers = []

        filesystem = CanvasFS({
            course_name: FileAccessWrapper(
                course_name, canvas_obj.get_course(course_id))
            for course_id, course_name in self.courses
        })

        fuse_options = set(pyfuse3.default_options)
        fuse_options.add('fsname=canvas_fs')

        if debug:
            fuse_options.add('debug')

        pyfuse3.init(filesystem, self.mount_point, fuse_options)

        def main():
            with trio.open_nursery() as nursery:
                nursery.start_soon(pyfuse3.main)
                for w in access_wrappers:
                    """
                    FileAccessWrapper.poll is an async method that lets each course decide when it should update its own data
                    """
                    nursery.start_soon(w.poll)

        try:
            logger.success("CanvasFS initialised. Starting filesystem...")
            trio.run(main)
        except KeyboardInterrupt:
            logger.success("Stopping filesystem.")
        except Exception as err:
            logger.critical("Unexpected exception while running filesystem")
            logger.exception(err)
        finally:
            pyfuse3.close(unmount=True)

        print(f"""
Statistics:
Current cache size: {cache_size() / 1e9:.3f} GB
Total bytes read: <NOT YET SUPPORTED>
""")


class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage())


if __name__ == "__main__":
    """
    [Logging levels](https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add)
    ===
    Level name 	value 	Logger method
    TRACE 	    5 	    logger.trace()
    DEBUG 	    10 	    logger.debug()
    INFO 	    20 	    logger.info()
    SUCCESS 	25 	    logger.success()
    WARNING 	30 	    logger.warning()
    ERROR 	    40 	    logger.error()
    CRITICAL 	50 	    logger.critical()
    """

    fs = FS.create()

    exit()

    par = argparse.ArgumentParser()
    par.add_argument("mount_point", type=str)
    par.add_argument("--debug", action="store_true")
    args = par.parse_args()

    if args.debug:
        log_level = "DEBUG"
    else:
        log_level = "INFO"

    logger.remove()
    logger.add(sys.stderr, level=log_level,
               filter={"__main__": 0, "canvasapi": 50, "_pyfuse3": 0, "pyfuse3": 0, "urllib3": 25})
    logging.basicConfig(handlers=[InterceptHandler()], level=0)

    fs_config = CanvasFSSetup(args.mount_point)
    fs_config.add_course(47152, "MATHS320")
    fs_config.add_course(47211, "MATHS332")
    fs_config.add_course(46253, "COMPSCI320")

    fs_config.start()

    # TODO: Invalidate cache when modified on Canvas

    """
    # TODO: When FileAccessWrapper rebuilt, need to invalidate returned handles
    # TODO: Get course list and find courses supporting `get_files()` automatically and set up dirs
    
    Ideas:
    # TODO: Be able to configure display/courses at runtime? Dynamically change directory listings.
    
    # TODO: Configurable option whether to keep Canvas folder structure or flatten into file list
    #       per-course configurable; do later
    # TODO: Add thoroughput MB/s to logging information
    efficient merge operation?
    
    FUSE API is really inflexible
    
    Compose directories, assert mutually exclusive
    
    Use absolute path within course as unique identifier instead of "id" - might not catch renames but more robust
    """


class InvalidConfig(Exception):
    @staticmethod
    def nonempty(value, message):
        if not value:
            raise InvalidConfig(message)
        return value


def start_from_config(config: dict):
    def get(k, e): return InvalidConfig.nonempty(config.get(k), e)

    """
    While the recommended way to access the Canvas API is via an OAuth2 generated token, 
    1) I don't know how OAuth works
    2) I don't have a developer key and doubt I could convince anyone to give me one 
    """
    canvas = Canvas(
        get("domain", "config file is missing key 'domain'"),
        get("token", "Canvas token was not provided")
    )
