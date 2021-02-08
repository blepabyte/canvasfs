import json
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
        raise ConfigError("Too many command-line arguments: Expecting either none or path to a configuration file")

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
        """
        This function along with the system for managing open files are terrible and really should be improved
        """

        # Note that we are dealing with both filesystem inode numbers, and the ids assigned by Canvas.
        all_files = [Node(bijection_forwards(self.number, f.id), f, self.number) for f in self.course.get_files()]
        all_folders = [Node(bijection_forwards(self.number, f.id), f, self.number) for f in self.course.get_folders()]

        # this should be a dictionary not a list you fucking moron
        self.files = {n.inode: n for n in all_files}

        # There is a root folder called 'course files' with a `parent_id` of None
        canvas_root = None
        for f in all_folders:
            if f.parent() is None:
                # There should be only ONE root
                assert canvas_root is None
                canvas_root = f
        assert canvas_root is not None

        self.subdirectories = {
            folder_node.inode: Subdirectory(
                folder_node,
                list(filter(lambda n: n.parent() == bijection_backwards(folder_node.inode)[1], all_files + all_folders)),
            )
            for folder_node in all_folders
        }
        # The `inode` assigned by bijection and the one derived from the Canvas id for the "root" folder are DIFFERENT
        # This is quite a hacky way to get `readdir` working on the 1st directory level without overriding with special behaviour
        self.subdirectories[self.root_inode] = Subdirectory(
            Node(self.root_inode, canvas_root.obj, self.number),
            list(filter(lambda n: n.parent() == canvas_root.id, all_files + all_folders)),
        )

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
        if not self.inode_belongs(parent_inode):
            return await self.child.lookup(parent_inode, name, ctx)

        found = self.subdirectories[parent_inode].with_name(name.decode('utf-8'))
        fuse_assert(found is not None)
        return found.attributes()

    async def opendir(self, inode, ctx) -> OpenNode:
        if not self.inode_belongs(inode):
            return await self.child.opendir(inode, ctx)

        fuse_assert(inode in self.subdirectories)
        # TODO: Return `weakref` as it's possible Subdirectory will be dumped into garbage when rebuild called
        assert self.subdirectories[inode].inode == inode
        return self.subdirectories[inode]

    async def open(self, inode, flags, ctx) -> OpenNode:
        if not self.inode_belongs(inode):
            return await self.child.open(inode, flags, ctx)

        assert (inode in self.files)
        assert self.files[inode].inode == inode
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
                if name.decode('utf-8') == sub.name:
                    return await sub.getattr(sub.root_inode, ctx)
            raise pyfuse3.FUSEError(errno.ENOENT)

        return await self.root_child.lookup(parent_inode, name, ctx)

    async def opendir(self, inode, ctx):
        if inode == pyfuse3.ROOT_INODE:
            return inode

        handle = await self.root_child.opendir(inode, ctx)
        # use handle.inode?
        self.open_handles[inode] = handle
        assert handle.is_folder()
        logger.debug(f"opendir({inode}) -> {handle.inode}")
        # return inode instead?
        return handle.inode

    async def open(self, inode, flags, ctx):
        handle = await self.root_child.open(inode, flags, ctx)
        self.open_handles[inode] = handle
        assert handle.is_file()
        return pyfuse3.FileInfo(fh=handle.inode)

    # Methods on file handles

    async def releasedir(self, fh):
        if fh != pyfuse3.ROOT_INODE:
            self.open_handles.pop(fh)

    async def release(self, fh):
        self.open_handles.pop(fh)

    async def readdir(self, fh, start_id, token):
        logger.debug(f"readdir({fh})")
        if fh == pyfuse3.ROOT_INODE:
            for num, sub in self.subsystem.items():
                if num < start_id:
                    continue
                if not pyfuse3.readdir_reply(token, sub.name.encode('utf-8'), await sub.getattr(sub.root_inode),
                                             num + 1):
                    return
        else:
            return await self.subsystem[self.open_handles[fh].parent_course_num].readdir(fh, start_id, token)

    async def read(self, fh, off, size):
        out = await self.subsystem[self.open_handles[fh].parent_course_num].read(fh, off, size)
        self.total_bytes_read += len(out)
        return out


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
    debug = config().get("debug", False)
    log_level = "DEBUG" if debug else "INFO"

    logger.remove()
    logger.add(sys.stderr, level=log_level,
               filter={"__main__": 0, "canvasapi": 40, "_pyfuse3": 20, "pyfuse3": 20, "urllib3": 25})
    logging.basicConfig(handlers=[InterceptHandler()], level=0)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=canvasfs')
    if debug:
        fuse_options.add('debug')

    fs = FS.create()
    logger.success("canvasfs initialised. Starting filesystem")
    # breakpoint()

    pyfuse3.init(fs, config()["mount_dir"], fuse_options)


    async def loop():
        # Ensure the files and directories have been fetched from Canvas and setup by the time the filesystem is started
        async with trio.open_nursery() as nursery:
            for sub in fs.subsystem.values():
                nursery.start_soon(sub.build)

        logger.info("Filesystem built for the first time")

        # Background polling to allow periodic refreshes that can be initiated by each SubFS
        async with trio.open_nursery() as nursery:
            nursery.start_soon(pyfuse3.main)

            for sub in fs.subsystem.values():
                nursery.start_soon(sub.poll)


    try:
        trio.run(loop)
    except KeyboardInterrupt:
        logger.success("Stopping filesystem")
    except Exception as err:
        logger.critical("Critical exception while running filesystem")
        logger.exception(err)
    finally:
        pyfuse3.close(unmount=True)

    print(f"""
    canvasfs runtime statistics:
    Current cache size: {cache_size() / 1e9:.3f}GB
    Total bytes read: {fs.total_bytes_read / 1e6:.3f}MB
    """)

"""
# TODO: Invalidate cache when modified on Canvas
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
