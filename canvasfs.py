import os, sys, weakref
from collections import defaultdict
from typing import Union
from datetime import datetime
from pathlib import Path
from time import perf_counter

import trio
import pyfuse3
import canvasapi

import logging
from loguru import logger

from fs import DirectoryListing, LocalFolder, fs_name, fs_attributes
from fs import bijection_forwards, default_folder_entry, datetime_to_fstime, fuse_assert
from config import ConfigError, config, cache_root, cache_size, process_course_configs

PYFUSE_ROOT_ATTR = default_folder_entry(pyfuse3.ROOT_INODE)


class NullFS:
    """
    Dummy filesystem with nothing in it. At the end of the lookup chain and just throws "Not Found"
    """

    def inode_belongs(self, _inode) -> bool:
        return False

    async def getattr(self, _inode, _ctx=None):
        fuse_assert(False)

    async def lookup(self, _parent_inode, _name, _ctx=None):
        fuse_assert(False)

    async def opendir(self, _inode, _ctx):
        fuse_assert(False)

    async def open(self, _inode, _flags, _ctx):
        fuse_assert(False)


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
        self_setup_params, *rest_params = init_list
        self.number = num
        self.course = self_setup_params["course"]
        self.builder = self_setup_params["builder"]

        self.root_inode = bijection_forwards(self.number, 0)

        self.name = self_setup_params["name"] if "name" in self_setup_params else self.course.course_code

        if rest_params:
            self.child = SubFS(rest_params, num + 1)
        else:
            self.child = NullFS()

        self.files: {int, canvasapi.canvas.File} = {}
        self.folders: {int, Union[canvasapi.canvas.Folder, LocalFolder]} = {}
        self.listings: {int, DirectoryListing} = defaultdict(lambda: DirectoryListing([]))

    def inode_belongs(self, inode) -> bool:
        return inode in self.files or inode in self.folders

    async def poll(self):
        interval = config().get("refresh_interval", 4)
        if interval <= 0:
            return

        # TODO: Automatically rebuild at "unlock_at" times

        while True:
            # If the laptop goes into sleep mode, will this still run at the correct time? Probably; it would be inefficient to actually count how long it's been waiting rather than just comparing against system time
            await trio.sleep(interval * 60 * 60)
            await self.build()

    async def build(self):
        logger.info(f"Build started for course: {self.name}")
        self.listings.clear()

        inode_mapper = lambda x: bijection_forwards(self.number, x)
        # Run build in separate thread as it blocks
        self.files, self.folders, dirfunc = await trio.to_thread.run_sync(self.builder, self.course, inode_mapper)

        # Build directory listings for `readdir` calls

        for inode, file in self.files.items():
            self.listings[dirfunc(file)].add(inode, fs_name(file))

        for inode, folder in self.folders.items():
            if inode == self.root_inode:
                continue
            self.listings[dirfunc(folder)].add(inode, fs_name(folder))

        logger.info(f"Build completed for course: {self.name}")

    # Passthrough methods

    async def getattr(self, inode, ctx=None):
        if not self.inode_belongs(inode):
            return await self.child.getattr(inode, ctx)

        if inode in self.files:
            return fs_attributes(self.files[inode], inode)

        if inode in self.folders:
            return fs_attributes(self.folders[inode], inode)

        assert False, inode

    async def lookup(self, parent_inode, name, ctx=None):
        if not self.inode_belongs(parent_inode):
            return await self.child.lookup(parent_inode, name, ctx)

        found_inode = self.listings[parent_inode].name_to_inode(name.decode('utf-8'))
        fuse_assert(found_inode is not None)
        return await self.getattr(found_inode)

    async def opendir(self, inode, ctx):
        if not self.inode_belongs(inode):
            return await self.child.opendir(inode, ctx)

        assert inode in self.folders
        return inode, weakref.ref(self)

    async def open(self, inode, flags, ctx):
        if not self.inode_belongs(inode):
            return await self.child.open(inode, flags, ctx)

        assert inode in self.files

        # Checks cache and purges if out of date
        file = self.files[inode]
        cache_path = cache_root() / str(file.id)
        if cache_path.exists() and cache_path.stat().st_mtime_ns != (await self.getattr(inode)).st_mtime_ns:
            logger.info(f"File `{fs_name(file)}` is out of date. Purging...")
            cache_path.unlink()

        return inode, weakref.ref(self)

    # Methods on file handles

    async def readdir(self, fh, start_id, token):
        for inode, name in self.listings[fh].list_from(start_id):
            if not pyfuse3.readdir_reply(token, name.encode('utf-8'), await self.getattr(inode), inode + 1):
                return

    async def read(self, fh, off, size):
        target_file = self.files[fh]
        cache_path = cache_root() / str(target_file.id)

        if cache_path.exists():
            with open(cache_path, "rb") as f:
                f.seek(off)
                return f.read(size)

        # Not in cache so must fetch from network
        # Synchronous network request wrapped in a trio thread

        def fetch():
            ts = perf_counter()
            # fail nicely if no connection (e.g. on resume from suspend)
            # TODO: handle network failures in other places as well (spurious failures)
            try:
                data = target_file.get_contents(binary=True)
            except Exception as e:
                logger.exception(e)
                fuse_assert(False)
            elapsed = perf_counter() - ts
            logger.info(f"Fetched '{fs_name(target_file)}' over network in {elapsed:.3f}s @ {len(data) / 1e6 / elapsed:.3f}MB/s")
            return data

        with open(cache_path, "wb") as f:
            f.write(await trio.to_thread.run_sync(fetch))

        # Allows cache invalidation when `mtime` on Canvas changes
        os.utime(cache_path, ns=(datetime_to_fstime(datetime.now()), (await self.getattr(fh)).st_mtime_ns))

        # No point rewriting the cache read from above. Also avoids indexing errors
        return await self.read(fh, off, size)


class FS(pyfuse3.Operations):
    supports_dot_lookup = False

    def __init__(self, courses_config):
        super().__init__()

        self.root_child = SubFS(courses_config)
        self.open_handles = {}  # a handle is an (inode, Weakref<SubFS>) pair
        self.total_bytes_read = 0

    def __iter__(self):
        """
        Iterates through all its SubFS instances
        """
        c = self.root_child
        while not isinstance(c, NullFS):
            yield c.number, c
            c = c.child

    # Passthrough methods

    async def getattr(self, inode, ctx=None):
        if inode == pyfuse3.ROOT_INODE:
            return PYFUSE_ROOT_ATTR

        return await self.root_child.getattr(inode, ctx)

    async def lookup(self, parent_inode, name, ctx=None):
        if parent_inode == pyfuse3.ROOT_INODE:
            for _, sub in self:
                if name.decode('utf-8') == sub.name:
                    return await sub.getattr(sub.root_inode, ctx)
            fuse_assert(False)

        return await self.root_child.lookup(parent_inode, name, ctx)

    async def opendir(self, inode, ctx):
        logger.trace(f"opendir({inode})")
        if inode == pyfuse3.ROOT_INODE:
            return inode

        handle = await self.root_child.opendir(inode, ctx)
        self.open_handles[inode] = handle
        return inode

    async def open(self, inode, flags, ctx):
        handle = await self.root_child.open(inode, flags, ctx)
        self.open_handles[inode] = handle
        return pyfuse3.FileInfo(fh=inode)

    # Methods on file handles

    async def releasedir(self, fh):
        if fh != pyfuse3.ROOT_INODE:
            # self.open_handles.pop(fh, None)
            pass

    async def release(self, fh):
        # self.open_handles.pop(fh, None)
        pass

    async def readdir(self, fh, start_id, token):
        # TODO: Could probably implement this with a `DirectoryListing` as well. Class decorator to monkey-patch method from a .listing field?
        logger.trace(f"readdir({fh})")
        if fh == pyfuse3.ROOT_INODE:
            for num, sub in self:
                if num < start_id:
                    continue
                if not pyfuse3.readdir_reply(token, sub.name.encode('utf-8'), await sub.getattr(sub.root_inode), num + 1):
                    return
        else:
            fuse_assert(fh in self.open_handles)
            inode, deref = self.open_handles[fh]
            # deref() is not None as each SubFS instance is in memory for the life of the program
            return await deref().readdir(inode, start_id, token)

    async def read(self, fh, off, size):
        # Some applications seem to close files and then try to read from them...
        fuse_assert(fh in self.open_handles)
        inode, deref = self.open_handles[fh]
        read_data = await deref().read(inode, off, size)
        self.total_bytes_read += len(read_data)
        return read_data


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

    # Call config before any logging begins, to run prompt if file does not exist
    debug = config().get("debug", False)
    log_level = "DEBUG" if debug else "INFO"

    logger.remove()
    logger.add(sys.stderr, level=log_level,
               filter={"__main__": 0, "canvasapi": 20 if debug else 30, "_pyfuse3": 30, "pyfuse3": 30, "urllib3": 30})
    logging.basicConfig(handlers=[InterceptHandler()], level=0)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=canvasfs')
    # if debug:
    #     fuse_options.add('debug')

    try:
        root_fs = FS(process_course_configs())
    except Exception as err:
        # We want any exception that is thrown to go through `better_exceptions`, which provides debug information that's **actually useful**
        logger.exception(err)
        raise

    logger.info("canvasfs initialised. Starting filesystem")

    mount_target = Path(config()["mount_dir"])
    mount_target.mkdir(parents=True, exist_ok=True)
    if len(list(mount_target.iterdir())) > 0:
        raise ConfigError("'mount_dir' must be empty, as its contents will be overwritten when mounted")

    pyfuse3.init(root_fs, str(mount_target), fuse_options)


    async def loop():
        # Ensure the files and directories have been fetched from Canvas and setup by the time the filesystem is started
        async with trio.open_nursery() as nursery:
            for _, sub in root_fs:
                nursery.start_soon(sub.build)

        logger.success("Filesystem built. Ready to go!")

        # Background polling to allow periodic refreshes that can be initiated by each SubFS
        async with trio.open_nursery() as nursery:
            nursery.start_soon(pyfuse3.main)

            for _, sub in root_fs:
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

    logger.info(f"canvasfs runtime statistics")
    logger.info(f"Current cache size: {cache_size() / 1e9:.3f}GB")
    logger.info(f"Total bytes read: {root_fs.total_bytes_read / 1e6:.3f}MB")
