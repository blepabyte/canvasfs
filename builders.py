from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from loguru import logger
from canvasapi import canvas

from fs import fs_parent, LocalFolder, datetime_to_fstime
from modules import extract_modules, extract_assignments

"""
---
# Builders

A build function returns: (
    files: {inode::Int => FileTrait}
    folders: {inode::Int => FolderTrait}
    dirfunc: FSTrait -> inode::Int
)

`dirfunc` returns the inode of the parent of the input filesystem object

FileTrait, FolderTrait <: FSTrait and must implement `fs_attributes`, `fs_name`, etc...

---
# Combining builders

- Take (djsoint) union of files, folders.
- Remap `dirfunc` output. Remap `root_folder` to an actual folder object with parent as actual root. 
To be completely rigorous could apply bijection again, but the current formula grows in magnitude too quickly and will exceed INT64_MAX. 

## Example

Can push assignment build output into "Assignments" folder and splat with files

---
TODO: Builds need to handle name and file duplicates
"""


class RootError(Exception):
    pass


class BuildOutput:
    def __init__(self, files, folders, dirfunc):
        self.files, self.folders, self.dirfunc = files, folders, dirfunc

    def __iter__(self):
        """
        Behave as if BuildOutput was just the tuple (self.files, self.folders, self.dirfunc)
        """
        yield self.files
        yield self.folders
        yield self.dirfunc

    def root(self) -> int:
        """
        Returns inode of root folder
        """
        for k, v in self.folders.items():
            # Calling dirfunc on root node should either throw ValueError or return its own inode
            try:
                # On allowing dirfunc to return sets, we are still assuming every path must eventually return to the root (no cycles)
                _pids = self.dirfunc(v)
            except RootError:
                return k
            if k in _pids:
                return k

        raise RootError("No root directory found")

    def push(self, inode, name):
        """
        Adds a new directory in between the "current root" and the rest of the files
        """
        files, folders = {**self.files}, {**self.folders}
        assert inode not in folders

        original_root_inode = self.root()
        original_root = folders[original_root_inode]

        pushed = LocalFolder(inode, name)
        # Preserve attributes of original root. Assumes `original_root` is also a LocalFolder
        pushed.attributes.st_mtime_ns = original_root.attributes.st_mtime_ns
        folders[inode] = pushed

        def dirfunc(child):
            # We again assume that `child` will never be the root inode. Is up to self.dirfunc to raise ValueError
            if child == original_root:
                raise RootError("Tried to call dirfunc on pushed root")
            if child == pushed:
                return {original_root_inode}
            return {inode if o == original_root_inode else o for o in self.dirfunc(child)}

        return BuildOutput(files, folders, dirfunc)

    @staticmethod
    def splat(*builds: [BuildOutput]) -> BuildOutput:
        """
        Smoosh build outputs together.

        Assumes no inode clashes other than the root nodes (assumed same between splat inputs)
        """

        """
        Problem: If files appear both in the "Files" tab and in "Modules" then they only appear once in canvasfs (because dirfunc lookup is overwritten). Can a file be listed in more than one directory? Probably (symlinks are a thing). However the design of dirfunc means each file can be assigned to only one parent directory. Can modify to return a set? (now no longer a rooted tree but who cares)
        
        This would allow some new features like tagging/grouping/filtering. 
        """

        # TODO: Handle duplicates by duplicating letters or inserting '>' at start

        files, folders = {}, {}

        # The same file might appear in multiple builds - WRONG
        # The files get mapped to the SAME inode, yet the objects themselves are not EQUAL
        dlookup = defaultdict(lambda: set())

        for b in builds:

            # files.update(b.files)

            # duplicate folders seems extremely unlikely
            folders.update(b.folders)

            for i, f in b.files.items():
                if i in files:
                    # Canonicalise, assuming files with the same inode are identical
                    # Since files[i] != f due to improper __eq__ implementation in canvasapi
                    dlookup[files[i]].update({b.dirfunc})
                else:
                    files[i] = f
                    dlookup[f].update({b.dirfunc})

            for f in b.folders.values():
                dlookup[f].update({b.dirfunc})

        # PROBLEM: We don't know which builder the input comes from, so we don't know which `dirfunc` to call
        # Inefficient solution: lookup table; assumes that file and folder objects are hashable
        # Actually, poor performance is irrelevant as dirfunc is only called during the initial build
        def dirfunc(child):
            return set.union(*(df(child) for df in dlookup[child]))

        # Fairly hacky workaround to get last modified times of course folders working
        b = BuildOutput(files, folders, dirfunc)
        b_root = b.folders[b.root()]
        if isinstance(b_root, LocalFolder):
            b_root.attributes.st_mtime_ns = datetime_to_fstime(latest_modified(files.values()))
        else:
            logger.warning("Failed to preserve mtime: Course root has wrong type")
        return b

    @staticmethod
    def empty():
        def dirfunc(_child):
            raise ValueError("Empty build has no contents, so dirfunc should not be called")

        return BuildOutput({}, {}, dirfunc)


def latest_modified(fs):
    # TODO: Will fail if any element of `fs` is not of canvas.File type
    # TODO: Set default to be 1970 or something so it doesn't mess up sorting
    return max((f.modified_at_date for f in fs), default=datetime.now(timezone.utc))


def create_course_root(course: canvas.Course, files, root_inode) -> LocalFolder:
    root_folder = LocalFolder(root_inode, "PLACEHOLDER")
    root_folder.attributes.st_birthtime_ns = datetime_to_fstime(course.created_at_date)
    root_folder.attributes.st_mtime_ns = datetime_to_fstime(latest_modified(files.values()))

    return root_folder


# This function used to be async, but it was pointless since `get_files` and `get_folders` are blocking. Even the module extractors are all synchronous. Will probably just wrap in a thread and call it a day.
def build_default(course: canvas.Course, inode_mapper) -> BuildOutput:
    files = {inode_mapper(file.id): file for file in course.get_files()}
    folders = {inode_mapper(folder.id): folder for folder in course.get_folders()}

    root_inode = inode_mapper(0)
    root_folder = create_course_root(course, files, root_inode)
    folders[root_inode] = root_folder

    # There should always be a root folder in Canvas called 'course files' with a `parent_id` of None
    # We want to replace that with our `root_folder`
    canvas_root_id = None
    for f in folders.values():  # I keep forgetting that these are dicts...
        if f == root_folder:
            continue
        if fs_parent(f) is None:
            if canvas_root_id is not None:
                logger.error(f"Multiple folder roots found in course: {course}")
                logger.error(course)
            canvas_root_id = f.id
    if canvas_root_id is None:
        logger.error(f"No folder root found in course: {course}")
    else:
        logger.info(f"Got `canvas_root_id` of {canvas_root_id} for course: {course}")

    # the 'course files' folder is useless so we may as well remove it, rather than setting its parent to a directory that doesn't exist. That would actually be a pretty neat way of "deleting" files
    folders.pop(inode_mapper(canvas_root_id))

    def dirfunc(child):
        if child == root_folder:
            raise RootError("Lookups on SubFS root inode must be special-cased")

        parent_folder_id = fs_parent(child)
        # If we could not find `canvas_root_id`, it's possible nothing gets attached to our root folder, which is not good...
        # Cannot give None to `inode_mapper`: (crid is not None) => (pfid is not None)
        assert (canvas_root_id is None) or (parent_folder_id is not None)
        if parent_folder_id == canvas_root_id:
            parent_folder_id = 0
        return {inode_mapper(parent_folder_id)}

    return BuildOutput(files, folders, dirfunc)


def build_flat(course: canvas.Course, inode_mapper) -> BuildOutput:
    files = {inode_mapper(file.id): file for file in course.get_files()}

    root_inode = inode_mapper(0)
    root_folder = create_course_root(course, files, root_inode)

    folders = {root_inode: root_folder}  # lookups and readdirs of the root are handled by the FS instance so the name is irrelevant. This allows us to avoid special-casing operations on the root

    dirfunc = lambda _: {root_inode}

    return BuildOutput(files, folders, dirfunc)


def build_from_modules(course: canvas.Course, inode_mapper) -> BuildOutput:
    root_inode = inode_mapper(0)
    files = {inode_mapper(file.id): file for file in extract_modules(course)}
    folders = {root_inode: create_course_root(course, files, root_inode)}

    dirfunc = lambda _: {root_inode}

    return BuildOutput(files, folders, dirfunc)


def build_from_assignments(course: canvas.Course, inode_mapper) -> BuildOutput:
    root_inode = inode_mapper(0)
    files = {inode_mapper(file.id): file for file in extract_assignments(course)}
    folders = {root_inode: create_course_root(course, files, root_inode)}

    dirfunc = lambda _: {root_inode}

    return BuildOutput(files, folders, dirfunc)


def build_markdown(course: canvas.Course, inode_mapper) -> BuildOutput:
    """
    An idea I had was to convert the HTML module pages into markdown and save as .md files
    """
    raise NotImplementedError()


def build_combined(course: canvas.Course, inode_mapper, build_files=True) -> BuildOutput:
    # TODO: try/except to continue if one of the builds fails
    return BuildOutput.splat(
        build_from_assignments(course, inode_mapper).push(inode_mapper(7), "Assignments"),
        build_from_modules(course, inode_mapper).push(inode_mapper(8), "Modules"),
        build_default(course, inode_mapper) if build_files else BuildOutput.empty(),
    )


def build_combined_flat(course: canvas.Course, inode_mapper, build_files=True) -> BuildOutput:
    return BuildOutput.splat(
        build_from_assignments(course, inode_mapper),
        build_from_modules(course, inode_mapper),
        build_flat(course, inode_mapper) if build_files else BuildOutput.empty(),
    )
