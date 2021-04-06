from typing import Callable
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

---
TODO: Builds need to handle name and file duplicates
"""


# This function used to be async, but it was pointless since `get_files` and `get_folders` are blocking. Even the module extractors are all synchronous. Will probably just wrap in a thread and call it a day.
def build_default(course: canvas.Course, inode_mapper) -> ({int: canvas.File}, {int: canvas.Folder}, Callable[[canvas.File], int]):
    root_inode = inode_mapper(0)
    root_folder = LocalFolder(root_inode, "PLACEHOLDER")
    root_folder.attributes.st_ctime_ns = datetime_to_fstime(course.created_at_date)

    files = {inode_mapper(file.id): file for file in course.get_files()}
    folders = {inode_mapper(folder.id): folder for folder in course.get_folders()}
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
            raise ValueError("Lookups on SubFS root inode must be special-cased")

        parent_folder_id = fs_parent(child)
        # If we could not find `canvas_root_id`, it's possible nothing gets attached to our root folder, which is not good...
        # Cannot give None to `inode_mapper`: (crid is not None) => (pfid is not None)
        assert (canvas_root_id is None) or (parent_folder_id is not None)
        if parent_folder_id == canvas_root_id:
            parent_folder_id = 0
        return inode_mapper(parent_folder_id)

    return files, folders, dirfunc


def build_flat(course: canvas.Course, inode_mapper) -> ({int: canvas.File}, {int: canvas.Folder}, Callable[[canvas.File], int]):
    root_inode = inode_mapper(0)
    files = {inode_mapper(file.id): file for file in course.get_files()}
    folders = {root_inode: LocalFolder(root_inode, "PLACEHOLDER")}  # lookups and readdirs of the root are handled by the FS instance so the name is irrelevant. This allows us to avoid special-casing operations on the root.

    dirfunc = lambda _: root_inode

    return files, folders, dirfunc


def build_from_modules(course: canvas.Course, inode_mapper) -> ({int: canvas.File}, {int: canvas.Folder}, Callable[[canvas.File], int]):
    root_inode = inode_mapper(0)
    files = {inode_mapper(file.id): file for file in extract_modules(course)}
    folders = {root_inode: LocalFolder(root_inode, "PLACEHOLDER")}

    dirfunc = lambda _: root_inode

    return files, folders, dirfunc


def build_from_assignments(course: canvas.Course, inode_mapper) -> ({int: canvas.File}, {int: canvas.Folder}, Callable[[canvas.File], int]):
    root_inode = inode_mapper(0)
    files = {inode_mapper(file.id): file for file in extract_assignments(course)}
    folders = {root_inode: LocalFolder(root_inode, "PLACEHOLDER")}

    dirfunc = lambda _: root_inode

    return files, folders, dirfunc


def build_markdown(course: canvas.Course, inode_mapper) -> ({int: canvas.File}, {int: canvas.Folder}, Callable[[canvas.File], int]):
    """
    An idea I had was to convert the HTML module pages into markdown and save as .md files
    """
    raise NotImplementedError()
