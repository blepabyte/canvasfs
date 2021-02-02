# canvasfs - FUSE filesystem for Canvas courses

`canvasfs.py` is a program that allows you to mount the **Files** tab of a Canvas course as if it were a local folder. Files are fetched from the network lazily (only when you open them) and are locally cached. 

- Currently this only supports read-only access. Write access is possible (for instructors) but I don't have the permissions to be able to test it. 
- Works on Linux, or more generally wherever [pyfuse3](https://github.com/libfuse/pyfuse3) is supported. 
- WIP: Syncing, and optional notifications when files are added

Ideas, PRs and issues are welcome. 

## Setup

```sh
git clone https://github.com/blepabyte/canvasfs.git
cd canvasfs
pip install -r requirements.txt
```

[Obtain an API key for your Canvas account](https://canvas.instructure.com/courses/785215/pages/getting-started-with-the-api) and place in `./config.json` like below. All 4 fields in the example below are **required**. Absolute paths are preferred. Also, delete the comments.    
```json5
{
    "token": "XXXXXXXXXXXXXXXXXXXXXXX",
    "domain": "https://canvas.auckland.ac.nz", // Replace with your institution's domain
    "mount_dir": "./remote", // Where you want the files to be available locally
    "cache_dir": "/mnt/storage/.canvas_fs" // Somewhere to cache files to avoid excessive re-downloading
}
```

Make sure that the required fuse libraries are installed via your distro package manager. On Arch/Manjaro:
```
pacman -S fuse3
```

## Usage

```
python canvasfs.py [path-to-config-file=./config.json]
```

By default, it will try to discover all enrolled *active* courses that have an accessible "Files" tab (some courses disable it) and setup a separate directory with the name of the course under `MOUNT_DIR/` for each. Optionally, you can the `courses` key to `config.json`: you will need the course ID, which can be found in the URL of the course home page. Optional parameters are: 

- `name`: sets the display name of the local folder
- `subdirectories`: if `false` will ignore directories in Canvas and just dump all the files at one level. Might be useful for courses that have an excessively convoluted file hierarchy. (todo: handle file name duplicates)

```json
{
    "courses": [
        {"id": 11111, "name": "COMPSCI320", "subdirectories": false},
        {"id": 22222}
    ],
    "token": "XXXXXXXXXXXXXXXXXXXXXXX"
}
```

##### Example

```sh
python canvasfs.py "./config.json"
```

![SCREENSHOT]()