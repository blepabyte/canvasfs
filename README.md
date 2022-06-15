# canvasfs / a FUSE filesystem for Canvas courses

`canvasfs` allows you to mount a Canvas course as if it were a local folder. Files are fetched from the network lazily when you open them and are locally cached.

## Why?

- You no longer have to click through module pages to download and save individual files 
- This is more convenient than simply exporting the course contents, as files will update automatically when their content is changed 
- It's easy to tell when new files have been added to a course by looking at the 'Date Modified' field in the file explorer (suggestion: sort folders by most recently modified first)
- You can use tools like `ripgrep` and `ripgrep-all` on course content

## Features

- Will find all files in the 'Files', 'Modules' and 'Assignment' tabs of each Canvas course
- Currently only supports read access. Write access is possible (for instructors) but I don't have the permissions to test it
- Works on Linux, or more generally wherever [pyfuse3](https://github.com/libfuse/pyfuse3) is supported. On Windows you can try using WSL2.

Ideas, PRs and issues are welcome.

## Setup

Make sure that the FUSE libraries are installed via your distro package manager. On Arch and Manjaro these are provided by the `fuse3` package.

```sh
git clone https://github.com/blepabyte/canvasfs.git
cd canvasfs
pip install -r requirements.txt
```

You will need to [obtain an API key for your Canvas account](https://canvas.instructure.com/courses/785215/pages/getting-started-with-the-api). To do this, go to the 'Settings' page of your Canvas profile, open the browser devtools, and add the following global CSS rule to the page:
```css
.add_access_token_link {
    display: block !important;
}
```
You can then click on the "New Access Token" button that appears under "Approved integrations" to get your token. Now either see **Configuration** below, or just run the main program and follow the prompts to automatically generate the `config.json` file.

```
python canvasfs.py
```

## Configuration

By default config parameters are stored in a `config.json` file in the root `canvasfs` folder. The first four parameters below are required.

```json5
{
    // API key
    "token": "XXXXXXXXXXXXXXXXXXXXXXX", 
    
    // Replace with your institution's domain
    "domain": "https://canvas.auckland.ac.nz",
    
    // An empty folder where files will be shown locally
    "mount_dir": "./remote",
    
    // Cache location on disk
    "cache_dir": "/mnt/storage/.canvasfs",
    
    // IDs of courses to mount
    "courses": [
        {
            "id": 11111,
            "name": "COMPSCI 000"
        },
        {
            "id": 22222
        }
    ]
}
```


#### Optional parameters

- `refresh_interval`: how often (**in hours**) it should check for new files on Canvas. The default is 4 hours. Set to `-1` to disable refreshing completely.
- `debug`: sets logging level to `DEBUG` rather than `INFO`


#### Per-course configuration

You can pass a list of courses you want to mount by specifying their ID (this can be found in the URL of the course home page: `https://canvas.auckland.ac.nz/courses/<ID>`). By default this will be all currently "active" courses - though some courses do not set their end date properly, so you will likely end up needing to manually configure this.

- `name`: sets the display name of the local folder. Obviously this should be unique between courses and a valid directory name


## Usage

```
python canvasfs.py [path-to-config-file=./config.json]
```

If you want, you can manually trigger a file list refresh by sending a `SIGUSR1` signal to the running Python process, e.g. using `btop` or `htop`. 

<!-- **Alternatively, you can use the provided `Dockerfile`**: The additional flags are needed for FUSE to work. 
```bsh
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN canvasfs:latest
```

```json5
{
    // ...
    
}
```

![SCREENSHOT]()

## Feature ideas

- Optional notifications when files are added/removed
- Windows and Mac support using FTP instead of FUSE
-->

