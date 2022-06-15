import json, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from loguru import logger
import canvasapi
import requests

import builders


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

    try:
        with open(config_location) as f:
            config.CONFIG = json.load(f)
    except FileNotFoundError:
        print("No config.json file found in current directory. Creating one now...")
        setup_config()

    return config()


config.CONFIG = None


def canvas() -> canvasapi.canvas.Canvas:
    if canvas.CANVAS is not None:
        return canvas.CANVAS

    token = config()["token"]
    domain = config()["domain"]
    canvas.CANVAS = canvasapi.canvas.Canvas(domain, token)
    return canvas()


canvas.CANVAS = None


def cache_root() -> Path:
    return Path(config()["cache_dir"])


def cache_size() -> int:
    # Total size of cache in bytes
    # https://stackoverflow.com/a/1392549
    return sum(f.stat().st_size for f in cache_root().glob('**/*') if f.is_file())


def format_course(c):
    return f"{getattr(c, 'name', '<Unknown course>')} ({c.id})"


def setup_config():
    api_key = input("API key (see README): ")
    domain = input("Institution Canvas domain: ")

    try:
        print(*map(format_course, canvasapi.canvas.Canvas(domain, api_key).get_courses()), sep='\n')
    except ConnectionError:
        print("Failed to connect: The domain is probably incorrect")
        # Using recursion to do input handling, because I'm lazy. If this fails >999 times then a recursion error will be thrown; but if you're that bad at entering inputs then you deserve it.
        setup_config()
        return
    except canvasapi.exceptions.InvalidAccessToken:
        print("API key was rejected. Try again.")
        setup_config()
        return
    except requests.exceptions.MissingSchema:
        print("The domain must begin with 'https://'")
        setup_config()
        return
    except Exception as e:
        logger.error("Unknown error initialising canvasapi object")
        logger.exception(e)
        raise

    mount_dir = input("Mount directory: ")
    while not Path(mount_dir).exists():
        mount_dir = input(f"{mount_dir} does not exist. Try again: ")

    cache_dir = input("Cache directory: ")
    while not Path(cache_dir).exists():
        cache_dir = f"{cache_dir} does not exist. Try again: "

    with open("config.json", "w") as f:
        json.dump({
            "token": api_key,
            "domain": domain,
            "mount_dir": mount_dir,
            "cache_dir": cache_dir
        }, f, indent=4)


def stream_course_configs(configs):
    for conf in configs:
        try:
            # Necessary to be able to parse `syllabus_body` field in `Course` object
            course = canvas().get_course(conf['id'], include='syllabus_body')
            conf["course"] = course
        except canvasapi.exceptions.Unauthorized:
            logger.warning(f"The course with id: {conf['id']} is not accessible (possible reason: access is restricted by date)")
            continue

        logger.info(f"Found course '{course.name}' (id: {course.id})")

        # flat (default): ignore folder structure in Canvas files tab and dump everything in root course folder
        # might be bugs in how duplicates files are handled?
        chosen_builder = builders.build_combined_flat if conf.get('flat', True) else builders.build_combined

        try:
            _f = list(course.get_files())
            # `files` are accessible
            conf["builder"] = chosen_builder
        except canvasapi.exceptions.Unauthorized:
            logger.warning(f"The course '{course.name}' (id: {course.id}) does not have an accessible files tab")
            conf["builder"] = lambda *args: chosen_builder(*args, build_files=False)

        yield conf


def process_course_configs():
    """
    If no courses are provided in the config this may take a while, as each course is processed synchronously
    IDEA: Parallelise checks via async threads (then might hit rate limit)
    """
    if "courses" in config():
        courses_config = config()["courses"]
    else:
        # Ignore courses that have already ended, as a sane default. Some courses don't properly set their end date, so some old courses will still show up and the user will need to manually modify the config file
        filter_by = lambda c: getattr(c, "end_at_date", datetime.now(timezone.utc) + timedelta(7)) > datetime.now(timezone.utc)
        courses_config = [{"id": c.id} for c in canvas().get_courses() if filter_by(c)]

    """
    Each course expects a dict with fields:
    - course: canvas.Course
    - builder: BuildOutput
    and optionally:
    - name: str
    """

    return list(stream_course_configs(courses_config))
