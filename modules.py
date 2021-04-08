"""
Extract links to course files in module pages
"""
import canvasapi
import re
from loguru import logger
import config


def extract_file_ids(raw_html: str) -> [int]:
    """
    Parses input HTML to get IDs of any linked Canvas files
    """
    # who says you can't parse HTML with Regex?
    PAT1 = r"data-api-endpoint=\"[^\"]+?/api/v1/courses/\d+/files/(\d+)\""

    relevant_domain = re.search(
        r"^(?:https?://(?:www\.)?)?(.*?)/?$",
        config.config()["domain"]
    ).group(1).replace(".", "\\.")
    PAT2 = relevant_domain + r"/courses/\d+/files/(\d+)[^\d]"

    return list({*map(int, re.findall(PAT1, raw_html)), *map(int, re.findall(PAT2, raw_html))})


def file_extractor(course, id_iterable: [int]):
    for i in id_iterable:
        try:
            yield course.get_file(i)
        except canvasapi.exceptions.ResourceDoesNotExist:
            logger.warning(f"Broken link: Failed to get file with id {i} in course: {course}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error: Getting file with id {i} in course: {course}")
            logger.exception(e)
            continue


def extract_modules(c: canvasapi.canvas.Course) -> [canvasapi.canvas.File]:
    for mod in c.get_modules():
        for mod_item in mod.get_module_items():
            if getattr(mod_item, "type", None) == 'File':
                yield c.get_file(mod_item.content_id)

            elif getattr(mod_item, "type", None) == 'Page':
                page_html = c.get_page(mod_item.page_url).body
                yield from file_extractor(c, extract_file_ids(page_html))

            elif getattr(mod_item, "type", None) in ['ExternalUrl', 'ExternalTool']:
                # Not sure what to do with these. Ignoring for now
                pass

            elif getattr(mod_item, "type", None) in ['Assignment']:
                # These can be picked up by the AssignmentBuilder, so ignoring here
                pass

            elif getattr(mod_item, "type", None) in ['SubHeader']:
                # Useless
                pass

            else:
                logger.error(f"Unknown module type")
                logger.error(mod_item)


def extract_assignments(c: canvasapi.canvas.Course):
    for ass in c.get_assignments():
        html_to_parse = ass.description
        if html_to_parse is None:
            continue
        # Sometimes file links might be broken, sometimes the Canvas API just returns nonsense
        # yield from map(c.get_file, extract_file_ids(html_to_parse))
        yield from file_extractor(c, extract_file_ids(html_to_parse))


if __name__ == '__main__':
    from config import canvas

    m326 = canvas().get_course(63078)
    m326_extracted = list(extract_modules(m326))

    breakpoint()

    a200g = canvas().get_course(48353)
    a200g_extracted = list(extract_modules(a200g))

    breakpoint()
