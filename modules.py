"""
Extract links to course files in module pages
"""
import canvasapi
import re
from loguru import logger


def extract_file_ids(raw_html: str) -> [int]:
    """
    Parses input HTML to get IDs of any linked Canvas files
    """
    # who says you can't parse HTML with Regex?
    PATTERNS = [
        r"data-api-endpoint=\"[^\"]+?/api/v1/courses/\d+/files/(\d+)\"",
        r"/courses/\d+/files/(\d+)[^\d]"
    ]
    return set.union(
        *(set(map(int, re.findall(P, raw_html))) for P in PATTERNS)
    )


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
    # Some courses put files on the home page, but the Course.show_front_page endpoint seems to be broken
    yield from file_extractor(c, extract_file_ids(getattr(c, 'syllabus_body', "")))

    for mod in c.get_modules():
        for mod_item in mod.get_module_items():
            mod_type = getattr(mod_item, "type", None)
            if mod_type == 'File':
                yield c.get_file(mod_item.content_id)

            elif mod_type == 'Page':
                page_html = c.get_page(mod_item.page_url).body
                yield from file_extractor(c, extract_file_ids(page_html))

            elif mod_type in ['ExternalUrl', 'ExternalTool']:
                # Not sure what to do with these. Ignoring for now
                pass

            elif mod_type in ['Assignment']:
                # These can be picked up by the AssignmentBuilder, so ignoring here
                pass

            elif mod_type in ['SubHeader']:
                # Useless
                pass

            else:
                logger.warning(f"Unknown module type: {mod_type}")
                logger.warning(mod_item)


def extract_assignments(c: canvasapi.canvas.Course):
    for ass in c.get_assignments():
        # `ass.description` might be outdated so we have to make a separate request
        html_to_parse = c.get_assignment(ass.id).description
        if html_to_parse is None:
            continue
        # Sometimes file links might be broken, sometimes the Canvas API just returns nonsense
        # yield from map(c.get_file, extract_file_ids(html_to_parse))
        yield from file_extractor(c, extract_file_ids(html_to_parse))


if __name__ == '__main__':
    from config import canvas

    m326 = canvas().get_course(63078, include='syllabus_body')
    m326_extracted = list(extract_modules(m326))
    # FP = m326.show_front_page()
    breakpoint()

    a200g = canvas().get_course(48353)
    a200g_extracted = list(extract_modules(a200g))

    breakpoint()
