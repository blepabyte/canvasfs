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
    PAT1 = r"data-api-endpoint=\"https://canvas.auckland.ac.nz/api/v1/courses/\d+/files/(\d+)\""
    PAT2 = r"canvas\.auckland\.ac\.nz/courses/\d+/files/(\d+)[^\d]"

    return list({*map(int, re.findall(PAT1, raw_html)), *map(int, re.findall(PAT2, raw_html))})


def extract_modules(c: canvasapi.canvas.Course) -> [canvasapi.canvas.File]:
    for mod in c.get_modules():
        for mod_item in mod.get_module_items():
            if getattr(mod_item, "type", None) == 'File':
                yield c.get_file(mod_item.content_id)

            elif getattr(mod_item, "type", None) == 'Page':
                page_html = c.get_page(mod_item.page_url).body
                yield from map(c.get_file, extract_file_ids(page_html))

            elif getattr(mod_item, "type", None) in ['ExternalUrl', 'ExternalTool']:
                # Not sure what to do with these. Ignoring for now.
                pass

            elif getattr(mod_item, "type", None) in ['Assignment']:
                # These can be picked up by the AssignmentBuilder, so ignoring here
                pass

            elif getattr(mod_item, "type", None) in ['SubHeader']:
                # Useless.
                pass

            else:
                logger.error(f"Unknown module type")
                logger.error(mod_item)


def extract_assignments(c: canvasapi.canvas.Course):
    for ass in c.get_assignments():
        yield from map(c.get_file, extract_file_ids(ass.description))


if __name__ == '__main__':
    from canvasfs import canvas

    m326 = canvas().get_course(63078)
    m326_extracted = list(extract_modules(m326))

    breakpoint()

    a200g = canvas().get_course(48353)
    a200g_extracted = list(extract_modules(a200g))

    breakpoint()
