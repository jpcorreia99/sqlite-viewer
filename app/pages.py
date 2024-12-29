from dataclasses import dataclass
from enum import Enum
from typing import List
from app.consts import INTERIOR_PAGE_HEADER_SIZE, LEAF_PAGE_HEADER_SIZE


class PageType(Enum):
    INTERIOR_INDEX = 0x02
    INTERIOR_TABLE = 0x05
    LEAF_INDEX = 0x0A
    LEAF_TABLE = 0x0D


# representation of the page (header + cells) declared in
# https://www.sqlite.org/fileformat.html#b_tree_pages
@dataclass(init=False)
class Page:
    page_type: PageType
    cell_count: int
    cell_area_start: int
    cell_pointer_array: List[int]

    #  The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    @staticmethod
    def __read_cell_pointers(bytes, cell_count) -> List[int]:
        return [
            int.from_bytes(bytes[i * 2 : i * 2 + 2], "big") for i in range(cell_count)
        ]

    @staticmethod
    def from_bytes(bytes):
        instance = Page()

        page_type_byte = bytes[0]
        try:
            instance.page_type = PageType(page_type_byte)
        except ValueError:
            raise ValueError(f"Invalid page type: {page_type_byte:#02x}")

        instance.cell_count = int.from_bytes(bytes[3:5], "big")

        post_header_bytes = []
        if (
            instance.page_type == PageType.LEAF_INDEX
            or instance.page_type == PageType.LEAF_TABLE
        ):
            post_header_bytes = bytes[LEAF_PAGE_HEADER_SIZE:]
        else:
            post_header_bytes = bytes[INTERIOR_PAGE_HEADER_SIZE:]

        instance.cell_pointer_array = Page.__read_cell_pointers(
            post_header_bytes, instance.cell_count
        )

        return instance
