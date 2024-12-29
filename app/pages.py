from __future__ import annotations
from enum import Enum

from app.consts import (
    INTERIOR_PAGE_HEADER_SIZE,
    LEAF_PAGE_HEADER_SIZE,
    DB_FILE_HEADER_SIZE,
    SQLITE_SEQUENCE_TABLE_NAME,
)
from app.reading import read_varint, read_record
from app.rows import Schema

from typing import List, BinaryIO


class PageType(Enum):
    INTERIOR_INDEX = 0x02
    INTERIOR_TABLE = 0x05
    LEAF_INDEX = 0x0A
    LEAF_TABLE = 0x0D


# representation of the page (header + cells) declared in
# https://www.sqlite.org/fileformat.html#b_tree_pages
class Page:
    bytes_content: bytearray
    page_type: PageType
    cell_count: int
    cell_area_start: int
    cell_pointer_array: List[int]

    #  The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    @staticmethod
    def __read_cell_pointers(bytes: bytearray, cell_count: int) -> List[int]:
        return [
            int.from_bytes(bytes[i * 2 : i * 2 + 2], "big") for i in range(cell_count)
        ]

    @staticmethod
    def from_bytes(bytes_content: bytearray, is_first_page: bool = False) -> Page:
        instance = Page()
        instance.bytes_content = bytes_content

        if is_first_page:
            bytes_content = bytes_content[DB_FILE_HEADER_SIZE:]

        page_type_byte = bytes_content[0]
        try:
            instance.page_type = PageType(page_type_byte)
        except ValueError:
            raise ValueError(f"Invalid page type: {page_type_byte:#02x}")

        instance.cell_count = int.from_bytes(bytes_content[3:5], "big")

        post_header_bytes = []
        if (
            instance.page_type == PageType.LEAF_INDEX
            or instance.page_type == PageType.LEAF_TABLE
        ):
            post_header_bytes = bytes_content[LEAF_PAGE_HEADER_SIZE:]
        else:
            post_header_bytes = bytes_content[INTERIOR_PAGE_HEADER_SIZE:]

        instance.cell_pointer_array = Page.__read_cell_pointers(
            post_header_bytes, instance.cell_count
        )

        return instance

    def read_sqlite_schema(self, database_file: BinaryIO) -> List[Schema]:
        if self.page_type != PageType.LEAF_TABLE:
            raise TypeError("Cannot read sqlite schema if page isn't the first")

        schema_records = []
        for cell_pointer in self.cell_pointer_array:
            # See https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/ for why the reads are done
            database_file.seek(cell_pointer)

            _header_size = read_varint(database_file)
            _rowid = read_varint(database_file)
            record = read_record(database_file, 5)  # the number of columns is known
            schema = Schema(
                table_type=record[0].decode("utf-8"),
                table_name=record[1].decode("utf-8"),
                name=record[2].decode("utf-8"),
                rootpage=record[3],
                sql=record[4],
            )

            if schema.name == SQLITE_SEQUENCE_TABLE_NAME:
                continue

            schema_records.append(schema)

        return schema_records
