from __future__ import annotations
from enum import Enum
from dataclasses import dataclass

from app.consts import (
    INTERIOR_PAGE_HEADER_SIZE,
    LEAF_PAGE_HEADER_SIZE,
    DB_FILE_HEADER_SIZE,
    SQLITE_SEQUENCE_TABLE_NAME,
)
from app.reading import read_varint, read_record, page_start
from app.rows import Schema

from typing import List, BinaryIO, Dict, Optional


@dataclass
class InteriorPointer:
    """
    Cell type used by interior pages to represent all the leaf pages containing
    a specific table the interior page is responsible for.

    An interior page will have at least 2 interior pointers.
    """

    page_index: int  # index of the pointed leaf page
    smallest_row_id: int  # varint representing the smallest row IF of the page


class PageType(Enum):
    INTERIOR_INDEX = 0x02
    """
    Page type used to point to the multipe pages that span a specific table.
    Its cell pointer array and right most pointer cna be used to find a specific key range

    Interior Page cell array Example:

|   Ptr1 | Key1 | Ptr2 | Key2 | Ptr3 | Key3 | Right-Most Ptr |
    Ptr1 → Points to keys < Key1
    Ptr2 → Points to keys Key1 ≤ x < Key2
    Ptr3 → Points to keys Key2 ≤ x < Key3
    Right-Most Ptr → Points to keys ≥ Key3
    """
    INTERIOR_TABLE = 0x05
    LEAF_INDEX = 0x0A
    LEAF_TABLE = 0x0D


# representation of the page (header + cells) declared in
# https://www.sqlite.org/fileformat.html#b_tree_pages
class Page:
    start: int
    page_type: PageType
    cell_count: int
    cell_area_start: int
    cell_pointer_array: List[int]
    right_most_pointer: Optional[int]  # Only present in inner page headers

    @staticmethod
    def from_bytes(
        bytes_content: bytearray, start: int, is_first_page: bool = False
    ) -> Page:
        instance = Page()
        instance.start = start

        if is_first_page:
            bytes_content = bytes_content[DB_FILE_HEADER_SIZE:]

        page_type_byte = bytes_content[0]
        try:
            instance.page_type = PageType(page_type_byte)
        except ValueError:
            raise ValueError(f"Invalid page type: {page_type_byte:#02x}")

        instance.cell_count = int.from_bytes(bytes_content[3:5], "big")

        if (
            instance.page_type == PageType.INTERIOR_INDEX
            or instance.page_type == PageType.INTERIOR_TABLE
        ):
            instance.right_most_pointer = int.from_bytes(bytes_content[8:12], "big")
        else:
            instance.right_most_pointer = None

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
            row_id, _ = read_varint(database_file)
            record = read_record(database_file)  # the number of columns is known
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

    # TODO: this can be improved by just wrapping the reaD_records method
    # to handle both leaf and inner nodes
    def load_pointed_table_leaf_pages(
        self, database_file: BinaryIO, page_size: int
    ) -> List[Page]:
        if self.page_type != PageType.INTERIOR_TABLE:
            raise TypeError(
                f"Can only load table leaf pages if current page is an interior page, but it is {self.page_type}"
            )
        leaf_page_pointer = self.__read_interior_page_pointers(database_file)

        pages = []
        for pointer in leaf_page_pointer:
            desired_page_start = page_start(pointer.page_index - 1, page_size)
            database_file.seek(desired_page_start)
            page_bytes = bytearray(database_file.read(page_size))
            pages.append(Page.from_bytes(page_bytes, desired_page_start))

        return pages

    # given a list of column names, reads the rows and returns the values as dicts
    def read_records_with_schema(
        self, database_file: BinaryIO, schema: List[str]
    ) -> List[Dict[str, any]]:
        records = self.__read_records(database_file)

        res = []
        for record in records:
            if len(record) != len(schema):
                raise TypeError(
                    "Len of record does not match len of provided schema",
                    len(record),
                    len(schema),
                )
            res.append({key: value for (key, value) in zip(schema, record)})

        return res

    #  The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    @staticmethod
    def __read_cell_pointers(bytes: bytearray, cell_count: int) -> List[int]:
        return [
            int.from_bytes(bytes[i * 2 : i * 2 + 2], "big") for i in range(cell_count)
        ]

    def __read_records(self, database_file: BinaryIO) -> List[List[any]]:
        records = []
        for cell_pointer in self.cell_pointer_array:
            database_file.seek(self.start + cell_pointer)

            # See https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/ for why the reads are done
            _header_size, _ = read_varint(database_file)
            row_id, count = read_varint(database_file)
            record = read_record(database_file, row_id)

            records.append(record)

        return records

    def __read_interior_page_pointers(
        self, database_file: BinaryIO
    ) -> List[InteriorPointer]:
        pointers = []
        for cell_pointer in self.cell_pointer_array:
            database_file.seek(self.start + cell_pointer)

            # See https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/ for why the reads are done
            page_index = int.from_bytes(database_file.read(4), byteorder="big")
            row_id = read_varint(database_file)[0]
            pointers.append(InteriorPointer(page_index, row_id))
        # return sorted(pointers, key=lambda cell: cell.smallest_row_id)
        return pointers
