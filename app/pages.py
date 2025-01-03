from __future__ import annotations
from enum import Enum
from dataclasses import dataclass
from collections import defaultdict

from app.consts import (
    INTERIOR_PAGE_HEADER_SIZE,
    LEAF_PAGE_HEADER_SIZE,
    DB_FILE_HEADER_SIZE,
    SQLITE_SEQUENCE_TABLE_NAME,
)
from app.reading import read_varint, read_table_record, page_start
from app.rows import Schema
from app.filtering import ValueFilter

from typing import List, BinaryIO, Dict, Optional


@dataclass
class InteriorPointer:
    """
    Cell type used by interior table pages to represent all the leaf pages containing
    a specific table the interior page is responsible for.

    An interior page will have at least 2 interior pointers.
    """

    page_index: int  # index of the pointed leaf page
    smallest_row_id: int  # varint representing the smallest row IF of the page


@dataclass
class IndexRecord:
    """
    Cell type used by interior index pages to represent all the rows containing
    a specific key the index is responsible for.
    """

    value: str
    row_id: int  # id of a row containing this value
    left_pointer: Optional[
        int
    ]  # if it's an inner node, pointer to an indes leaf page contianing more about this row


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


class Page:
    start: int
    page_type: PageType
    cell_count: int
    cell_area_start: int
    cell_pointer_array: List[int]
    right_most_pointer: Optional[int]  # Only present in inner page headers

    @staticmethod
    def from_file(
        database_file: BinaryIO, start: int, is_first_page: bool = False
    ) -> Page:
        """
        Loads the database page from located at the "start" offset.
        Parses the page header as described in https://www.sqlite.org/fileformat2.html#b_tree_pages
        and, based on that, loads the cell pointer array
        """
        instance = Page()
        instance.start = start

        database_file.seek(start)
        real_start = start

        # For the first page, we must skip the 100 byte database header
        if is_first_page:
            database_file.seek(DB_FILE_HEADER_SIZE)
            real_start = DB_FILE_HEADER_SIZE

        page_type_int = int.from_bytes(database_file.read(1), "big")
        try:
            instance.page_type = PageType(page_type_int)
        except ValueError:
            raise ValueError(f"Invalid page type: {page_type_int}")

        database_file.seek(real_start + 3)
        instance.cell_count = int.from_bytes(database_file.read(2), "big")

        database_file.seek(real_start + 8)
        if (
            instance.page_type == PageType.INTERIOR_INDEX
            or instance.page_type == PageType.INTERIOR_TABLE
        ):
            instance.right_most_pointer = int.from_bytes(database_file.read(4), "big")
        else:
            instance.right_most_pointer = None

        instance.cell_pointer_array = Page.__read_cell_pointers_from_file(
            database_file, instance.cell_count
        )

        return instance

    def read_sqlite_schema(self, database_file: BinaryIO) -> List[Schema]:
        if self.page_type != PageType.LEAF_TABLE:
            raise TypeError("Cannot read sqlite schema if page isn't the first")

        schema_records = []
        for cell_pointer in self.cell_pointer_array:
            # See https://saveriomiroddidatabase_file.github.io/SQLIte-database-file-format-diagrams/ for why the reads are done
            database_file.seek(cell_pointer)

            _header_size = read_varint(database_file)
            row_id, _ = read_varint(database_file)
            record = read_table_record(database_file)  # the number of columns is known
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

    def load_table_leaf_pages(
        self, database_file: BinaryIO, page_size: int, row_ids: List[int] = None
    ) -> List[Page]:
        """
        Returns itself if it's a table leaf node already
        or traverses the inner nodes to collect all leaf nodes.

        Args:
            row_ids (list(str)): list of row ids to filter by and only load those pages.
                will load all pages in case nothing is provided
        """
        if self.page_type == PageType.LEAF_TABLE:
            return [self]

        if self.page_type != PageType.INTERIOR_TABLE:
            raise TypeError(
                f"Can only load table leaf pages if current page is an interior or leaf table page, but it is {self.page_type}"
            )

        leaf_page_pointers = self.__read_interior_page_pointers(database_file)
        pages = []
        if not row_ids:
            # Besides traversing all the nodes pointeb by this page, we must also traverse to itx
            # right side neighbour, which points row IDs > than any in this page
            leaf_page_pointers.append(InteriorPointer(self.right_most_pointer, -1))

            # no row_ids means we didn't use an index, therefore we load all the data
            for pointer in leaf_page_pointers:
                # We must make it recursive to handle tables which require multiple interior pages
                pointed_page = load_page_at_location(
                    database_file, pointer.page_index - 1, page_size
                )
                pages += pointed_page.load_table_leaf_pages(database_file, page_size)
        else:
            # we used an index that already pointed us the row ids that fullfill this condition
            # Therefore, we only need to load pages that contain any of those row_ids
            # The strategy here is to check for each row_id if it falls inside the provided interval or not
            pages_idx_to_row_id = defaultdict(list)
            i = 0
            for pointer in leaf_page_pointers:
                while i < len(row_ids):
                    if row_ids[i] < pointer.smallest_row_id:
                        pages_idx_to_row_id[pointer.page_index].append(row_ids[i])
                        i += 1
                    else:
                        break
            if i < len(row_ids):  # if there are row ids that do not fit inside
                pages_idx_to_row_id[self.right_most_pointer] = row_ids[i:]

            for page_idx, row_ids in pages_idx_to_row_id.items():
                pointed_page = pointed_page = load_page_at_location(
                    database_file, page_idx - 1, page_size
                )
                pages += pointed_page.load_table_leaf_pages(
                    database_file, page_size, row_ids
                )

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
                    record,
                    schema,
                )
            res.append({key: value for (key, value) in zip(schema, record)})

        return res

    def load_filter_compliant_row_ids(
        self, database_file: BinaryIO, value_filter: ValueFilter, page_size: int
    ) -> List[int]:
        if (
            self.page_type != PageType.INTERIOR_INDEX
            and self.page_type != PageType.LEAF_INDEX
        ):
            raise TypeError(
                "Cannot apply filtered loading to a non index page. Type was",
                self.page_type,
            )

        row_ids = []
        index_records = self.__read_index_records(database_file)
        if self.page_type == PageType.LEAF_INDEX:
            row_ids = [
                index_record.row_id
                for index_record in index_records
                if index_record.value == value_filter.value
            ]
        else:
            # binary search now
            # if first value in node is > than our value, fallback to first left pointer
            # else, search for any match and follow those nodes
            #   NOTE: a match can be an equality or our value falling between the values of two records
            # else, traverse to the right_most pointer
            page_indices_to_query = []
            for i, record in enumerate(index_records):
                if record.value == value_filter.value:
                    row_ids.append(record.row_id)
                    page_indices_to_query.append(record.left_pointer)
                elif i >= 1:
                    if (
                        index_records[i - 1].value  # to handle NULLS
                        and index_records[i - 1].value < value_filter.value
                        and index_records[i].value
                        and index_records[i].value > value_filter.value
                    ):
                        page_indices_to_query.append(index_records[i].left_pointer)

            if not page_indices_to_query:
                if index_records[0].value > value_filter.value:
                    page_indices_to_query = [index_records[0].left_pointer]
                else:
                    page_indices_to_query = [self.right_most_pointer]

            for page_idx in page_indices_to_query:
                pointed_page = load_page_at_location(
                    database_file, page_idx - 1, page_size
                )
                row_ids += pointed_page.load_filter_compliant_row_ids(
                    database_file, value_filter, page_size
                )

        row_ids = sorted(row_ids)
        return list(dict.fromkeys(row_ids))

    #  The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    @staticmethod
    def __read_cell_pointers_from_file(
        database_file: BinaryIO, cell_count: int
    ) -> List[int]:
        return [int.from_bytes(database_file.read(2), "big") for _ in range(cell_count)]

    def __read_records(self, database_file: BinaryIO) -> List[List[any]]:
        records = []
        for cell_pointer in self.cell_pointer_array:
            database_file.seek(self.start + cell_pointer)

            # See https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/ for why the reads are done
            _header_size, _ = read_varint(database_file)
            row_id, count = read_varint(database_file)
            record = read_table_record(database_file, row_id)

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

    def __read_index_records(self, database_file: BinaryIO) -> List[IndexRecord]:
        records = []
        for i, cell_pointer in enumerate(self.cell_pointer_array):
            database_file.seek(self.start + cell_pointer)
            # See https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/ for why the reads are done
            left_child_pointer = None
            if self.page_type == PageType.INTERIOR_INDEX:
                left_child_pointer = int.from_bytes(database_file.read(4), "big")

            _payload_bytes_size = read_varint(database_file)
            record = read_table_record(database_file)
            # print(cell_pointer, self.page_type, record)
            value = record[0]
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            #  print("!", record, self.page_type)
            #     else:

            records.append(IndexRecord(value, record[1], left_child_pointer))

        return records


def load_page_at_location(database_file, page_idx: int, page_size: int) -> [Page]:
    page_location = page_start(page_idx, page_size)

    database_file.seek(page_location)
    # page_bytes = bytearray(database_file.read(page_size))

    return Page.from_file(database_file, page_location)
