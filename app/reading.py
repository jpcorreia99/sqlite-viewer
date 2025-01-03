from app.consts import LAST_SEVEN_BITS_MASK
from typing import BinaryIO, Tuple, List


def page_start(page_index, page_size):
    return page_index * page_size


def read_varint(stream: BinaryIO) -> Tuple[int, int]:
    # https://www.sqlite.org/fileformat.html#varint

    value = 0
    byte_count = 0
    for c in range(9):
        byte_count += 1
        value <<= 7 if c < 8 else 8

        byte = stream.read(1)[0]
        # Continue extracting the 7 least significant bits until the most significant bit is 0
        value += byte & (LAST_SEVEN_BITS_MASK if c < 8 else 0b_1111_1111)
        if (byte & 0b_1000_0000) == 0:
            return value, byte_count

    return value, byte_count


def read_table_record(stream: BinaryIO, row_id: int = None) -> List[any]:
    # Reference record format in https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/
    header_size, num_header_bytes = read_varint(stream)

    i = num_header_bytes
    # read all the other bytes past the bytes used to declare the header size
    serial_types = []
    while i < header_size:
        serial_type, bytes_used = read_varint(stream)
        i += bytes_used
        serial_types.append(serial_type)

    record_columns = [
        read_column_value(stream, serial_type) for serial_type in serial_types
    ]

    # According to SQLite docs
    # When an SQL table includes an INTEGER PRIMARY KEY column (which aliases the rowid)
    # then that column appears in the record as a NULL value. SQLite will always use the
    # table b-tree key rather than the NULL value when referencing the INTEGER PRIMARY KEY column.
    if row_id:
        record_columns[0] = row_id

    return record_columns


def read_column_value(stream, serial_type):
    if serial_type == 0:
        return None
    elif serial_type == 1:
        return int.from_bytes(stream.read(1), "big")
    elif serial_type == 2:
        return int.from_bytes(stream.read(2), "big")
    elif serial_type == 3:
        return int.from_bytes(stream.read(3), "big")
    elif serial_type == 4:
        return int.from_bytes(stream.read(4), "big")
    elif serial_type == 5:
        return int.from_bytes(stream.read(6), "big")
    elif serial_type == 6:
        return int.from_bytes(stream.read(), "big")
    elif serial_type == 8:
        return 0
    elif serial_type == 9:
        return 1
    elif (serial_type >= 13) and (serial_type % 2 == 1):
        n_bytes = (serial_type - 13) // 2
        return stream.read(n_bytes)
    elif (serial_type >= 12) and (serial_type % 2 == 0):
        n_bytes = (serial_type - 12) // 2
        return stream.read(n_bytes)

    else:
        raise Exception(f"Unknown serial_type {serial_type}")
