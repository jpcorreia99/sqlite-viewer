from app.consts import IS_FIRST_BIT_ZERO_MASK, LAST_SEVEN_BITS_MASK
from typing import BinaryIO, Tuple, List


def page_start(page_index, page_size):
    return page_index * page_size


def read_varint(stream: BinaryIO) -> Tuple[int, int]:
    # https://www.sqlite.org/fileformat.html#varint

    byte_count = 1
    byte = stream.read(1)[0]
    # Extract the 7 least s ignificant bits
    value = byte & LAST_SEVEN_BITS_MASK
    shift = 7

    # Continue extracting the 7 least significant bits until the most significant bit is 0
    while byte & IS_FIRST_BIT_ZERO_MASK:  #  0x80 is 10000000 in binary, checks the MSB
        byte = stream.read(1)[0]
        value |= (byte & 0x7F) << shift
        shift += 7
        byte_count += 1

    return value, byte_count


def read_record(stream: BinaryIO) -> List[any]:
    # Reference record format in https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/
    header_size, num_header_bytes = read_varint(stream)

    i = num_header_bytes
    # read all the other bytes past the bytes used to declare the header size
    serial_types = []
    while i < header_size:
        serial_type, bytes_used = read_varint(stream)
        i += bytes_used
        serial_types.append(serial_type)

    return [read_column_value(stream, serial_type) for serial_type in serial_types]


def read_column_value(stream, serial_type):
    if (serial_type >= 13) and (serial_type % 2 == 1):
        n_bytes = (serial_type - 13) // 2
        return stream.read(n_bytes)
    elif serial_type == 1:
        return int.from_bytes(stream.read(1), "big")
    else:
        raise Exception(f"Unknown serial_type {serial_type}")
