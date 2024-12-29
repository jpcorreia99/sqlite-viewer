from app.consts import DB_FILE_HEADER_SIZE, IS_FIRST_BIT_ZERO_MASK, LAST_SEVEN_BITS_MASK
from typing import BinaryIO


def page_start(page_index, page_size):
    return DB_FILE_HEADER_SIZE + page_index * page_size


def read_varint(stream: BinaryIO) -> int:
    # https://www.sqlite.org/fileformat.html#varint

    byte = stream.read(1)[0]
    # Extract the 7 least s ignificant bits
    value = byte & LAST_SEVEN_BITS_MASK
    shift = 7

    # Continue extracting the 7 least significant bits until the most significant bit is 0
    while byte & IS_FIRST_BIT_ZERO_MASK:  #  0x80 is 10000000 in binary, checks the MSB
        byte = stream.read(1)[0]
        value |= (byte & 0x7F) << shift
        shift += 7

    return value


def read_record(stream: BinaryIO, column_count: int):
    _number_of_bytes_in_header = read_varint(stream)

    serial_types = [read_varint(stream) for i in range(column_count)]
    return [read_column_value(stream, serial_type) for serial_type in serial_types]


def read_column_value(stream, serial_type):
    if (serial_type >= 13) and (serial_type % 2 == 1):
        n_bytes = (serial_type - 13) // 2
        return stream.read(n_bytes)
    elif serial_type == 1:
        return int.from_bytes(stream.read(1), "big")
    else:
        raise Exception(f"Unknown serial_type {serial_type}")
