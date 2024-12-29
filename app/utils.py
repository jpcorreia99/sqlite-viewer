from app.consts import DB_FILE_HEADER_SIZE


def page_start(page_index, page_size):
    return DB_FILE_HEADER_SIZE + page_index * page_size
