import sqlparse
from app.pages import Page


def page_start(page_index, page_size):
    return page_index * page_size


def handle_sql_query(query, database_file, first_page, page_size):
    query_args = query.split(" ")
    table_name = query_args[-1]
    sqlite_schema = first_page.read_sqlite_schema(database_file)

    if query_args[0].lower() != "select":
        raise TypeError("Only SELECT queries are supported")

    if query_args[1].lower() == "count(*)":
        print_table_count(database_file, sqlite_schema, table_name, page_size)
    else:
        column_name = query_args[1]


def print_table_count(database_file, sqlite_schema, table_name, page_size):
    desired_table_schema = next(
        (schema for schema in sqlite_schema if schema.name == table_name), None
    )

    # IMPORTANT, page count is 1-indexed, meaning the first page is represented as rootpage "1"
    desired_table_rootpage = desired_table_schema.rootpage - 1

    desired_table_location = page_start(desired_table_rootpage, page_size)
    database_file.seek(desired_table_location)
    page_bytes = bytearray(database_file.read(page_size))
    table_page = Page.from_bytes(page_bytes)

    print(table_page.cell_count)


def print_column_contents(
    database_file, sqlite_schema, table_name, column_name, page_size
):
    pass
