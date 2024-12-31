import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Statement, Parenthesis
from sqlparse.tokens import Keyword, Wildcard
from app.pages import Page
from typing import List


def handle_sql_query(query, database_file, first_page, page_size):
    query_args = query.split(" ")
    table_name = query_args[-1]
    sqlite_schema = first_page.read_sqlite_schema(database_file)

    if query_args[0].lower() != "select":
        raise TypeError("Only SELECT queries are supported")

    if query_args[1].lower() == "count(*)":
        print_table_count(database_file, sqlite_schema, table_name, page_size)
    else:
        column_names = extract_columns_names_from_query(query)
        print_column_contents(
            database_file, sqlite_schema, table_name, column_names, page_size
        )


def print_table_count(database_file, sqlite_schema, table_name, page_size):
    table_page = get_table_page(database_file, sqlite_schema, table_name, page_size)
    print(table_page.cell_count)


def print_column_contents(
    database_file, sqlite_schema, table_name, requested_column_names, page_size
):
    desired_table_schema = next(
        (schema for schema in sqlite_schema if schema.name == table_name), None
    )

    creation_query = desired_table_schema.sql.split(b"\r")[0]
    parsed_creation_query = sqlparse.parse(creation_query.decode("utf-8"))[0]

    schema = get_column_names_from_creation_query(parsed_creation_query)
    table_page = get_table_page(database_file, sqlite_schema, table_name, page_size)
    rows = table_page.read_records_with_schema(database_file, schema)

    column_values_per_row = [
        [row[column_name].decode("utf8") for column_name in requested_column_names]
        for row in rows
    ]

    print(
        "\n".join(["|".join([entry for entry in row]) for row in column_values_per_row])
    )


def get_table_page(database_file, sqlite_schema, table_name, page_size) -> Page:
    desired_table_schema = next(
        (schema for schema in sqlite_schema if schema.name == table_name), None
    )

    desired_table_rootpage = desired_table_schema.rootpage - 1
    desired_table_location = page_start(desired_table_rootpage, page_size)

    desired_table_location = page_start(desired_table_rootpage, page_size)
    database_file.seek(desired_table_location)
    page_bytes = bytearray(database_file.read(page_size))

    return Page.from_bytes(page_bytes, desired_table_location)


def get_column_names_from_creation_query(parsed_creation_query: Statement) -> List[str]:
    """
    Creation query will look like

    b'''CREATE TABLE apples
    (
        id integer primary key autoincrement,
        name text,
        color text
    )'''

    We must extract the inner tokens form the last token to access the names
    """
    column_names = []
    for token in parsed_creation_query.tokens:
        if isinstance(token, Parenthesis):  #
            for subtoken in token.tokens:
                if isinstance(subtoken, Identifier):
                    column_names.append(subtoken.get_name())
                if isinstance(subtoken, IdentifierList):
                    for identifier in subtoken.get_identifiers():
                        # bug: SQLParse seems to always think autoincrement keyword is an identifier :(
                        if (
                            identifier.value.lower() != "autoincrement"
                            and identifier.value.lower() != "primary key"
                        ):
                            try:
                                column_names.append(identifier.get_name())
                            except AttributeError:
                                raise TypeError("The token that failed is", identifier)

            break  # no need to iterate more

    return column_names


def extract_columns_names_from_query(query):
    statement = sqlparse.parse(query)[0]

    column_names = []

    for token in statement.tokens:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                if identifier.ttype is not Wildcard:
                    column_names.append(identifier.get_real_name())
        elif isinstance(token, Identifier):
            column_names.append(token.get_real_name())
        elif token.ttype is Keyword and token.value.upper() == "FROM":
            break

    return column_names


def page_start(page_index, page_size):
    return page_index * page_size
