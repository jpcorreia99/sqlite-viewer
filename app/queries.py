from __future__ import annotations
import re

import sqlparse
from sqlparse.sql import (
    IdentifierList,
    Identifier,
    Where,
    Statement,
    Parenthesis,
    Comparison,
)
from sqlparse.tokens import Keyword, Wildcard, Whitespace

from app.pages import Page, PageType
from app.filtering import ValueFilter
from app.rows import Schema
from app.reading import page_start
from app.consts import TABLE_CREATION_REGEX

from typing import List, Optional, BinaryIO


class Query:
    query_components: List[str]
    parsed_query: Statement

    table_name: str
    value_filter: Optional[ValueFilter]
    requested_column_names: List[str]

    def __init__(
        self,
        query_components: List[str],
        parsed_query: Statement,
        table_name: str,
        value_filter: Optional[ValueFilter],
        requested_column_names: List[str],
    ):
        self.query_components = query_components
        self.parsed_query = parsed_query
        self.table_name = table_name
        self.value_filter = value_filter
        self.requested_column_names = requested_column_names

    @staticmethod
    def parse_query(query_str: str) -> Query:
        query_components = query_str.split(" ")

        if query_components[0].lower() != "select":
            raise TypeError("Only SELECT queries are supported")

        statement = sqlparse.parse(query_str)[0]
        table_name = Query._extract_table_name_from_query(statement)
        value_filter = Query._extract_value_filter_from_query(statement)
        requested_column_names = Query._extract_columns_names_from_query(statement)

        return Query(
            query_components,
            statement,
            table_name,
            value_filter,
            requested_column_names,
        )

    @staticmethod
    def _extract_table_name_from_query(statement: Statement) -> str:
        found_from = False
        for token in statement.tokens:
            if token.ttype == Keyword and token.value.upper() == "FROM":
                found_from = True
            elif found_from and isinstance(token, Identifier):
                return token.get_real_name()

        raise RuntimeError(f"Failed to extract table name from query {statement}")

    @staticmethod
    def _extract_value_filter_from_query(statement: Statement) -> Optional[ValueFilter]:
        where_clause = None
        for token in statement.tokens:
            if isinstance(token, Where):
                where_clause = token
                break

        if not where_clause:
            return None

        for token in where_clause.tokens:
            if isinstance(token, Comparison):
                comparison_parts = [
                    t.value.strip() for t in token.tokens if t.ttype != Whitespace
                ]
                column = comparison_parts[0]
                operator = comparison_parts[1]  # Operator (=, >, <, etc.)
                value = comparison_parts[2].strip("'")

                return ValueFilter(column, operator, value)

    @staticmethod
    def _extract_columns_names_from_query(statement: Statement) -> List[str]:
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

    def execute(self, database_file: BinaryIO, first_page: Page, page_size: int):
        sqlite_schema = first_page.read_sqlite_schema(database_file)

        if self.query_components[1].lower() == "count(*)":
            table_page = get_table_leaf_pages(
                database_file, sqlite_schema, self.table_name, page_size
            )[0]  # TODO: this is wrong
            print(table_page.cell_count)
        else:
            self._execute_query(database_file, sqlite_schema, page_size)

    def _execute_query(
        self, database_file: BinaryIO, sqlite_schema: List[Schema], page_size: int
    ):
        desired_table_schema = next(
            (schema for schema in sqlite_schema if schema.name == self.table_name), None
        )

        creation_query = desired_table_schema.sql.split(b"\r")[0].decode("utf-8")
        schema = get_column_names_from_creation_query(creation_query)
        pages = get_table_leaf_pages(
            database_file, sqlite_schema, self.table_name, page_size
        )

        rows = [
            row
            for page in pages
            for row in page.read_records_with_schema(database_file, schema)
        ]

        if self.value_filter:
            rows = filter(self.value_filter, rows)

        column_values_per_row = [
            [
                row[column_name].decode("utf8")
                if type(row[column_name]) is bytes
                else row[column_name]
                for column_name in self.requested_column_names
            ]
            for row in rows
        ]
        print(
            "\n".join(
                [
                    "|".join([str(entry) for entry in row])
                    for row in column_values_per_row
                ]
            )
        )


def get_table_leaf_pages(
    database_file: BinaryIO,
    sqlite_schema: List[Schema],
    table_name: str,
    page_size: int,
) -> List[Page]:
    """
    Given a table name, return all the pages contianing rows for the table.

    If the table is small and fits in a single page, just return that leaf page.
    Else, read the interior node representing the table and, for each pointer,
    collect the pointed page.
    """
    desired_table_schema = next(
        (schema for schema in sqlite_schema if schema.name == table_name), None
    )

    desired_table_rootpage = desired_table_schema.rootpage - 1
    desired_table_location = page_start(desired_table_rootpage, page_size)

    desired_table_location = page_start(desired_table_rootpage, page_size)
    database_file.seek(desired_table_location)
    page_bytes = bytearray(database_file.read(page_size))

    page = Page.from_bytes(page_bytes, desired_table_location)

    if page.page_type == PageType.INTERIOR_TABLE:
        return page.load_pointed_table_leaf_pages(database_file, page_size)
    else:
        return [page]


def get_column_names_from_creation_query(sql_creation_query: str) -> List[str]:
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

    # Extract everything before and during the first parenthesis match
    match = re.search(TABLE_CREATION_REGEX, sql_creation_query)

    if match:
        sql_creation_query = match.group(1)
    else:
        raise ValueError(
            "Could not regex match sql creation query.", sql_creation_query
        )

    parsed_creation_query = sqlparse.parse(sql_creation_query)[0]

    reserved_sqlite_keywords = ["autoincrement", "primary key", "not null"]
    column_names = []
    for token in parsed_creation_query.tokens:
        if isinstance(token, Parenthesis):  #
            for subtoken in token.tokens:
                if isinstance(subtoken, Identifier):
                    column_names.append(subtoken.get_name())
                if isinstance(subtoken, IdentifierList):
                    for identifier in subtoken.get_identifiers():
                        # bug: SQLParse seems to always think modifier keywords are identifiers :(
                        if identifier.value.lower() not in reserved_sqlite_keywords:
                            try:
                                column_names.append(identifier.get_name())
                            except AttributeError:
                                raise TypeError("The token that failed is", identifier)

            break  # no need to iterate more

    return column_names
