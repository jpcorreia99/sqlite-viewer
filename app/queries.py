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

from app.pages import Page, load_page_at_location
from app.filtering import ValueFilter
from app.rows import Schema
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

    def execute(
        self, database_file: BinaryIO, sqlite_schema: List[Schema], page_size: int
    ):
        if self.query_components[1].lower() == "count(*)":
            table_pages = get_table_leaf_pages(
                database_file, sqlite_schema, self.table_name, None, page_size
            )
            print(sum([table_page.cell_count for table_page in table_pages]))
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
            database_file,
            sqlite_schema,
            self.table_name,
            self.value_filter,
            page_size,
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
    value_filter: Optional[ValueFilter],
    page_size: int,
) -> List[Page]:
    """
    Given a table name, return all the pages contianing rows for the table.

    If the table is small and fits in a single page, just return that leaf page.
    Else, read the interior node representing the table and, for each pointer,
    collect the pointed page.
    """

    # If there's a WHERE clause, search if there's an index we should use
    row_ids = None
    if value_filter:
        index_name = generate_index_name(table_name, value_filter.column)
        index_schema = next(
            (schema for schema in sqlite_schema if schema.table_name == index_name),
            None,
        )

        if index_schema:
            row_ids = load_filter_compliant_row_ids_via_index(
                database_file, index_schema, value_filter, page_size
            )

    table_schema = next(
        (schema for schema in sqlite_schema if schema.table_name == table_name),
        None,
    )

    desired_table_rootpage = table_schema.rootpage - 1
    page = load_page_at_location(database_file, desired_table_rootpage, page_size)

    return page.load_table_leaf_pages(database_file, page_size, row_ids)


def load_filter_compliant_row_ids_via_index(
    database_file: BinaryIO,
    index_schema: Schema,
    value_filter: ValueFilter,
    page_size: int,
) -> List[int]:
    """
    Given an index and a value filter representing a WHERE condition,
    return row IDs for payloads satisfying the condition.
    """
    desired_table_rootpage = index_schema.rootpage - 1

    page = load_page_at_location(database_file, desired_table_rootpage, page_size)

    return page.load_filter_compliant_row_ids(database_file, value_filter, page_size)


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

    SQLparse is not well fit for parsing the creation query. As such,
    we opt for a more manual choice of regex matching the column description and
    stripping all sqlite keywords to just keep the column names
    """
    reserved_sqlite_keywords = [
        "autoincrement",
        "primary key",
        "not null",
        "text",
        "integer",
        ",",
    ]

    sql_creation_query = sql_creation_query.replace("\n", "")
    sql_creation_query = sql_creation_query.replace("\t", "")

    # find the content inside parenthesis
    match = re.search(TABLE_CREATION_REGEX, sql_creation_query)
    if match:
        sql_creation_query = match.group(1)
    else:
        raise ValueError(
            "Could not regex match sql creation query.", sql_creation_query
        )

    for keyword in reserved_sqlite_keywords:
        sql_creation_query = sql_creation_query.replace(keyword, "")

    sql_creation_query = re.sub(r"\s+", " ", sql_creation_query).strip()

    # return all words or quoted strings
    return re.findall(r'"[^"]*"|\S+', sql_creation_query)


def generate_index_name(table_name: str, column_name: str) -> str:
    return f"idx_{table_name}_{column_name}"


def get_index_on_column_if_exists(
    table_name: str, sqlite_schema: List[Schema], value_filter: ValueFilter
) -> Optional[Schema]:
    if value_filter:
        index_name = generate_index_name(table_name, value_filter.column)

        return next(
            (schema for schema in sqlite_schema if schema.table_name == index_name),
            None,
        )
