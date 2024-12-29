# record format
# https://www.sqlite.org/fileformat.html#record_format
# A record contains a header and a body, in that order
from dataclasses import dataclass


# https://www.sqlite.org/fileformat.html#storage_of_the_sql_database_schema
@dataclass
class Schema:
    table_type: str
    name: str
    table_name: str
    rootpage: int
    sql: str
