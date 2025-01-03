import sys
from app.pages import Page
from app.queries import Query

database_file_path = sys.argv[1]
command = sys.argv[2]

with open(database_file_path, "rb") as database_file:
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!", file=sys.stderr)

    # Skip the first 16 bytes of the header, its the sqlite version
    # 0 - means to seek from the start of the file
    database_file.seek(16, 0)
    page_size = int.from_bytes(database_file.read(2), byteorder="big")

    # we will read the first full page, seek back to the start of the file
    database_file.seek(0, 0)
    first_page_bytes = bytearray(database_file.read(page_size))
    first_page = Page.from_file(database_file, 0, is_first_page=True)

    # The first page in an sqlite db is a special node that contains the schema of the db
    sqlite_schema = first_page.read_sqlite_schema(database_file)

    database_file.seek(0)

    if command == ".dbinfo":
        print(f"database page size: {page_size}")
        print(f"number of tables:  {first_page.cell_count}")
    elif command == ".tables":
        print(
            f"table names: {' '.join([schema.table_name for schema in sqlite_schema])}"
        )
    else:
        query = Query.parse_query(command)
        query.execute(database_file, sqlite_schema, page_size)
