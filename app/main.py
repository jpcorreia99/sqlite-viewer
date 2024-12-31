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

    # Number of tables:
    # The easy way would be to assume only one leaf node and read the cell count (which
    # would equal the number of rows in the SQLite_schema table)

    # Unfortunately, if we have more than just one inner node, this no longer holds true,
    # Correct approach:
    # 1. Start at page 1, and look at its "cell pointer array" to find all the offsets to all its cells.
    # 2. Go to the cell area and read them. We know the 1st page is an inner node
    # 3. For each cell, they have a page pointer (number), go the the page (by basing on page size) and read the page type.
    # 4. If the page type is leaf page we can increment table count, otherwise repeat the cycle

    # we will read the first full page, seek back to the start of the file
    database_file.seek(0, 0)
    first_page_bytes = bytearray(database_file.read(page_size))
    first_page = Page.from_bytes(first_page_bytes, 0, is_first_page=True)
    database_file.seek(0)
    if command == ".dbinfo":
        print(f"database page size: {page_size}")
        print(f"number of tables:  {first_page.cell_count}")
    elif command == ".tables":
        # The first page in an sqlite db is a special node that contains the schema of the db
        sqlite_schema = first_page.read_sqlite_schema(database_file)
        print(f"table names: {' '.join([schema.name for schema in sqlite_schema])}")
    else:  # for now assume it's a select count query
        query = Query.parse_query(command)
        query.execute(database_file, first_page, page_size)
