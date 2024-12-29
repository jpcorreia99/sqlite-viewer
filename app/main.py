import sys
from app.pages import Page
from app.utils import page_start

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        # You can use print statements as follows for debugging, they'll be visible when running tests.
        print("Logs from your program will appear here!", file=sys.stderr)

        # Skip the first 16 bytes of the header, its the sqlite version
        # 0 - means to seek from the start of the file
        database_file.seek(16, 0)
        page_size = int.from_bytes(database_file.read(2), byteorder="big")

        # Number of tables:
        # The easy way would be to assume only one inner node and read the cell count (which
        # would equal the number of rows in the SQLite_schema table)

        # Unfortunately, if we have more than just one inner node, this no longer holds true,
        # Correct approach:
        # 1. Start at page 1, and look at its "cell pointer array" to find all the offsets to all its cells.
        # 2. Go to the cell area and read them. We know the 1st page is an inner node
        # 3. For each cell, they have a page pointer (number), go the the page (by basing on page size) and read the page type.
        # 4. If the page type is leaf page we can increment table count, otherwise repeat the cycle

        first_page_start = page_start(0, page_size)
        database_file.seek(first_page_start, 0)

        # Read 8 byte header
        first_page_bytes = database_file.read(page_size)
        first_page = Page.from_bytes(first_page_bytes)

        # Read cells
        #       database_file.seek(100 + 8)
        #       cell_pointers = [
        #           int.from_bytes(database_file.read(2), "big")
        #           for _ in range(page_header.cell_count)
        #       ]

        print(f"database page size: {page_size}")
        print(f"number of tables:  {first_page.cell_count}")
else:
    print(f"Invalid command: {command}")
