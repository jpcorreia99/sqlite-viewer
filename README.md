#  SQLite viewer
[![progress-banner](https://backend.codecrafters.io/progress/sqlite/daa83443-036c-4926-9f5c-0cbeefcf0fc9)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# Contents
   - **[What?](#what)**
   - **[Usage](#usage)**
     - **[Supported Queries](#supported-queries)**
   - **[Understanding SQLite](#understanding-sqlite)**

# What?

Full implementation of the codecrafters ["Build Your Own SQLite" Challenge](https://codecrafters.io/challenges/sqlite).

This challenge consists of building program capable of interpreting the SQLite file format and answering basic queries.

I highly recommend codecrafters as a way to deepend system's knowledge and practice coding in a non-surface level context. 

# Usage

The `sqlite_viewer.sh` script supports multiple commands:

-  **`./sqlite_viewer.sh <path_to_db> ".dbinfo"`**
   -  Returns basic info about the db, such as page size and number of tables
-  **`./sqlite_viewer.sh <path_to_db> ".tables"`**
   -  Returns the name of the existing db tables
-  **`./sqlite_viewer.sh <path_to_db> <QUERY>`**
   -  Executes the query and prints the returned rows

### Supported queries
**NOTE**: As the program depends on external libraries, please run `pipenv install`to setup.


As the name implies, this program only supports **READ** queries.

This repository includes some sample databases in `databases/` for exploration.

List of types of supported queries and examples:

- **Total Count queries**
  - `./sqlite_viewer.sh databases/sample.db "SELECT COUNT(*) FROM apples"`
- **Column selection with filtering spanning multiple pages**
   - `./sqlite_viewer.sh databases/superheroes.db "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'"`
 - **Column selection relying on indices**
   - `./sqlite_viewer.sh databases/companies.db "SELECT id, name FROM companies WHERE country = 'eritrea'"`
   - This query is extra performant due to the existence of an `idx_companies_country`index

## Understanding SQLite
There are plenty of very good resources to understand the SQLite file format:

- SQLite file format: https://www.sqlite.org/fileformat2.html
- Diagram on page structure: https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/#table-interior-page
- Page structure interactive viz: https://torymur.github.io/sqlite-repr/
- Great blog post explainign B+Trees in SQLite https://fly.io/blog/sqlite-internals-btree/

**Note**: *Node* and *Page* are used interchangeably.

I wrote some detailed ramblings in `docs/notes.md`, but here's the super short TLDR:

- SQLite stores tables and indices as **B+Trees**.These trees contain **interior** and **leaf** nodes.

- So in total we have
  - **Table Interior nodes**
    - When a table span multiple nodes, these nodes point to which leaf nodes contain rows with different Row_Ids 
  - **Table Leaf nodes**
    - Store rows of the tables
  - **Index Interior nodes**
    - When indices span multiple nodes, points to which nodes are reponsible for each section of the keyspace
  - **Index Leaf nodes**
    - For a section of the keyspace, point which row_ids satisfy the index
  
  Pretty much all pages follow the same structure, whith slight variations:
  - **Page header**: an 8-12 bytes long sequence that indicates the page type, and how many cells (rows or pointer) are in this page
  - **Cell pointer array**: an array of 2-byte cells pointing to where to search, inside of the page, for the payload
  - **Payload array**: a variable size array of payloads. In table leaf cells these are rows, everywhere else these are pairs of values + pointers to help navigate the tree/index. 

An interesting detail of SQLite is the usage of **varints**

### Varints 
(Taken from https://fly.io/blog/sqlite-internals-btree/)

This encoding is used so that we don’t use a huge 8-byte field for every integer.

The high bit is used as a flag to indicate if there are more bytes to be read and the other 7 bits are our actual data.

To represent 1,000, we start with its binary representation split into 7 bit chunks:

> 0000111 1101000

We add a 1 to signify mnore chunks to come and 0 for the final

> 10000111 01101000

SQLite goes even further and uses this type to encode not only integers but data types, as per https://www.sqlite.org/fileformat2.html#record_format

