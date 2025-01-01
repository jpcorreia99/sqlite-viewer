import sqlparse

from sqlparse.sql import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML, Wildcard


query = 'CREATE TABLE "superheroes" (id integer primary key autoincrement, name text not null, eye_color text, hair_color text, appearance_count integer, first_appearance text, first_appearance_year text)P\t\x06\x17++\x01Ytablesqlite_sequencesqlite_sequence\x03CREATE TABLE sqlite_sequence(name,seq)\x00\x00\x007\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00l'


def get_column_names_from_creation_query(parsed_creation_query):
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
    reserved_sqlite_keywords = ["autoincrement", "primary key", "not null"]

    column_names = []
    for token in parsed_creation_query.tokens:
        if isinstance(token, Parenthesis):
            print("ª!!")  #
            for subtoken in token.tokens:
                print("Subtoken", subtoken)
                if isinstance(subtoken, Identifier):
                    column_names.append(subtoken.get_name())
                if isinstance(subtoken, IdentifierList):
                    for identifier in subtoken.get_identifiers():
                        # bug: SQLParse seems to always think autoincrement keyword is an identifier :(
                        if (
                            identifier.value.lower() != "autoincrement"
                            and identifier.value.lower() != "primary key"
                            and identifier.value.lower() != "not null"
                        ):
                            try:
                                column_names.append(identifier.get_name())
                            except AttributeError:
                                raise TypeError(
                                    "The token that failed is", identifier.value
                                )

            break  # no need to iterate more
    print("here!", column_names)
    return column_names


import re


# Extract the first parentheses section
match = re.search(r"^(.*?\(.*?\))", query)

if match:
    first_parentheses = match.group(1)
    print(first_parentheses)
    print(get_column_names_from_creation_query(sqlparse.parse(first_parentheses)[0]))

else:
    print("No parentheses found.")


# print(get_column_names_from_creation_query(sqlparse.parse(query)[0]))
