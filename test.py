import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Statement, Parenthesis

query = "CREATE TABLE grape (id integer primary key, butterscotch text,watermelon text,vanilla text,coffee text,pistachio text)"

parsed_query = sqlparse.parse(query)[0]


column_names = []
print("Parsed creation query: ", parsed_query)
for token in parsed_query.tokens:
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


print(parsed_query)
