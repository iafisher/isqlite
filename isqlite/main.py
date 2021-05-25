import collections
import re
import sys

import click
from xcli import Table, colors, input2

from . import Database


@click.group()
def cli():
    pass


@cli.command(name="create")
@click.argument("path_to_database")
@click.argument("table")
def main_create(path_to_database, table):
    """
    Create a new row interactively.
    """
    with Database(path_to_database) as db:
        schema_row = db.sql(
            "SELECT sql FROM sqlite_master WHERE name = :name AND type = 'table'",
            {"name": table},
            multiple=False,
        )

        if schema_row is None:
            print(f"Table {table!r} not found.")
            sys.exit(1)

        schema = parse_create_table_statement(schema_row["sql"])

        payload = {}
        for name, definition in schema.items():
            if name in {"id", "created_at", "last_updated_at"}:
                continue

            column_type = definition.split(maxsplit=1)[0]
            print(colors.blue(name), definition)
            v = input2("? ", verify=lambda v: validate_column(column_type, v))

            if not v and column_type != "TEXT":
                v = None

            payload[name] = v

        pk = db.create(table, payload)
        print(f"Row {pk} created.")


@cli.command(name="delete")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("pk", type=int)
def main_delete(path_to_database, table, pk):
    """
    Delete a row.
    """
    with Database(path_to_database) as db:
        try:
            row = db.get_by_rowid(table, pk)
        except Exception:
            # This can happen, e.g., if a timestamp column is invalidly formatted and
            # the Python wrapper around sqlite3 chokes trying to convert it to a
            # datetime.
            print(
                "Unable to fetch row from database, possibly due to validation error."
            )
        else:
            if row is None:
                print(f"Row {pk} not found in table {table!r}.")
                sys.exit(1)

            prettyprint_row(row)

        print()
        if not click.confirm("Are you sure you wish to delete this record?"):
            print()
            print("Operation aborted.")
            sys.exit(1)

        db.delete_by_rowid(table, pk)
        print()
        print(f"Row {pk} deleted.")


@cli.command(name="drop-column")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("column")
def main_drop_column(path_to_database, table, column):
    """
    Drop a column from a table.
    """
    with Database(path_to_database) as db:
        count = db.count(table)
        print(f"WARNING: Table {table!r} contains {count} row(s) of data.")
        print()
        if not click.confirm("Are you sure you wish to drop this column?"):
            print()
            print("Operation aborted.")
            sys.exit(1)

        db.drop_column(table, column)
        print()
        print(f"Column {column!r} dropped from table {table!r}.")


@cli.command(name="get")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("pk", type=int)
def main_get(path_to_database, table, pk):
    """
    Fetch a single row.
    """
    with Database(path_to_database, readonly=True) as db:
        row = db.get_by_rowid(table, pk)
        if row is None:
            print(f"Row {pk} not found in table {table!r}.")
        else:
            prettyprint_row(row)


@cli.command(name="list")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("--where", default="1")
def main_list(path_to_database, table, *, where):
    """
    List the rows in the table, optionally filtered by a SQL clause.
    """
    with Database(path_to_database, readonly=True) as db:
        rows = db.list(table, where=where)

        if not rows:
            if where:
                print(f"No rows found in table {table!r} with constraint {where!r}.")
            else:
                print(f"No row founds in table {table!r}.")
        else:
            prettyprint_rows(rows)


@cli.command(name="schema")
@click.argument("path_to_database")
@click.argument("table", default="")
def main_schema(path_to_database, table=""):
    """
    Print the schema of a single table or the whole database.
    """
    with Database(path_to_database, readonly=True) as db:
        if table:
            rows = db.sql(
                "SELECT sql FROM sqlite_master WHERE name = :name AND type = 'table'",
                {"name": table},
            )

            if not rows:
                print("Table {table!r} not found.")
                sys.exit(1)

            prettyprint_row(parse_create_table_statement(rows[0]["sql"]))
        else:
            rows = db.sql(
                "SELECT name, sql FROM sqlite_master "
                + "WHERE NOT name LIKE 'sqlite%' AND type = 'table'"
            )

            if not rows:
                print("No tables found.")
                sys.exit(1)

            for i, row in enumerate(rows):
                if i != 0:
                    print()

                print(row["name"])
                prettyprint_row(parse_create_table_statement(row["sql"]))


@cli.command(name="sql")
@click.argument("path_to_database")
@click.argument("query")
@click.option("--write", is_flag=True, default=False)
def main_sql(path_to_database, query, *, write):
    """
    Run a SQL command.
    """
    readonly = not write
    with Database(path_to_database, readonly=readonly) as db:
        rows = db.sql(query)
        if rows:
            prettyprint_rows(rows)
        else:
            print("No rows found.")


@cli.command(name="update")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("pk", type=int)
def main_update(path_to_database, table, pk):
    """
    Update an existing row interactively.
    """
    with Database(path_to_database) as db:
        schema_row = db.sql(
            "SELECT sql FROM sqlite_master WHERE name = :name AND type = 'table'",
            {"name": table},
            multiple=False,
        )

        if schema_row is None:
            print(f"Table {table!r} not found.")
            sys.exit(1)

        schema = parse_create_table_statement(schema_row["sql"])

        original = db.get_by_rowid(table, pk)
        if original is None:
            print(f"Row {pk} not found in table {table!r}.")

        print("To clear a value, enter NULL (case-sensitive).")
        print("To keep an existing value, leave the field blank.")
        print()

        updates = {}
        for key, value in original.items():
            if key in {"id", "created_at", "last_updated_at"}:
                continue

            definition = schema[key]
            column_type = definition.split(maxsplit=1)[0]
            print(colors.blue(key), definition)
            print("Currently:", value)
            v = input2("? ", verify=lambda v: validate_column(column_type, v))

            if v:
                if v == "NULL":
                    updates[key] = None
                else:
                    updates[key] = v

        if not updates:
            print()
            print("No updates specified.")
            sys.exit(1)

        db.update_by_rowid(table, pk, updates)
        print()
        print(f"Row {pk} updated.")


def prettyprint_rows(rows):
    for i, row in enumerate(rows):
        if i != 0:
            print()

        prettyprint_row(row)


def prettyprint_row(row):
    table = Table(padding=2)
    for key, value in row.items():
        table.row(colors.blue(key), value)
    print(table)


def parse_create_table_statement(statement):
    statement = statement.strip()
    open_index = statement.find("(")
    close_index = statement.rfind(")")
    if open_index == -1 or close_index == -1:
        raise SyntaxError

    # This crude method will fail in some cases, e.g. if a column definition contains a
    # string literal expression (e.g., in a DEFAULT clause) that contains a comma.
    columns = statement[open_index + 1 : close_index].split(",")

    r = collections.OrderedDict()
    for column in columns:
        words = column.split(maxsplit=1)
        r[words[0].strip()] = words[1].strip()

    return r


timestamp_pattern = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2} "
    + r"[0-9]{1,2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}\+[0-9]{2}:[0-9]{2}$"
)
date_pattern = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def validate_column(column_type, v):
    # Accept empty input and let the database throw an error if it's not allowed.
    #
    # (We can't also do this for type validation because SQLite doesn't do type
    #  validation.)
    if not v:
        return True

    if column_type == "TIMESTAMP":
        return timestamp_pattern.match(v)
    elif column_type == "DATE":
        return date_pattern.match(v)
    elif column_type == "INTEGER":
        return v.isdigit()
    elif column_type == "BOOLEAN":
        return v == "0" or v == "1"
    else:
        return True
