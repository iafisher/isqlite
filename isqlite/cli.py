import collections
import re
import sys

import click
import sqliteparser
from xcli import Table, colors, input2

from . import Database


@click.group()
def cli():
    pass


@cli.command(name="add-column")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("column")
def main_add_column(path_to_database, table, column):
    """
    Add a column to a table.
    """
    with Database(path_to_database) as db:
        db.add_column(table, column)
        print(f"Column added to table {table!r}.")


@cli.command(name="alter-column")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("column")
def main_alter_column(path_to_database, table, column):
    """
    Alter a column's definition.
    """
    with Database(path_to_database) as db:
        column_name, column_def = column.split(maxsplit=1)
        db.alter_column(table, column_name, column_def)
        print(f"Column {column_name!r} altered in table {table!r}.")


@cli.command(name="create")
@click.argument("path_to_database")
@click.argument("table")
@click.option(
    "--auto-timestamp",
    multiple=True,
    default=[],
    help="Automatically fill in zero or more columns with the current time.",
)
def main_create(path_to_database, table, *, auto_timestamp):
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

        print("To set a value to be null, enter NULL (case-sensitive).")
        print("To set a value to the empty string, enter a blank line.")
        print()

        payload = {}
        for name, definition in schema.items():
            if any(
                isinstance(c, sqliteparser.ast.PrimaryKeyConstraint)
                for c in definition.constraints
            ):
                continue

            if name in auto_timestamp:
                continue

            print(definition)
            v = input2("? ", verify=lambda v: validate_column(definition.type, v))

            if v == "NULL":
                v = None

            payload[name] = v

        pk = db.create(table, payload, auto_timestamp=auto_timestamp,)
        print(f"Row {pk} created.")


@cli.command(name="create-table")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("columns", nargs=-1)
def main_create_table(path_to_database, table, columns):
    """
    Create a table.
    """
    with Database(path_to_database) as db:
        db.create_table(table, *columns)
        print("Table {table!r} created.")


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


@cli.command(name="drop-table")
@click.argument("path_to_database")
@click.argument("table")
def main_drop_table(path_to_database, table):
    """
    Drop a table from the database.
    """
    with Database(path_to_database) as db:
        count = db.count(table)
        print(f"WARNING: Table {table!r} contains {count} row(s) of data.")
        print()
        if not click.confirm("Are you sure you wish to drop this table?"):
            print()
            print("Operation aborted.")
            sys.exit(1)

        db.drop_table(table)
        print()
        print(f"Table {table!r} dropped from the database.")


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


@cli.command(name="rename-column")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("old_name")
@click.argument("new_name")
def main_rename_column(path_to_database, table, old_name, new_name):
    """
    Rename a column.
    """
    with Database(path_to_database) as db:
        db.rename_column(table, old_name, new_name)
        print(f"Column {old_name!r} renamed to {new_name!r} in table {table!r}.")


@cli.command(name="reorder-columns")
@click.argument("path_to_database")
@click.argument("table")
@click.argument("columns", nargs=-1)
def main_reorder_columns(path_to_database, table, columns):
    """
    Change the order of columns in a table.
    """
    with Database(path_to_database) as db:
        db.reorder_columns(table, columns)
        print(f"Columns of table {table!r} reordered.")


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
                print(f"Table {table!r} not found.")
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
@click.option(
    "--auto-timestamp",
    multiple=True,
    default=[],
    help="Automatically fill in zero or more columns with the current time.",
)
def main_update(path_to_database, table, pk, *, auto_timestamp):
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
            definition = schema[key]
            if any(
                isinstance(c, sqliteparser.ast.PrimaryKeyConstraint)
                for c in definition.constraints
            ):
                continue

            if key in auto_timestamp:
                continue

            print(definition)
            print("Currently:", value)
            v = input2("? ", verify=lambda v: validate_column(definition.type, v))

            if v == "NULL":
                v = None

            updates[key] = v

        if not updates:
            print()
            print("No updates specified.")
            sys.exit(1)

        db.update_by_rowid(table, pk, updates, auto_timestamp=auto_timestamp)
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
    if not statement.endswith(";"):
        statement = statement + ";"

    tree = sqliteparser.parse(statement)[0]
    return collections.OrderedDict((c.name, c) for c in tree.columns)


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
