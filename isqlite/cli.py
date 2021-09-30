import collections
import importlib
import readline  # noqa: F401
import shutil
import sys
import tempfile
import traceback

import click
from tabulate import tabulate

from .core import (
    CreateTableMigration,
    Database,
    DatabaseMigrator,
    DropTableMigration,
    schema_module_to_dict,
)

# Help strings used in multiple places.
COLUMNS_HELP = "Only display these columns in the results."
HIDE_HELP = "Hide these columns in the results."
PAGE_HELP = (
    "Select the page of results to show, "
    + "if the table is too wide to display in one screen."
)
LIMIT_HELP = "Limit the number of rows returned from the database."
OFFSET_HELP = "Offset a query with --limit."
ORDER_BY_HELP = "Order the results by one or more columns."
DESC_HELP = (
    "When combined with --order-by, order the results in descending rather than "
    + "ascending order."
)


@click.group()
def cli():
    pass


@cli.command(name="add-column")
@click.option("--db", "db_path")
@click.argument("table")
@click.argument("column")
def main_add_column(db_path, table, column):
    """
    Add a column to a table.
    """
    with Database(db_path) as db:
        db.add_column(table, column)
        print(f"Column added to table {table!r}.")


@cli.command(name="alter-column")
@click.option("--db", "db_path")
@click.argument("table")
@click.argument("column")
def main_alter_column(db_path, table, column):
    """
    Alter a column's definition.
    """
    with Database(db_path) as db:
        column_name, column_def = column.split(maxsplit=1)
        db.alter_column(table, column_name, column_def)
        print(f"Column {column_name!r} altered in table {table!r}.")


@cli.command(name="create")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
def main_create(db_path, schema_path, table):
    """
    Create a new row interactively.
    """
    full_schema = get_schema_dict(schema_path)
    with Database(db_path) as db:
        schema = full_schema.get(table)
        if schema is None:
            print(f"Table {table!r} not found in schema.")
            sys.exit(1)

        payload = {}
        for column in schema.columns.values():
            if column.name in ("id", "created_at", "last_updated_at"):
                continue

            while True:
                raw_value = input(f"{column.description()}? ").strip()
                value, is_valid = column.validate(raw_value)
                if is_valid:
                    payload[column.name] = value
                    break

        pk = db.create(table, payload)
        print(f"Row {pk} created.")


@cli.command(name="create-table")
@click.option("--db", "db_path")
@click.argument("table")
@click.argument("columns", nargs=-1)
def main_create_table(db_path, table, columns):
    """
    Create a table.
    """
    with Database(db_path) as db:
        db.create_table(table, *columns)
        print(f"Table {table!r} created.")


@cli.command(name="delete")
@click.option("--db", "db_path")
@click.argument("table")
@click.argument("pk", type=int, required=False, default=None)
@click.option("-w", "--where", default="")
def main_delete(db_path, table, pk, *, where):
    """
    Delete a row.
    """
    with Database(db_path) as db:
        if pk is not None:
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
            print(f"Row {pk} deleted from table {table!r}.")
        else:
            n = db.count(table, where=where)
            if not where:
                msg = f"Are you sure you wish to delete ALL {n} row(s) from {table!r}?"
            else:
                msg = f"Are you sure you wish to delete {n} row(s) from {table!r}?"

            if not click.confirm(msg):
                print()
                print("Operation aborted.")
                sys.exit(1)

            # `Database.delete` doesn't accept a blank `where` parameter for safety
            # reasons, so we use an explicit WHERE clause that will match every row.
            if not where:
                where = "1"

            db.delete(table, where=where)
            print()
            print(f"{n} row(s) from table {table!r} deleted.")


@cli.command(name="drop-column")
@click.option("--db", "db_path")
@click.argument("table")
@click.argument("column")
def main_drop_column(db_path, table, column):
    """
    Drop a column from a table.
    """
    with Database(db_path) as db:
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
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
def main_drop_table(db_path, schema_path, table):
    """
    Drop a table from the database.
    """
    with Database() as db:
        count = db.count(table)
        print(f"WARNING: Table {table!r} contains {count} row(s) of data.")
        print()
        if not click.confirm("Are you sure you wish to drop this table?"):
            print()
            print("Operation aborted.")
            sys.exit(1)

    schema_module = get_schema_module(schema_path)
    with DatabaseMigrator(schema_module=schema_module) as migrator:
        migrator.drop_table(table)
        print()
        print(f"Table {table!r} dropped from the database.")


@cli.command(name="get")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
@click.argument("pk", type=int)
def main_get(db_path, schema_path, table, pk):
    """
    Fetch a single row.
    """
    schema_module = get_schema_module(schema_path)
    schema_dict = schema_module_to_dict(schema_module)
    with Database(db_path, readonly=True, schema_module=schema_module) as db:
        row = db.get_by_rowid(table, pk, get_related=True)
        for key, value in row.items():
            if isinstance(value, collections.OrderedDict):
                row[key] = get_column_as_string(schema_dict, table, key, value)

        if row is None:
            print(f"Row {pk} not found in table {table!r}.")
        else:
            prettyprint_row(row)


@cli.command(name="list")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
@click.option("-w", "--where", default="")
@click.option("-s", "--search")
@click.option("--columns", multiple=True, default=[], help=COLUMNS_HELP)
@click.option("--hide", multiple=True, default=[], help=HIDE_HELP)
@click.option("-p", "--page", default=1, help=PAGE_HELP)
@click.option("--limit", default=None, help=LIMIT_HELP)
@click.option("--offset", default=None, help=OFFSET_HELP)
@click.option("--order-by", multiple=True, default=[], help=ORDER_BY_HELP)
@click.option("--desc", is_flag=True, default=False, help=DESC_HELP)
def main_list(
    db_path,
    schema_path,
    table,
    *,
    where,
    search,
    columns,
    hide,
    page,
    limit,
    offset,
    order_by,
    desc,
):
    """
    List the rows in the table, optionally filtered by a SQL clause.
    """
    base_list(
        db_path,
        schema_path,
        table,
        where=where,
        search=search,
        columns=columns,
        hide=hide,
        page=page,
        limit=limit,
        offset=offset,
        order_by=order_by,
        desc=desc,
    )


def base_list(
    db_path,
    schema_path,
    table,
    *,
    where,
    search,
    columns,
    hide,
    page,
    limit,
    offset,
    order_by,
    desc,
):
    """
    Base implementation shared by `main_list` and `main_search`
    """
    schema_module = get_schema_module(schema_path)
    schema_dict = schema_module_to_dict(schema_module)
    with Database(db_path, readonly=True, schema_module=schema_module) as db:
        rows = db.list(
            table,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
            descending=desc if order_by else None,
            # `get_related` is only possible when the database has a schema.
            get_related=schema_module is not None,
        )

        for row in rows:
            for key, value in row.items():
                if isinstance(value, collections.OrderedDict):
                    row[key] = get_column_as_string(schema_dict, table, key, value)

        if search:
            search = search.lower()
            filtered_rows = []
            for row in rows:
                yes = False
                for value in row.values():
                    if isinstance(value, str) and search in value.lower():
                        yes = True
                    elif isinstance(value, collections.OrderedDict):
                        for subvalue in value.values():
                            if isinstance(subvalue, str) and search in subvalue.lower():
                                yes = True
                                break

                    if yes:
                        break

                if yes:
                    filtered_rows.append(row)
            rows = filtered_rows

        if not rows:
            if search:
                print(
                    f"No rows found in table {table!r} "
                    + f"matching search query {search!r}."
                )
            elif where:
                print(f"No rows found in table {table!r} with constraint {where!r}.")
            else:
                print(f"No row founds in table {table!r}.")
        else:
            prettyprint_rows(rows, columns=columns, hide=hide, page=page)


@cli.command(name="migrate")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table", required=False, default=None)
@click.option(
    "--write",
    is_flag=True,
    default=False,
    help="Perform the migration. By default, migrate will only do a dry run.",
)
@click.option(
    "--no-backup",
    is_flag=True,
    default=False,
    help="Don't create a backup of the database.",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Run the database in debug mode.",
)
def main_migrate_wrapper(*args, **kwargs):
    """
    Migrate the database to match the Python schema.
    """
    main_migrate(*args, **kwargs)


def main_migrate(db_path, schema_path, table, *, write, no_backup, debug):
    schema_module = get_schema_module(schema_path)
    with DatabaseMigrator(
        db_path, schema_module=schema_module, readonly=not write, debugger=debug
    ) as migrator:
        diff = migrator.diff(table)
        if not diff:
            print("Nothing to migrate - database matches schema.")
            return

        if write and not no_backup:
            _, backup_name = tempfile.mkstemp(
                prefix="isqlite-backup-", suffix=".sqlite3"
            )
            shutil.copy2(db_path, backup_name)

        tables_created = 0
        tables_dropped = 0
        printed = False
        for table, table_diff in diff.items():
            if printed:
                print()
            else:
                printed = True

            if len(table_diff) == 1 and isinstance(table_diff[0], CreateTableMigration):
                op = table_diff[0]
                print(f"Create table {op.table_name}")
                tables_created += 1
            elif len(table_diff) == 1 and isinstance(table_diff[0], DropTableMigration):
                op = table_diff[0]
                print(f"Drop table {op.table_name}")
                tables_dropped += 1
            else:
                print(f"Table {table}")
                for op in table_diff:
                    print(f"- {op}")

        if write:
            try:
                migrator.apply_diff(diff)
            except Exception:
                traceback.print_exc()

                print()
                print("Migration rolled back due to Python exception.")
                sys.exit(2)
            finally:
                if not no_backup:
                    print()
                    print(f"Backup of database before migration saved at {backup_name}")

        print()
        print()
        if tables_created > 0:
            if write:
                print("Created ", end="")
            else:
                print("Would have created ", end="")
            print(f"{tables_created} table(s).")

        if tables_dropped > 0:
            if write:
                print("Dropped ", end="")
            else:
                print("Would have dropped ", end="")
            print(f"{tables_dropped} table(s).")

        if diff:
            total_ops = (
                sum(len(table_diff) for table_diff in diff.values())
                - tables_created
                - tables_dropped
            )
            total_tables = len(diff) - tables_created - tables_dropped

            if total_ops > 0:
                if write:
                    print("Performed ", end="")
                else:
                    print("Would have performed ", end="")
                print(f"{total_ops} operation(s) on {total_tables} table(s).")

        if not write:
            print()
            print("To perform this migration, re-run with the --write flag.")


@cli.command(name="rename-column")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
@click.argument("old_name")
@click.argument("new_name")
def main_rename_column(schema_path, db_path, table, old_name, new_name):
    """
    Rename a column.
    """
    schema_module = get_schema_module(schema_path)
    with DatabaseMigrator(db_path, schema_module=schema_module) as migrator:
        migrator.rename_column(table, old_name, new_name)
        print(f"Column {old_name!r} renamed to {new_name!r} in table {table!r}.")


@cli.command(name="rename-table")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
@click.argument("new_name")
def main_rename_table(schema_path, db_path, table, new_name):
    """
    Rename a table.
    """
    schema_module = get_schema_module(schema_path)
    with DatabaseMigrator(db_path, schema_module=schema_module) as migrator:
        migrator.rename_table(table, new_name)
        print(f"Table {table!r} renamed to {new_name!r}.")


@cli.command(name="reorder-columns")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
@click.argument("columns", nargs=-1)
def main_reorder_columns(schema_path, db_path, table, columns):
    """
    Change the order of columns in a table.
    """
    schema_module = get_schema_module(schema_path)
    with DatabaseMigrator(db_path, schema_module=schema_module) as migrator:
        migrator.reorder_columns(table, columns)
        print(f"Columns of table {table!r} reordered.")


@cli.command(name="search")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
@click.argument("query")
@click.option("-w", "--where", default="")
@click.option("--columns", multiple=True, default=[], help=COLUMNS_HELP)
@click.option("--hide", multiple=True, default=[], help=HIDE_HELP)
@click.option("-p", "--page", default=1, help=PAGE_HELP)
@click.option("--limit", default=None, help=LIMIT_HELP)
@click.option("--offset", default=None, help=OFFSET_HELP)
@click.option("--order-by", multiple=True, default=[], help=ORDER_BY_HELP)
@click.option("--desc", is_flag=True, default=False, help=DESC_HELP)
def main_search(
    db_path,
    schema_path,
    table,
    query,
    *,
    where,
    columns,
    hide,
    page,
    limit,
    offset,
    order_by,
    desc,
):
    """
    Shorthand for `list <table> -s <query>`
    """
    base_list(
        db_path,
        table,
        where=where,
        search=query,
        columns=columns,
        hide=hide,
        page=page,
        limit=limit,
        offset=offset,
        order_by=order_by,
        desc=desc,
    )


@cli.command(name="sql")
@click.option("--db", "db_path")
@click.argument("query")
@click.option("--columns", multiple=True, default=[], help=COLUMNS_HELP)
@click.option("--hide", multiple=True, default=[], help=HIDE_HELP)
@click.option("-p", "--page", default=1, help=PAGE_HELP)
@click.option(
    "--write",
    is_flag=True,
    default=False,
    help="Allow writing to the database. False by default.",
)
def main_sql(db_path, query, *, columns, hide, page, write):
    """
    Run a SQL command.
    """
    readonly = not write
    with Database(db_path, readonly=readonly) as db:
        rows = db.sql(query)
        if rows:
            prettyprint_rows(rows, columns=columns, hide=hide, page=page)
        else:
            print("No rows found.")


@cli.command(name="update")
@click.option("--db", "db_path")
@click.option("--schema", "schema_path")
@click.argument("table")
@click.argument("pk", type=int)
def main_update(db_path, schema_path, table, pk):
    """
    Update an existing row interactively.
    """
    full_schema = get_schema_dict(schema_path)
    with Database(db_path) as db:
        schema = full_schema.get(table)
        if schema is None:
            print(f"Table {table!r} not found.")
            sys.exit(1)

        row = db.get_by_rowid(table, pk)
        if row is None:
            print(f"Row {pk} not found in table {table!r}.")
            sys.exit(1)

        print("To keep a column's current value, enter a blank line.")
        print("To set a column to null, enter NULL in all caps.")
        print()

        updates = {}
        for column in schema.columns.values():
            if column.name in ("id", "created_at", "last_updated_at"):
                continue

            while True:
                currently = (
                    "null"
                    if row[column.name] is None or row[column.name] == ""
                    else repr(row[column.name])
                )
                raw_value = input(
                    f"{column.description()} (currently: {currently})? "
                ).strip()
                if raw_value == "":
                    break
                elif raw_value == "NULL":
                    raw_value = ""

                value, is_valid = column.validate(raw_value)
                if is_valid:
                    updates[column.name] = value
                    break

        db.update_by_rowid(table, pk, updates)
        print()
        print(f"Row {pk} updated.")


def prettyprint_rows(rows, *, columns=[], hide=[], page=1):
    headers = [key for key in rows[0].keys() if should_show_column(key, columns, hide)]
    table_rows = [
        [cell for key, cell in row.items() if should_show_column(key, columns, hide)]
        for row in rows
    ]
    table = tabulate(table_rows, headers=headers)

    overflow = False
    width = shutil.get_terminal_size().columns
    placeholder = " ..."
    for line in table.splitlines():
        if len(line) > width:
            start = width * (page - 1)
            end = start + width - len(placeholder)
            if end < len(line):
                print(line[start:end] + placeholder)
                overflow = True
            else:
                print(line[start:end])
        else:
            print(line)

    if table_rows:
        print()
        print(f"{len(table_rows)} row(s).")

    if overflow or page > 1:
        print()
        if overflow:
            print("Some columns truncated or hidden due to overflow.")
        if page > 1:
            print(
                f"To see the previous page of columns, re-run with --page={page - 1}."
            )
        if overflow:
            print(f"To see the next page of columns, re-run with --page={page + 1}.")


def prettyprint_row(row):
    table = list(row.items())
    print(tabulate(table))


def should_show_column(key, columns, hide):
    if columns:
        return key in columns

    if hide:
        return key not in hide

    return True


def get_column_as_string(schema, base_table, column_name, column_value):
    base_table_def = schema[base_table]
    table = schema[base_table_def.columns[column_name].model]
    return table.as_string(column_value)


def get_schema_module(schema_path):
    if schema_path is None:
        return None

    spec = importlib.util.spec_from_file_location("schema", schema_path)
    schema_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_module)
    return schema_module


def get_schema_dict(schema_path):
    return schema_module_to_dict(get_schema_module(schema_path))
