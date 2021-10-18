"""
The implementation of the `isqlite` command-line tool.
"""
import collections
import importlib
import shutil
import sqlite3
import sys
import tempfile
import traceback

import click
import sqliteparser
from tabulate import tabulate

from . import Database, migrations

# Help strings used in multiple places.
HELP_COLUMNS = "Only display these columns in the results."
HELP_DESC = (
    "When combined with --order-by, order the results in descending rather than "
    + "ascending order."
)
HELP_HIDE = "Hide these columns in the results."
HELP_LIMIT = "Limit the number of rows returned from the database."
HELP_NO_CONFIRM = "Do not prompt for confirmation."
HELP_OFFSET = "Offset a query with --limit."
HELP_ORDER_BY = "Order the results by one or more columns."
HELP_PAGE = (
    "Select the page of results to show, "
    + "if the table is too wide to display in one screen."
)
HELP_PLAIN_FOREIGN_KEYS = (
    "By default, isqlite will pretty-print foreign key columns with the first "
    + "TEXT column of the foreign key table. Pass this flag if you would rather just "
    + "see the foreign key values themselves."
)


@click.group()
def cli():
    pass


@cli.command(name="add-column")
@click.argument("db_path")
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
@click.argument("db_path")
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


@cli.command(name="count")
@click.argument("db_path")
@click.argument("table")
@click.option("-w", "--where", default="")
@click.option(
    "--distinct",
    default=None,
    help="Only count rows with distinct values of this column.",
)
def main_count(db_path, table, *, where, distinct):
    """
    Count the number of rows that match the criteria.
    """
    with Database(db_path) as db:
        print(db.count(table, where=where, distinct=distinct))


@cli.command(name="create-table")
@click.argument("db_path")
@click.argument("table")
@click.argument("columns", nargs=-1)
def main_create_table(db_path, table, columns):
    """
    Create a table.
    """
    with Database(db_path) as db:
        db.create_table(table, columns)
        print(f"Table {table!r} created.")


@cli.command(name="delete")
@click.argument("db_path")
@click.argument("table")
@click.argument("pk", type=int, required=False, default=None)
@click.option("-w", "--where", default="")
@click.option(
    "--no-confirm",
    is_flag=True,
    default=False,
    help=HELP_NO_CONFIRM,
)
def main_delete(db_path, table, pk, *, where, no_confirm):
    """
    Delete a row.
    """
    with Database(db_path) as db:
        if pk is not None:
            try:
                row = db.get_by_pk(table, pk)
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

            if not no_confirm:
                print()
                if not click.confirm("Are you sure you wish to delete this record?"):
                    print()
                    print("Operation aborted.")
                    sys.exit(1)

            db.delete_by_pk(table, pk)
            print()
            print(f"Row {pk} deleted from table {table!r}.")
        else:
            if not no_confirm:
                n = db.count(table, where=where)
                if not where:
                    msg = f"Are you sure you wish to delete ALL {pluralize(n, 'row')} from {table!r}?"
                else:
                    msg = f"Are you sure you wish to delete {pluralize(n, 'row')} from {table!r}?"

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
            print(f"{pluralize(n, 'row')} from table {table!r} deleted.")


@cli.command(name="diff")
@click.argument("db_path")
@click.argument("schema_path")
@click.option("--table", default=None)
def main_diff(db_path, schema_path, table):
    """
    Diff the database against the Python schema.
    """
    schema = get_schema_from_path(schema_path)
    with Database(db_path, readonly=True) as db:
        _diff(db, schema, table)


def _diff(db, schema, table):
    diff = db.diff(schema, table=table)
    if not diff:
        print("Nothing to migrate: database matches schema.")
        return None

    tables_created = 0
    tables_dropped = 0
    columns_dropped = 0
    printed = False
    grouped_diff = group_diff_by_table(diff)
    for table, table_diff in sorted(grouped_diff.items(), key=lambda kv: kv[0]):
        if printed:
            print()
        else:
            printed = True

        print(f"Table {blue(table)}")
        for op in table_diff:
            if isinstance(op, migrations.DropColumnMigration):
                columns_dropped += 1
            elif isinstance(op, migrations.DropTableMigration):
                tables_dropped += 1
            elif isinstance(op, migrations.CreateTableMigration):
                tables_created += 1

            print(f"- {op}")

    print()
    print()
    print("Summary")
    print(f"- {blue(pluralize(len(grouped_diff), 'table'))} affected.")
    print(f"- {blue(pluralize(len(diff), 'operation'))} in total.")
    if tables_created > 0:
        print(f"- {blue(pluralize(tables_created, 'table'))} created.")
    if tables_dropped > 0:
        print(f"- {red(pluralize(tables_dropped, 'table'))} dropped.")
    if columns_dropped > 0:
        print(f"- {red(pluralize(columns_dropped, 'column'))} dropped.")

    return (diff, tables_dropped or columns_dropped)


@cli.command(name="drop-column")
@click.argument("db_path")
@click.argument("table")
@click.argument("column")
@click.option(
    "--no-confirm",
    is_flag=True,
    default=False,
    help=HELP_NO_CONFIRM,
)
def main_drop_column(db_path, table, column, *, no_confirm):
    """
    Drop a column from a table.
    """
    if not no_confirm:
        with Database(db_path) as db:
            count = db.count(table)

            print(
                f"WARNING: Table {table!r} contains {pluralize(count, 'row')} of data."
            )
            print()
            if not click.confirm("Are you sure you wish to drop this column?"):
                print()
                print("Operation aborted.")
                sys.exit(1)

    with Database(db_path) as db:
        db.drop_column(table, column)
        print()
        print(f"Column {column!r} dropped from table {table!r}.")


@cli.command(name="drop-table")
@click.argument("db_path")
@click.argument("table")
@click.option(
    "--no-confirm",
    is_flag=True,
    default=False,
    help=HELP_NO_CONFIRM,
)
def main_drop_table(db_path, table, *, no_confirm):
    """
    Drop a table from the database.
    """
    with Database(db_path) as db:
        if not no_confirm:
            count = db.count(table)
            print(
                f"WARNING: Table {table!r} contains {pluralize(count, 'row')} of data."
            )
            print()
            if not click.confirm("Are you sure you wish to drop this table?"):
                print()
                print("Operation aborted.")
                sys.exit(1)

        db.drop_table(table)
        print()
        print(f"Table {table!r} dropped from the database.")


@cli.command(name="get")
@click.argument("db_path")
@click.argument("table")
@click.argument("pk", type=int)
@click.option(
    "--plain-foreign-keys", is_flag=True, default=False, help=HELP_PLAIN_FOREIGN_KEYS
)
def main_get(db_path, table, pk, *, plain_foreign_keys):
    """
    Fetch a single row.
    """
    with Database(db_path, readonly=True) as db:
        row = db.get_by_pk(table, pk, get_related=not plain_foreign_keys)
        if not plain_foreign_keys:
            for key, value in row.items():
                if isinstance(value, collections.OrderedDict):
                    row[key] = get_column_as_string(value)

        if row is None:
            print(f"Row {pk} not found in table {table!r}.")
        else:
            prettyprint_row(row)


@cli.command(name="insert")
@click.argument("db_path")
@click.argument("table")
@click.argument("payload", nargs=-1)
@click.option(
    "--auto-timestamp/--no-auto-timestamp",
    default=True,
    help=(
        "Automatically populate `created_at` and `last_updated_at` columns with "
        + "current time."
    ),
)
def main_insert(db_path, table, payload, *, auto_timestamp=True):
    """
    Create a new row non-interactively.

    PAYLOAD should be a list of space-separated key-value pairs, e.g.

        isqlite insert --db db.sqlite3 my_table a=1 b=2
    """
    if not payload:
        report_error_and_exit("payload must not be empty")

    payload_as_map = {}
    for key_value in payload:
        key, value = key_value.split("=")
        payload_as_map[key] = value

    if auto_timestamp:
        auto_timestamp_columns = ["created_at", "last_updated_at"]
    else:
        auto_timestamp_columns = []

    with Database(db_path) as db:
        pk = db.insert(
            table, payload_as_map, auto_timestamp_columns=auto_timestamp_columns
        )
        print(f"Row {pk} created.")


@cli.command(name="migrate")
@click.argument("db_path")
@click.argument("schema_path")
@click.option("--table", default=None)
@click.option(
    "--no-confirm",
    is_flag=True,
    default=False,
    help="Perform the migration without requiring human confirmation.",
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
def main_migrate(db_path, schema_path, table, *, no_confirm, no_backup, debug):
    """
    Migrate the database to match the Python schema.
    """
    schema = get_schema_from_path(schema_path)
    with Database(db_path, transaction=False, debug=debug) as db:
        diff, data_dropped = _diff(db, schema, table)
        if diff is None:
            return

        if not no_confirm:
            prompt = "Do you wish to perform the migration?"
            if data_dropped:
                prompt += red(" Some data may be lost.")

            print()
            if not click.confirm(prompt):
                print()
                print("Migration aborted.")
                sys.exit(2)

        if not no_backup:
            _, backup_name = tempfile.mkstemp(
                prefix="isqlite-backup-", suffix=".sqlite3"
            )
            shutil.copy2(db_path, backup_name)

        try:
            db.apply_diff(diff)
        except Exception:
            traceback.print_exc()

            print()
            print("Migration rolled back due to Python exception.")
            sys.exit(2)

        if no_backup:
            print("Migration completed successfully.")
        else:
            print(
                "Migration completed successfully. "
                + f"Backup of database before migration saved at {backup_name}."
            )


@cli.command(name="rename-column")
@click.argument("db_path")
@click.argument("table")
@click.argument("old_name")
@click.argument("new_name")
def main_rename_column(db_path, table, old_name, new_name):
    """
    Rename a column.
    """
    with Database(db_path) as db:
        db.rename_column(table, old_name, new_name)
        print(f"Column {old_name!r} renamed to {new_name!r} in table {table!r}.")


@cli.command(name="rename-table")
@click.argument("db_path")
@click.argument("table")
@click.argument("new_name")
def main_rename_table(db_path, table, new_name):
    """
    Rename a table.
    """
    with Database(db_path) as db:
        db.rename_table(table, new_name)
        print(f"Table {table!r} renamed to {new_name!r}.")


@cli.command(name="reorder-columns")
@click.argument("db_path")
@click.argument("table")
@click.argument("columns", nargs=-1)
def main_reorder_columns(db_path, table, columns):
    """
    Change the order of columns in a table.
    """
    with Database(db_path) as db:
        db.reorder_columns(table, columns)
        print(f"Columns of table {table!r} reordered.")


@cli.command(name="schema")
@click.argument("db_path")
@click.argument("table", required=False, default=None)
@click.option("--as-python", is_flag=True, default=False)
def main_schema(db_path, table, *, as_python):
    """
    List the names of the tables in the database.
    """
    with Database(db_path, readonly=True) as db:
        if table is not None:
            row = db.get(
                "sqlite_master",
                where="type = 'table' AND name = :table",
                values={"table": table},
            )
            sql = row["sql"]

            if as_python:
                raise NotImplementedError
            else:
                try:
                    # Use sqliteparser to parse and pretty-print the table schema.
                    create_table_statement = sqliteparser.parse(sql)[0]
                    print(create_table_statement)
                except sqliteparser.SQLiteParserError:
                    # If sqliteparser can't parse the table schema, just directly print
                    # what SQLite returned to us.
                    print(sql)
        else:
            if as_python:
                raise NotImplementedError
            else:
                rows = db.select(
                    "sqlite_master",
                    where="type = 'table' AND name NOT LIKE 'sqlite_%'",
                    order_by="name",
                )

            if rows:
                print("\n".join(row["name"] for row in rows))


@cli.command(name="search")
@click.argument("db_path")
@click.argument("table")
@click.argument("query")
@click.option("-w", "--where", default="")
@click.option("--columns", multiple=True, default=[], help=HELP_COLUMNS)
@click.option("--hide", multiple=True, default=[], help=HELP_HIDE)
@click.option("-p", "--page", default=1, help=HELP_PAGE)
@click.option("--limit", default=None, help=HELP_LIMIT)
@click.option("--offset", default=None, help=HELP_OFFSET)
@click.option("--order-by", multiple=True, default=[], help=HELP_ORDER_BY)
@click.option("--desc", is_flag=True, default=False, help=HELP_DESC)
@click.option(
    "--plain-foreign-keys", is_flag=True, default=False, help=HELP_PLAIN_FOREIGN_KEYS
)
def main_search(
    db_path,
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
    plain_foreign_keys,
):
    """
    Shorthand for `select <table> -s <query>`
    """
    base_select(
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
        plain_foreign_keys=plain_foreign_keys,
    )


@cli.command(name="select")
@click.argument("db_path")
@click.argument("table")
@click.option("-w", "--where", default="")
@click.option("-s", "--search")
@click.option("--columns", multiple=True, default=[], help=HELP_COLUMNS)
@click.option("--hide", multiple=True, default=[], help=HELP_HIDE)
@click.option("-p", "--page", default=1, help=HELP_PAGE)
@click.option("--limit", default=None, help=HELP_LIMIT)
@click.option("--offset", default=None, help=HELP_OFFSET)
@click.option("--order-by", multiple=True, default=[], help=HELP_ORDER_BY)
@click.option("--desc", is_flag=True, default=False, help=HELP_DESC)
@click.option(
    "--plain-foreign-keys", is_flag=True, default=False, help=HELP_PLAIN_FOREIGN_KEYS
)
def main_select(
    db_path,
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
    plain_foreign_keys,
):
    """
    List the rows in the table, optionally filtered by a SQL clause.
    """
    base_select(
        db_path,
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
        plain_foreign_keys=plain_foreign_keys,
    )


def base_select(
    db_path,
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
    plain_foreign_keys,
):
    """
    Base implementation shared by `main_search` and `main_select`.
    """
    with Database(db_path, readonly=True) as db:
        try:
            rows = db.select(
                table,
                where=where,
                order_by=order_by,
                limit=limit,
                offset=offset,
                descending=desc if order_by else None,
                get_related=not plain_foreign_keys,
            )
        except sqlite3.OperationalError:
            # Because `get_related` uses SQL joins, it may cause 'ambiguous column'
            # errors if the user-supplied WHERE clause has unqualified column names. So
            # we simply retry on error with `get_related=False`.
            rows = db.select(
                table,
                where=where,
                order_by=order_by,
                limit=limit,
                offset=offset,
                descending=desc if order_by else None,
                get_related=False,
            )

        if not plain_foreign_keys:
            for row in rows:
                for key, value in row.items():
                    if isinstance(value, collections.OrderedDict):
                        row[key] = get_column_as_string(value)

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


@cli.command(name="sql")
@click.argument("db_path")
@click.argument("query")
@click.option("--columns", multiple=True, default=[], help=HELP_COLUMNS)
@click.option("--hide", multiple=True, default=[], help=HELP_HIDE)
@click.option("-p", "--page", default=1, help=HELP_PAGE)
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
@click.argument("db_path")
@click.argument("table")
@click.argument("pk", type=int)
@click.argument("payload", nargs=-1)
@click.option(
    "--auto-timestamp/--no-auto-timestamp",
    default=True,
    help="Automatically populate `last_updated_at` column with current time.",
)
def main_update(db_path, table, pk, payload, *, auto_timestamp):
    """
    Update an existing row non-interactively.

    PAYLOAD should be a list of space-separated key-value pairs, e.g.

        isqlite update --db db.sqlite3 my_table 123 a=1 b=2
    """
    if not payload:
        report_error_and_exit("payload must not be empty")

    payload_as_map = {}
    for key_value in payload:
        key, value = key_value.split("=")
        payload_as_map[key] = value

    if auto_timestamp:
        auto_timestamp_columns = ["created_at", "last_updated_at"]
    else:
        auto_timestamp_columns = []

    with Database(db_path) as db:
        db.update_by_pk(
            table, pk, payload_as_map, auto_timestamp_columns=auto_timestamp_columns
        )
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
        print(f"{pluralize(len(table_rows), 'row')}.")

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


def get_column_as_string(column):
    pk = None
    text = None

    # Pretty rudimentary logic: assume the first integer value is the row's primary key
    # and the first string value is a reasonable choice for displaying the row.
    for value in column.values():
        if isinstance(value, int) and pk is None:
            pk = value
        elif isinstance(value, str) and text is None:
            text = value

        if pk is not None and text is not None:
            break

    if pk is not None:
        if text is not None:
            return f"{pk} ({text})"
        else:
            return str(pk)
    else:
        if text is not None:
            return text
        else:
            return "<foreign row>"


def get_schema_from_path(schema_path):
    if schema_path is None:
        return None

    spec = importlib.util.spec_from_file_location("schema", schema_path)
    if spec is None:
        report_error_and_exit(f"could not load schema file at {schema_path}")

    schema_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schema_module)
    return schema_module.SCHEMA


def group_diff_by_table(diff):
    diff_map = collections.defaultdict(list)
    for op in diff:
        diff_map[op.table_name].append(op)
    return diff_map


def red(s):
    return click.style(s, fg="red")


def blue(s):
    return click.style(s, fg="blue")


def pluralize(n, word):
    if n == 1:
        return f"{n} {word}"
    else:
        return f"{n} {word}s"


def report_error_and_exit(message):
    print(f"Error: {message}", file=sys.stderr)
    sys.exit(1)
