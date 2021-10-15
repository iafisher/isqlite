import collections
import sqlite3
import textwrap

import sqliteparser
from sqliteparser import quote

from . import migrations
from .columns import primary_key as primary_key_column
from .columns import timestamp as timestamp_column

CURRENT_TIMESTAMP_SQL = "STRFTIME('%Y-%m-%d %H:%M:%f', 'now')"
AUTO_TIMESTAMP_DEFAULT = ("created_at", "last_updated_at")
AUTO_TIMESTAMP_UPDATE_DEFAULT = ("last_updated_at",)


# Sentinel object to detect keyword arguments that were not specified by the caller.
_Unset = object()


class Database:
    """
    A class to represent a connection to a SQLite database. Typically used as a context
    manager::

        with Database("db.sqlite3") as db:
            ...

    On creation, the ``Database`` connection will open a SQL transaction which will be
    either committed or rolled back by the context manager, depending on whether an
    exception occurs.

    You can also use multiple transactions over the life of the connection::

        with Database("db.sqlite3", transaction=False) as db:
            with db.transaction():
                ...

            with db.transaction():
                ...
    """

    def __init__(
        self,
        path,
        *,
        transaction=True,
        debugger=None,
        readonly=None,
        uri=False,
        cached_statements=_Unset,
        create_auto_timestamp_columns=[],
        update_auto_timestamp_columns=[],
    ):
        """
        Initialize a ``Database`` object.

        :param path: The path to the database file. You may pass ``":memory"`` for an
            in-memory database.
        :param transaction: If true, a transaction is automatically opened with BEGIN.
            When the ``Database`` class is used in a ``with`` statement, the transaction
            will be committed at the end (or rolled back if an exception occurs), so
            either all of the changes in the ``with`` block will be enacted, or none of
            them.

            If false, the database will operate in autocommit mode by default, meaning
            that every statement will be committed immediately. Users can then manage
            their transactions explicitly with ``Database.transaction``, e.g.::

                with Database(transaction=False) as db:
                    with db.transaction():
                        ...

                    with db.transaction():
                        ...

        :param debugger: If true, each SQL statement executed will be printed to
            standard output. You can also pass an object of a class which defines
            ``execute(sql, values)`` and ``executemany(sql, values)`` methods; these
            methods will be invoked each time the corresponding method is invoked on the
            underlying SQLite connection, and the debugger class can use this
            information for debugging, profiling, or whatever else.
        :param readonly: If true, the database will be opened in read-only mode. This
            option is incompatibility with ``uri=True``; if you need to pass a URI, then
            append ``?mode=ro`` to make it read-only.
        :param uri: If true, the ``path`` argument is interpreted as a URI rather than a
            file path.
        :param cached_statements: Passed on to ``sqlite3.connect``.
        :param create_auto_timestamp_columns: A default value for
            ``auto_timestamp_columns`` in ``create`` and ``create_many``. Usually set to
            ``["created_at", "last_updated_at"]`` in conjunction with a schema defined
            using ``AutoTable``.
        :param update_auto_timestamp_columns: A default value for
            ``auto_timestamp_columns`` in ``update``. Usually set to
            ``["last_updated_at"]`` in conjunction with a schema defined using
            ``AutoTable``.
        """
        # Validate arguments.
        if readonly is not None:
            if uri is True:
                raise ISqliteApiError(
                    "The `readonly` parameter cannot be set if `uri` is False. Append "
                    + "'?mode=ro' (or omit it if you don't want your connection to be "
                    + "read-only) to your URI instead."
                )
        else:
            # Default value of `readonly` if not specified is False.
            readonly = False

        if not uri:
            if readonly is True:
                path = f"file:{path}?mode=ro"
            else:
                path = f"file:{path}"

        self.create_auto_timestamp_columns = create_auto_timestamp_columns
        self.update_auto_timestamp_columns = update_auto_timestamp_columns

        # We have to do this instead of passing the keyword arguments directly to
        # `sqlite3.connect` because we conditionally pass `cached_statements` if the
        # caller of `Database.__init__` gave it an explicit value.
        #
        # We _could_ use the current `sqlite3` default of 100 as the default value of
        # the argument, but then if `sqlite3` ever changed its default, `isqlite` would
        # have to be updated.
        kwargs = dict(
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            uri=True,
            isolation_level=None,
        )
        if cached_statements is not _Unset:
            kwargs["cached_statements"] = cached_statements

        # Setting `isolation_level` to None disables quirky behavior around
        # transactions, per https://stackoverflow.com/questions/30760997/
        self.connection = sqlite3.connect(path, **kwargs)

        if debugger is True:
            debugger = PrintDebugger()
        self.debugger = debugger

        self.connection.row_factory = ordered_dict_row_factory
        self.cursor = self.connection.cursor()

        # This must be executed outside a transaction, according to the official
        # SQLite docs: https://sqlite.org/pragma.html#pragma_foreign_keys
        self.sql("PRAGMA foreign_keys = 1")
        if transaction:
            self.sql("BEGIN")

        self.refresh_schema()

    def list(
        self,
        table,
        *,
        where=None,
        values={},
        limit=None,
        offset=None,
        order_by=None,
        descending=None,
        get_related=[],
    ):
        """
        Return a list of database rows as ``OrderedDict`` objects.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param where: A 'where' clause to restrict the query, as a string. The initial
            ``WHERE`` keyword should be omitted. To interpolate Python values, put,
            e.g., ``:placeholder`` in the SQL and then pass ``{"placeholder": x}`` as
            the ``values`` parameter.
        :param values: A dictionary of values to interpolate into the ``where``
            argument.
        :param limit: An integer limit to the number of rows returned.
        :param offset: If not None, return results starting from this offset. Can be
            used in conjunction with ``limit`` for pagination.
        :param order_by: If not None, order the results by this column. Order is
            ascending (smallest values first) by default; for descending order, pass
            ``descending=True``.
        :param descending: If true, return results in descending order instead of
            ascending.
        :param get_related: A list of foreign-key columns of the table to be retrieved
            and embedded into the returned dictionaries. If true, all foreign-key
            columns will be retrieved. This parameter requires that ``Database`` was
            initialized with a ``schema`` parameter.
        """
        if order_by:
            if isinstance(order_by, (tuple, list)):
                order_by = ", ".join(order_by)

            direction = "DESC" if descending is True else "ASC"
            order_clause = f"ORDER BY {quote(order_by)} {direction}"
        else:
            if descending is not None:
                raise ISqliteApiError(
                    "The `descending` parameter to `list` requires the `order_by` "
                    + "parameter to be set."
                )
            order_clause = ""

        if limit is not None:
            if offset is not None:
                limit_clause = f"LIMIT {limit} OFFSET {offset}"
            else:
                limit_clause = f"LIMIT {limit}"
        else:
            if offset is not None:
                raise ISqliteApiError(
                    "The `offset` parameter to `list` requires the `limit` parameter "
                    + "to be set."
                )

            limit_clause = ""

        where_clause = f"WHERE {where}" if where else ""

        if get_related:
            columns, joins = self._get_related_columns_and_joins(table, get_related)
            rows = self.sql(
                f"SELECT {columns} FROM {quote(table)} {joins} {where_clause}"
                + f" {order_clause} {limit_clause}",
                values,
            )
        else:
            rows = self.sql(
                f"SELECT * FROM {quote(table)} {where_clause} {order_clause}"
                + f" {limit_clause}",
                values,
            )

        return rows

    def get(self, table, *, where=None, values={}, get_related=[]):
        """
        Retrieve a single row from the database table and return it as an ``OrderedDict``
        object.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param where: Same as for ``Database.list``.
        :param values: Same as for ``Database.list``.
        :param get_related: Same as for ``Database.list``.
        """
        where_clause = f"WHERE {where}" if where else ""

        if get_related:
            columns, joins = self._get_related_columns_and_joins(table, get_related)
            row = self.sql(
                f"SELECT {columns} FROM {quote(table)} {joins} {where_clause}",
                values,
                multiple=False,
            )
        else:
            row = self.sql(
                f"SELECT * FROM {quote(table)} {where_clause}",
                values,
                multiple=False,
            )

        return row

    def get_by_pk(self, table, pk, **kwargs):
        """
        Retrieve a single row from the database table by its primary key.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param pk: The primary key of the row to return.
        :param kwargs: Additional arguments to pass on to ``Database.get``.
        """
        pk_column = f"{quote(table)}.rowid"
        return self.get(table, where=f"{pk_column} = :pk", values={"pk": pk}, **kwargs)

    def get_or_create(self, table, data, **kwargs):
        """
        Retrieve a single row from the database table matching the parameters in
        ``data``. If no such row exists, create it and return it.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param data: The parameters to match the database row. All required columns of
            the table must be included, or else the internal call to ``Database.create``
            will fail.
        :param kwargs: Additional arguments to pass on to ``Database.create``. If the
            database row already exists, these arguments are ignored.
        """
        if not data:
            raise ISqliteError(
                "The `data` parameter to `get_or_create` cannot be empty."
            )

        query = " AND ".join(f"{key} = :{key}" for key in data)
        row = self.get(table, where=query, values=data)
        if row is None:
            pk = self.create(table, data, **kwargs)
            return self.get_by_pk(table, pk)
        else:
            return row

    def count(self, table, *, where=None, values={}, distinct=None):
        """
        Return the count of rows matching the parameters.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param where: Same as for ``Database.list``.
        :param values: Same as for ``Database.list``.
        :param distinct: Only count rows with distinct values of this column.
        """
        where_clause = f"WHERE {where}" if where else ""
        count_expression = (
            "COUNT(*)" if distinct is None else f"COUNT(DISTINCT {distinct})"
        )
        result = self.sql(
            f"SELECT {count_expression} FROM {quote(table)} {where_clause}",
            values,
            as_tuple=True,
            multiple=False,
        )
        return result[0]

    def create(self, table, data, *, auto_timestamp_columns=_Unset):
        """
        Insert a new row.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param data: The row to insert, as a dictionary from column names to column
            values.
        :param auto_timestamp_columns: A list of columns into which to insert the
            current date and time, as an ISO 8601 timestamp. Defaults to the value of
            ``create_auto_timestamp_columns`` passed to ``__init__`` if unset.
        """
        if auto_timestamp_columns is _Unset:
            auto_timestamp_columns = self.create_auto_timestamp_columns

        keys = list(data.keys())
        placeholders = ",".join("?" for _ in range(len(keys)))
        values = list(data.values())

        extra = []
        for column in auto_timestamp_columns:
            keys.append(column)
            extra.append(CURRENT_TIMESTAMP_SQL)

        if extra:
            extra = (", " if data else "") + ", ".join(extra)
        else:
            extra = ""

        sql = f"""
        INSERT INTO {quote(table)}({', '.join(map(quote, keys))})
        VALUES ({placeholders}{extra});
        """
        if self.debugger:
            self.debugger.execute(sql, values)
        self.cursor.execute(sql, values)
        return self.cursor.lastrowid

    def create_many(self, table, data, *, auto_timestamp_columns=[]):
        """
        Insert multiple rows at once.

        Equivalent to::

            for row in data:
                db.create(table, row)

        but more efficient.
        """
        if not data:
            return

        if auto_timestamp_columns is _Unset:
            auto_timestamp_columns = self.create_auto_timestamp_columns

        keys = list(data[0].keys())
        placeholders = ",".join("?" for _ in range(len(keys)))

        extra = []
        for column in auto_timestamp_columns:
            keys.append(column)
            extra.append(CURRENT_TIMESTAMP_SQL)

        if extra:
            extra = (", " if data else "") + ", ".join(extra)
        else:
            extra = ""

        sql = f"""
        INSERT INTO {quote(table)}({', '.join(map(quote, keys))})
        VALUES ({placeholders}{extra});
        """
        values = [tuple(d.values()) for d in data]
        if self.debugger:
            self.debugger.executemany(sql, values)
        self.cursor.executemany(sql, values)

    def update(
        self,
        table,
        data,
        *,
        where=None,
        values={},
        auto_timestamp_columns=_Unset,
    ):
        """
        Update an existing row.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param data: The columns to update, as a dictionary from column names to column
            values.
        :param where: Restrict the set of rows to update. Same as for ``Database.list``.
        :param values: Same as for ``Database.list``.
        :param auto_timestamp_columns: Same as for ``Database.create``, except that if
            the same column appears in both ``values`` and ``auto_timestamp_columns``,
            the timestamp will be inserted instead of the value. Defaults to the value
            of ``update_auto_timestamp_columns`` passed to ``__init__`` if unset.
        """
        if auto_timestamp_columns is _Unset:
            auto_timestamp_columns = self.update_auto_timestamp_columns

        updates = []
        for key, value in data.items():
            if key in auto_timestamp_columns:
                continue

            placeholder = f"v{len(values)}"
            values[placeholder] = value
            updates.append(f"{quote(key)} = :{placeholder}")

        for column in auto_timestamp_columns:
            updates.append(f"{quote(column)} = {CURRENT_TIMESTAMP_SQL}")

        if not updates:
            raise ISqliteError(
                "updates cannot be empty - either `data` or `auto_timestamp_columns` "
                + "must be set"
            )

        updates = ", ".join(updates)
        where_clause = f"WHERE {where}" if where else ""
        sql = f"UPDATE {quote(table)} SET {updates} {where_clause}"
        if self.debugger:
            self.debugger.execute(sql, values)
        self.cursor.execute(sql, values)

    def update_by_pk(self, table, pk, data, **kwargs):
        """
        Update a single row.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param pk: The primary key of the row to update.
        :param data: Same as for ``Database.update``.
        :param kwargs: Additional arguments to pass on to ``Database.update``.
        """
        pk_column = f"{quote(table)}.rowid"
        return self.update(
            table,
            data,
            where=f"{pk_column} = :pk",
            values={"pk": pk},
            **kwargs,
        )

    def delete(self, table, *, where, values={}):
        """
        Delete a set of rows.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param where: Same as for ``Database.list``, except that it is required, to
            avoid accidentally deleting every row in a table. If you indeed wish to
            delete every row, then pass ``where="1"``.
        :param values: Same as for ``Database.list``.
        """
        self.sql(f"DELETE FROM {quote(table)} WHERE {where}", values=values)

    def delete_by_pk(self, table, pk, **kwargs):
        """
        Delete a single row.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param pk: The primary key of the row to delete.
        :param kwargs: Additional arguments to pass on to ``Database.delete``.
        """
        pk_column = f"{quote(table)}.rowid"
        return self.delete(
            table, where=f"{pk_column} = :pk", values={"pk": pk}, **kwargs
        )

    def sql(self, query, values={}, *, as_tuple=False, multiple=True):
        """
        Execute a raw SQL query.

        :param query: The SQL query, as a string.
        :param values: A dictionary of values to interpolate into the query.
        :param as_tuple: If true, the rows are returned as tuples of values instead of
            ``OrderedDict`` objects. This is useful for aggregation queries, e.g.
            ``COUNT(*)``.
        :param multiple: If true, the return type will be a list (though the list may
            be empty or only contain a single row). If false, the return type will
            either be a tuple (if ``as_tuple=True``) or an ``OrderedDict`` object.
        """
        if multiple:
            if self.debugger:
                self.debugger.execute(query, values)
            self.cursor.execute(query, values)
            rows = self.cursor.fetchall()
            if as_tuple:
                return [tuple(row.values()) for row in rows]

            return rows
        else:
            query = query + " LIMIT 1"
            if self.debugger:
                self.debugger.execute(query, values)
            self.cursor.execute(query, values)
            row = self.cursor.fetchone()
            if row is None:
                return row

            if as_tuple:
                return tuple(row.values())

            return row

    def create_table(self, table_name, columns):
        """
        Create a new table.

        :param table_name: The name of the table to create.
        :param columns: A list of columns, as raw SQL strings.
        """
        if isinstance(columns, str):
            raise ISqliteApiError(
                "second argument to DatabaseMigrator.create_table must be a list, "
                + "not a string"
            )

        self.sql(f"CREATE TABLE {quote(table_name)}({','.join(map(str, columns))})")
        self.refresh_schema()

    def drop_table(self, table_name):
        """
        Drop a table.

        :param table_name: The name of the table to drop.
        """
        self.sql(f"DROP TABLE {quote(table_name)}")
        self.refresh_schema()

    def rename_table(self, old_table_name, new_table_name):
        """
        Rename a table.
        """
        self.sql(
            f"ALTER TABLE {quote(old_table_name)} RENAME TO {quote(new_table_name)}"
        )
        self.refresh_schema()

    def add_column(self, table_name, column_def):
        """
        Add a column to the table's schema.

        :param table_name: The name of the table.
        :param column_def: The definition of the column to add, as raw SQL.
        """
        self.sql(f"ALTER TABLE {quote(table_name)} ADD COLUMN {column_def}")
        self.refresh_schema()

    def drop_column(self, table_name, column_name):
        """
        Drop a column from the database.
        """
        # ALTER TABLE ... DROP COLUMN is only supported since SQLite version 3.35, so we
        # implement it by hand here.
        create_table_statement = self._get_create_table_statement(table_name)
        columns = [
            str(column)
            for column in create_table_statement.columns
            if column.name != column_name
        ]
        if len(columns) == len(create_table_statement.columns):
            raise ColumnDoesNotExistError(table_name, column_name)

        select = ", ".join(
            quote(c.name)
            for c in create_table_statement.columns
            if c.name != column_name
        )
        self._migrate_table(table_name, columns, select=select)
        self.refresh_schema()

    def reorder_columns(self, table_name, column_names):
        """
        Reorder the columns of a database table.

        :param table_name: The table to reorder.
        :param column_names: The new order of the columns, as a list of strings. The
            column names must be the same as in the database; otherwise, an exception
            will be raised.
        """
        create_table_statement = self._get_create_table_statement(table_name)
        column_map = collections.OrderedDict(
            (c.name, c) for c in create_table_statement.columns
        )

        if set(column_names) != set(column_map.keys()):
            raise ISqliteError(
                "The set of reordered columns is not the same as the set of original "
                + "columns."
            )

        columns = [str(column_map[name]) for name in column_names]
        self._migrate_table(
            table_name, columns, select=", ".join(map(quote, column_names))
        )
        self.refresh_schema()

    def alter_column(self, table_name, column_name, new_column):
        """
        Alter the definition of a column.

        :param table_name: The table to alter.
        :param column_name: The column to alter.
        :param new_column: The new definition of the column, without the name, as a SQL
            string.
        """
        create_table_statement = self._get_create_table_statement(table_name)
        columns = []
        altered = False
        for column in create_table_statement.columns:
            if column.name == column_name:
                columns.append(f"{column_name} {new_column}")
                altered = True
            else:
                columns.append(str(column))

        if not altered:
            raise ColumnDoesNotExistError(table_name, column_name)

        self._migrate_table(
            table_name,
            columns,
            select=", ".join(quote(c.name) for c in create_table_statement.columns),
        )
        self.refresh_schema()

    def rename_column(self, table_name, old_column_name, new_column_name):
        """
        Rename a column.
        """
        # ALTER TABLE ... RENAME COLUMN is only supported since SQLite version 3.25, so
        # we implement it by hand here.
        create_table_statement = self._get_create_table_statement(table_name)
        columns_before = create_table_statement.columns
        columns_after = []
        altered = False
        for column in create_table_statement.columns:
            if column.name == old_column_name:
                definition = (
                    str(column.definition) if column.definition is not None else ""
                )
                columns_after.append(f"{new_column_name} {definition}")
                altered = True
            else:
                columns_after.append(str(column))

        if not altered:
            raise ColumnDoesNotExistError(table_name, old_column_name)

        self._migrate_table(
            table_name,
            columns_after,
            select=", ".join(quote(c.name) for c in columns_before),
        )
        self.refresh_schema()

    def diff(self, schema, *, table=None):
        """
        Return a list of differences between the Python schema and the actual database
        schema.

        :param schema: The Python schema to compare against the database.
        :param table: The table to diff. If None, the entire database will be diffed.
        """
        schema = collections.OrderedDict((table.name, table) for table in schema)

        tables_in_db = self._get_sql_schema()
        tables_in_schema = schema.values() if table is None else [schema[table]]

        diff = collections.defaultdict(list)
        for table_in_schema in tables_in_schema:
            name = table_in_schema.name
            if name in tables_in_db:
                columns_in_database = tables_in_db.pop(table_in_schema.name).columns
                columns_in_schema = [
                    column for column in table_in_schema.columns.values()
                ]
                self._diff_table(diff, name, columns_in_database, columns_in_schema)
            else:
                diff[table_in_schema.name].append(
                    migrations.CreateTableMigration(
                        table_in_schema.name,
                        [str(column) for column in table_in_schema.columns.values()],
                    )
                )

        if table is None:
            for name in tables_in_db:
                diff[name].append(migrations.DropTableMigration(name))

        return diff

    def apply_diff(self, diff):
        """
        Apply the diff returned by ``Database.diff`` to the database.

        WARNING: This may cause columns or entire tables to be dropped from the
        database. Make sure to examine the diff before applying it, e.g. by using the
        ``isqlite migrate`` command.

        The entire operation will occur in a transaction.

        :param diff: A list of differences, as returned by ``Database.diff``.
        """
        with self.transaction(disable_foreign_keys=True):
            for table_diff in diff.values():
                for op in table_diff:
                    if isinstance(op, migrations.CreateTableMigration):
                        self.create_table(op.table_name, op.columns)
                    elif isinstance(op, migrations.DropTableMigration):
                        self.drop_table(op.table_name)
                    elif isinstance(op, migrations.AlterColumnMigration):
                        self.alter_column(
                            op.table_name,
                            op.column.name,
                            str(op.column.definition)
                            if op.column.definition is not None
                            else "",
                        )
                    elif isinstance(op, migrations.AddColumnMigration):
                        self.add_column(op.table_name, op.column)
                    elif isinstance(op, migrations.DropColumnMigration):
                        self.drop_column(op.table_name, op.column_name)
                    elif isinstance(op, migrations.ReorderColumnsMigration):
                        self.reorder_columns(op.table_name, op.column_names)
                    elif isinstance(op, migrations.RenameColumnMigration):
                        self.rename_column(
                            op.table_name, op.old_column_name, op.new_column_name
                        )
                    else:
                        raise ISqliteError("unknown migration op type")

            self.refresh_schema()

    def migrate(self, schema):
        """
        Migrate the database to match the Python schema.

        WARNING: This may cause columns or entire tables to be dropped from the
        database.

        The entire operation will occur in a transaction.

        :param schema: The Python schema to compare against the database.
        """
        self.apply_diff(self.diff(schema))

    def refresh_schema(self):
        """
        Refresh the database's internal representation of the SQL schema.

        Users do not normally need to call this function, as all the schema-altering
        methods on this class already call it automatically. But if you alter the schema
        using ``Database.sql`` or in an external database connection, you may need to
        call this method for correct behavior.

        The internal schema is used by the ``get_related`` functionality of ``list`` and
        ``get``.
        """
        self.schema = self._get_sql_schema()

    def transaction(self, *, disable_foreign_keys=False):
        """
        Begin a new transaction in a context manager.

        Intended for use as::

            with Database(transaction=False) as db:
               with db.transaction():
                   ...

               with db.transaction():
                   ...

        The return value of this method should be ignored.

        :param disable_foreign_keys: If true, foreign key enforcement will be disabled
            during the transaction. This is useful during database migrations.
        """
        return TransactionContextManager(
            self, disable_foreign_keys=disable_foreign_keys
        )

    def begin_transaction(self):
        """
        Begin a new transaction.

        Most users do not need this method. Instead, they should either use the default
        transaction opened by ``Database`` as a context manager, or they should
        explicitly manage their transactions with nested ``with db.transaction()``
        statements.
        """
        self.sql("BEGIN")

    def commit(self):
        """
        Commit the current transaction.

        Most users do not need this method. See the note to
        ``Database.begin_transaction``.
        """
        self.sql("COMMIT")

    def rollback(self):
        """
        Roll back the current transaction.

        Most users do not need this method. See the note to
        ``Database.begin_transaction``.
        """
        self.sql("ROLLBACK")

    @property
    def in_transaction(self):
        """
        Whether or not the database is currently in a transaction.
        """
        return self.connection.in_transaction

    def close(self):
        """
        Close the database connection. If a transaction is pending, commit it.

        Most users do not need this method. Instead, they should use ``Database`` in a
        ``with`` statement so that the database will be closed automatically.
        """
        if self.in_transaction:
            self.commit()
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.in_transaction:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()

        self.close()

    def _diff_table(self, diff, table_name, columns_in_database, columns_in_schema):
        columns_in_database_map = {
            column.name: i for i, column in enumerate(columns_in_database)
        }
        renamed_columns = set()
        reordered = False
        for new_index, column in enumerate(columns_in_schema):
            old_index = columns_in_database_map.get(column.name)
            if old_index is None:
                # TODO(#55): Re-enable this.
                # if (
                #     new_index < len(columns_in_database)
                #     and column.definition == columns_in_database[new_index].definition
                # ):
                #     old_column_name = columns_in_database[new_index].name
                #     renamed_columns.add(old_column_name)
                #     diff[table_name].append(
                #         RenameColumnMigration(table_name, old_column_name, column.name)
                #     )
                # else:
                #     diff[table_name].append(AddColumnMigration(table_name, column))
                diff[table_name].append(
                    migrations.AddColumnMigration(table_name, column)
                )
                continue

            if old_index != new_index:
                reordered = True

            old_column = columns_in_database[old_index]
            if old_column != column:
                diff[table_name].append(
                    migrations.AlterColumnMigration(table_name, column)
                )

        columns_in_schema_map = {
            column.name: i for i, column in enumerate(columns_in_schema)
        }
        for column in columns_in_database:
            if (
                column.name not in columns_in_schema_map
                and column.name not in renamed_columns
            ):
                diff[table_name].append(
                    migrations.DropColumnMigration(table_name, column.name)
                )

        if reordered:
            diff[table_name].append(
                migrations.ReorderColumnsMigration(
                    table_name, [column.name for column in columns_in_schema]
                )
            )

        return diff

    def _migrate_table(self, name, columns, *, select):
        # This procedure is copied from https://sqlite.org/lang_altertable.html
        # Create the new table under a temporary name.
        tmp_table_name = quote(f"isqlite_tmp_{name}")
        self.sql(f"CREATE TABLE {tmp_table_name}({', '.join(columns)})")

        # Copy over all data from the old table into the new table using the
        # provided SELECT values.
        self.sql(f"INSERT INTO {tmp_table_name} SELECT {select} FROM {quote(name)}")

        # Drop the old table.
        self.sql(f"DROP TABLE {quote(name)}")

        # Rename the new table to the original name.
        self.sql(f"ALTER TABLE {tmp_table_name} RENAME TO {quote(name)}")

        # Check that no foreign key constraints have been violated.
        self.sql("PRAGMA foreign_key_check")

    def _get_create_table_statement(self, table_name):
        sql = self.sql(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :table",
            {"table": table_name},
            as_tuple=True,
            multiple=False,
        )[0]
        return sqliteparser.parse(sql)[0]

    def _get_related_columns_and_joins(self, table, get_related):
        table_schema = self.schema[table]
        if get_related is True:
            get_related = {
                column.name
                for column in table_schema.columns
                if is_foreign_key_column(column)
                # Don't fetch recursive relations because this will cause 'ambiguous
                # column' errors in the SQL query.
                and get_foreign_key_model(column) != table
            }
        else:
            get_related = set(get_related)

        columns = []
        joins = []
        for column in table_schema.columns:
            if column.name in get_related:
                # Remove the column from the set so that we can check for any
                # non-existent columns at the end.
                get_related.remove(column.name)

                if not is_foreign_key_column(column):
                    raise ISqliteError(
                        f"{column.name!r} was passed in `get_related`, "
                        + "but it is not a foreign key column"
                    )

                foreign_table = get_foreign_key_model(column)
                related_table_schema = self.schema[foreign_table]
                for related_column in related_table_schema.columns:
                    name = f"{column.name}____{related_column.name}"
                    columns.append(
                        f"{quote(foreign_table)}.{quote(related_column.name)} "
                        + f"AS {quote(name)}"
                    )

                joins.append((column.name, foreign_table))
            else:
                columns.append(f"{quote(table)}.{quote(column.name)}")

        # We popped columns from `get_related` as we went, so if there are any left,
        # they are not valid columns of the table.
        if get_related:
            random = get_related.pop()
            raise ColumnDoesNotExistError(table, random)

        columns = ", ".join(columns)
        joins = "\n".join(
            f"LEFT JOIN {quote(join_table)} ON "
            + f"{quote(table)}.{quote(join_column)} = {quote(join_table)}.id"
            for join_column, join_table in joins
        )
        return columns, joins

    def _get_sql_schema(self):
        tables_in_db = {
            row["name"]: sqliteparser.parse(row["sql"])[0]
            for row in self.list(
                "sqlite_master", where="type = 'table' AND NOT name LIKE 'sqlite_%'"
            )
        }
        return tables_in_db


class TransactionContextManager:
    def __init__(self, db, *, disable_foreign_keys=False):
        self.db = db
        self.disable_foreign_keys = disable_foreign_keys

    def __enter__(self):
        if self.disable_foreign_keys:
            # We disable foreign keys before the BEGIN statement because, per the SQLite
            # docs:
            #
            #  foreign key constraint enforcement may only be enabled or disabled when
            #  there is no pending BEGIN or SAVEPOINT
            #
            # Source: https://sqlite.org/pragma.html#pragma_foreign_keys
            if self.db.in_transaction:
                raise ISqliteError(
                    "Foreign key enforcement cannot be disabled inside a transaction."
                )
            self.db.sql("PRAGMA foreign_keys = 0")

        self.db.begin_transaction()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is not None:
            self.db.rollback()
        else:
            self.db.commit()

        self.db.sql("PRAGMA foreign_keys = 1")


def ordered_dict_row_factory(cursor, row):
    r = collections.OrderedDict()

    for i, column in enumerate(cursor.description):
        name = column[0]
        value = row[i]

        # When `get_related` is passed to `Database.get` or `Database.list`, the SQL
        # query fetches columns from foreign key relationships and names them with the
        # format {original_table_column}____{related_table_column}, i.e. if the
        # `students` table has a `major` column that points to the `departments` table,
        # and the `departments` table has a `name` column, then `major____departments`
        # would be one of the columns in a query on the `students` table.
        if "____" in name:
            base_name, child_name = name.split("____", maxsplit=1)

            # The logic here is a little tricky for null foreign keys. What we will see
            # is a bunch of columns like `student____id`, `student____name` etc. that
            # are all set to None, so naively we would construct a dictionary for the
            # `student` column with every key set to None, when really we want `student`
            # itself to be None.
            #
            # First we check if we've already started populating this column, in which
            # case `base_name` will already be in `r`.
            dct = r.get(base_name)
            if dct is None:
                # If it is not, we know that we are looking at the ID column of the
                # related row.
                if value is None:
                    # If the ID column is None, then the whole row must be null.
                    r[base_name] = None
                else:
                    # If not, we initialize the OrderedDict for the related row.
                    r[base_name] = collections.OrderedDict([(child_name, value)])
            elif isinstance(dct, collections.OrderedDict):
                # If we've already started populating this column, then either (a) it is
                # None and we shouldn't do anything, or (b) it is an OrderedDict and we
                # should add to it.
                dct[child_name] = value
        else:
            r[name] = value

    return r


def is_foreign_key_column(column: sqliteparser.ast.Column) -> bool:
    return any(
        isinstance(constraint, sqliteparser.ast.ForeignKeyConstraint)
        for constraint in column.definition.constraints
    )


def get_foreign_key_model(column: sqliteparser.ast.Column) -> str:
    for constraint in column.definition.constraints:
        if isinstance(constraint, sqliteparser.ast.ForeignKeyConstraint):
            return constraint.foreign_table

    return None


class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = collections.OrderedDict()

        for column in columns:
            if isinstance(column, str):
                column = sqliteparser.parse_column(column)

            self.columns[column.name] = column


class AutoTable(Table):
    def __init__(self, name, columns):
        id_column = primary_key_column("id")
        created_at_column = timestamp_column("created_at", required=True)
        last_updated_at_column = timestamp_column("last_updated_at", required=True)
        columns = [id_column] + columns + [created_at_column, last_updated_at_column]
        super().__init__(name, columns)


class PrintDebugger:
    def execute(self, sql, values):
        self._execute("Execute", sql, values)

    def executemany(self, sql, values):
        self._execute("Execute many", sql, values)

    def _execute(self, title, sql, values):
        print()
        print("=== SQL DEBUGGER ===")
        print(f"{title}:")
        print()
        print(textwrap.indent(sql, "  "))
        print()
        print(textwrap.indent(f"Values: {values!r}", "  "))
        print()
        print("=== END SQL DEBUGGER ===")


class ISqliteError(Exception):
    pass


class ISqliteApiError(ISqliteError):
    pass


class ColumnDoesNotExistError(ISqliteError):
    pass


class TableDoesNotExistError(ISqliteError):
    pass
