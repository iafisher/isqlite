import collections
import sqlite3
import textwrap
import warnings
from typing import Any, Dict, List, Optional, Tuple, Union

import sqliteparser
from sqliteparser import quote

from . import migrations
from .exceptions import (
    ColumnDoesNotExistError,
    ISqliteApiError,
    ISqliteError,
    TableDoesNotExistError,
)
from .schema import Diff, Schema, diff_schemas, diff_tables, rename_column

CURRENT_ISO_8601_TIMESTAMP_SQL = "STRFTIME('%Y-%m-%d %H:%M:%f', 'now')"
CURRENT_EPOCH_TIMESTAMP_SQL = "STRFTIME('%s', 'now')"
AUTO_TIMESTAMP_DEFAULT = ("created_at", "last_updated_at")
AUTO_TIMESTAMP_UPDATE_DEFAULT = ("last_updated_at",)


# Type aliases
Row = Dict[str, Any]
Rows = List[Dict]


class Database:
    """
    A class to represent a connection to a SQLite database. Typically used as a context
    manager::

        with Database("db.sqlite3") as db:
            ...

    On creation, the ``Database`` connection will open a SQL transaction which will be
    either committed or rolled back at the end of the ``with`` statement, depending on
    whether an exception occurs.

    You can also have multiple transactions over the life of the connection::

        with Database("db.sqlite3", transaction=False) as db:
            with db.transaction():
                ...

            with db.transaction():
                ...
    """

    connection: sqlite3.Connection
    cursor: sqlite3.Cursor
    debugger: Optional["Debugger"]
    schema: Schema
    insert_auto_timestamp_columns: List[str]
    update_auto_timestamp_columns: List[str]

    def __init__(
        self,
        path: str,
        *,
        transaction: bool = True,
        debug: bool = False,
        readonly: Optional[bool] = None,
        uri: bool = False,
        cached_statements: int = 100,
        enforce_foreign_keys: bool = True,
        insert_auto_timestamp_columns: List[str] = [],
        update_auto_timestamp_columns: List[str] = [],
        use_epoch_timestamps: bool = False,
    ) -> None:
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

        :param debug: If true, each SQL statement executed will be printed to standard
            output.
        :param readonly: If true, the database will be opened in read-only mode. This
            option is incompatibility with ``uri=True``; if you need to pass a URI, then
            append ``?mode=ro`` to make it read-only. Defaults to false.
        :param uri: If true, the ``path`` argument is interpreted as a URI rather than a
            file path.
        :param cached_statements: Passed on to ``sqlite3.connect``.
        :param enforce_foreign_keys: If true, foreign-key constraint enforcement will be
            turned out with ``PRAGMA foreign_keys = 1``.
        :param insert_auto_timestamp_columns: A default value for
            ``auto_timestamp_columns`` in ``insert`` and ``insert_many``. Usually set to
            ``["created_at", "last_updated_at"]`` in conjunction with a schema defined
            using ``AutoTable``. It is recommended to set ``use_epoch_timestamps`` to
            ``True`` if using this parameter.
        :param update_auto_timestamp_columns: A default value for
            ``auto_timestamp_columns`` in ``update``. Usually set to
            ``["last_updated_at"]`` in conjunction with a schema defined using
            ``AutoTable``. It is recommended to set ``use_epoch_timestamps`` to ``True``
            if using this parameter.
        :param use_epoch_timestamps: Store ``auto_timestamp_columns`` as seconds since
            the Unix epoch instead of as ISO 8601 datetime strings. Recommended setting
            is ``True``, but default is ``False`` for backwards compatibility.
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

        if path == ":memory":
            warnings.warn("Did you mean to pass `:memory:` instead of `:memory`?")

        if not uri:
            if readonly is True:
                path = f"file:{path}?mode=ro"
            else:
                path = f"file:{path}"

        self.insert_auto_timestamp_columns = insert_auto_timestamp_columns
        self.update_auto_timestamp_columns = update_auto_timestamp_columns
        self.current_timestamp_sql = (
            CURRENT_EPOCH_TIMESTAMP_SQL
            if use_epoch_timestamps
            else CURRENT_ISO_8601_TIMESTAMP_SQL
        )

        self.connection = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            uri=True,
            # Setting `isolation_level` to None disables quirky behavior around
            # transactions, per https://stackoverflow.com/questions/30760997/
            isolation_level=None,
            cached_statements=cached_statements,
        )

        self.debugger = Debugger() if debug else None

        self.connection.row_factory = ordered_dict_row_factory
        self.cursor = self.connection.cursor()

        if enforce_foreign_keys:
            # This must be executed outside a transaction, according to the official
            # SQLite docs: https://sqlite.org/pragma.html#pragma_foreign_keys
            self.sql("PRAGMA foreign_keys = 1")

        if transaction:
            self.sql("BEGIN")

        self.refresh_schema()

    def select(
        self,
        table: str,
        *,
        columns: List[str] = [],
        where: str = "",
        values: Dict[str, Any] = {},
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[Union[Tuple[str], str]] = None,
        descending: Optional[bool] = None,
        get_related: Union[List[str], bool] = [],
    ) -> Rows:
        """
        Return a list of database rows as ``OrderedDict`` objects.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param columns: The columns of the table to return. By default, all columns are
            returned.
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
            ``descending=True``. To order by multiple columns, pass in a tuple.
        :param descending: If true, return results in descending order instead of
            ascending.
        :param get_related: A list of foreign-key columns of the table to be retrieved
            and embedded into the returned dictionaries. If true, all foreign-key
            columns will be retrieved. This parameter requires that ``Database`` was
            initialized with a ``schema`` parameter.
        """
        if order_by:
            if isinstance(order_by, (tuple, list)):
                order_by = ", ".join(map(quote, order_by))

            direction = "DESC" if descending is True else "ASC"
            order_clause = f"ORDER BY {order_by} {direction}"
        else:
            if descending is not None:
                raise ISqliteApiError(
                    "The `descending` parameter to `select` requires the `order_by` "
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
                    "The `offset` parameter to `select` requires the `limit` parameter "
                    + "to be set."
                )

            limit_clause = ""

        where_clause = f"WHERE {where}" if where else ""

        if get_related:
            selection, joins = self._get_related_columns_and_joins(
                table, columns, get_related
            )
            rows = self.sql(
                f"SELECT {selection} FROM {quote(table)} {joins} {where_clause}"
                + f" {order_clause} {limit_clause}",
                values,
            )
        else:
            if columns:
                selection = ", ".join(map(quote, columns))
            else:
                selection = "*"

            rows = self.sql(
                f"SELECT {selection} FROM {quote(table)} {where_clause} {order_clause}"
                + f" {limit_clause}",
                values,
            )

        return rows

    def get(
        self,
        table: str,
        *,
        columns: List[str] = [],
        where: str = "",
        values: Dict[str, Any] = {},
        order_by: Optional[Union[Tuple[str], str]] = None,
        descending: Optional[bool] = None,
        get_related: Union[List[str], bool] = [],
    ) -> Optional[Row]:
        """
        Retrieve a single row from the database table and return it as an
        ``OrderedDict`` object.

        Equivalent to ``Database.select(*args, **kwargs)[0]`` except that ``None`` is
        returned if no matching row is found, and the SQLite engine only fetches a
        single row from the database.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param columns: Same as for ``Database.select``.
        :param where: Same as for ``Database.select``.
        :param values: Same as for ``Database.select``.
        :param order_by: Same as for ``Database.select``.
        :param descending: Same as for ``Database.select``.
        :param get_related: Same as for ``Database.select``.
        """
        rows = self.select(
            table,
            columns=columns,
            where=where,
            values=values,
            order_by=order_by,
            descending=descending,
            limit=1,
            get_related=get_related,
        )
        return rows[0] if rows else None

    def get_by_pk(
        self,
        table: str,
        pk: int,
        *,
        columns: List[str] = [],
        get_related: Union[List[str], bool] = [],
    ) -> Optional[Row]:
        """
        Retrieve a single row from the database table by its primary key.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param pk: The primary key of the row to return.
        :param columns: Passed on to ``Database.get``.
        :param get_related: Passed on to ``Database.get``.
        """
        pk_column = f"{quote(table)}.rowid"
        return self.get(
            table,
            columns=columns,
            where=f"{pk_column} = :pk",
            values={"pk": pk},
            get_related=get_related,
        )

    def get_or_insert(self, table: str, data: Row, **kwargs) -> Row:
        """
        Retrieve a single row from the database table matching the parameters in
        ``data``. If no such row exists, insert it and return it.

        Not to be confused with ``insert_and_get``, which unconditionally inserts a row
        and returns it.

        The returned ``collections.OrderedDict`` object will have an additional
        ``inserted`` attribute that indicates whether or not a new row was inserted into
        the database.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param data: The parameters to match the database row. All required columns of
            the table must be included, or else the internal call to ``Database.insert``
            will fail.
        :param kwargs: Additional arguments to pass on to ``Database.insert``. If the
            database row already exists, these arguments are ignored.
        """
        if not data:
            raise ISqliteError(
                "The `data` parameter to `get_or_insert` cannot be empty."
            )

        query = " AND ".join(f"{key} = :{key}" for key in data)
        row = self.get(table, where=query, values=data)
        if row is None:
            row = self.insert_and_get(table, data, **kwargs)
            row.inserted = True  # type: ignore
        else:
            row.inserted = False  # type: ignore

        return row

    def count(
        self,
        table: str,
        *,
        where: str = "",
        values: Dict[str, Any] = {},
        distinct: str = "",
    ) -> int:
        """
        Return the count of rows matching the parameters.

        :param table: The database table to query. WARNING: This value is directly
            interpolated into the SQL statement. Do not pass untrusted input, to avoid
            SQL injection attacks.
        :param where: Same as for ``Database.select``.
        :param values: Same as for ``Database.select``.
        :param distinct: Only count rows with distinct values of this column.
        """
        where_clause = f"WHERE {where}" if where else ""
        count_expression = "COUNT(*)" if not distinct else f"COUNT(DISTINCT {distinct})"
        result = self.sql(
            f"SELECT {count_expression} FROM {quote(table)} {where_clause}",
            values,
            as_tuple=True,
            multiple=False,
        )
        return result[0]

    def insert(
        self,
        table: str,
        data: Row,
        *,
        auto_timestamp_columns: Union[List[str], bool] = True,
    ) -> int:
        """
        Insert a new row and return its primary key.

        To get the contents of the row after it is inserted, use ``insert_and_get``.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param data: The row to insert, as a dictionary from column names to column
            values.
        :param auto_timestamp_columns: A list of columns into which to insert the
            current date and time, as an ISO 8601 timestamp. If true, it defaults to
            the value of ``insert_auto_timestamp_columns`` passed to ``__init__``.
        """
        if isinstance(auto_timestamp_columns, bool):
            if auto_timestamp_columns is True:
                auto_timestamp_columns_list = self.insert_auto_timestamp_columns
            else:
                auto_timestamp_columns_list = []
        else:
            auto_timestamp_columns_list = auto_timestamp_columns

        keys = list(data.keys())
        # Profiling revealed that constructing the placeholder string in this fashion
        # is significantly faster than using ``join``.
        placeholders = ("?," * len(keys))[:-1]
        values = list(data.values())

        extra_columns_list = []
        for column in auto_timestamp_columns_list:
            keys.append(column)
            extra_columns_list.append(self.current_timestamp_sql)

        if extra_columns_list:
            extra_columns = (", " if data else "") + ", ".join(extra_columns_list)
        else:
            extra_columns = ""

        sql = f"""
        INSERT INTO {quote(table)}({', '.join(map(quote, keys))})
        VALUES ({placeholders}{extra_columns});
        """
        if self.debugger:
            self.debugger.execute(sql, values)
        self.cursor.execute(sql, values)
        return self.cursor.lastrowid

    def insert_and_get(
        self,
        table: str,
        data: Row,
        *,
        columns: List[str] = [],
        auto_timestamp_columns: Union[List[str], bool] = True,
        get_related: Union[List[str], bool] = [],
    ) -> Row:
        """
        Same as ``insert``, except it fetches the row after it is inserted and returns
        it. Note that this requires an extra SQL query.

        Not to be confused with ``get_or_insert``, which first tries to fetch a matching
        row and only inserts a new row if no matching one exists.

        The returned row may differ from ``data`` for two reasons:

        - The ``auto_timestamp_columns`` parameter causes isqlite to insert values
          into additional columns besides those in ``data``.
        - SQLite will supply default values if possible for any columns of the table
          omitted from ``data``.
        """
        pk = self.insert(table, data, auto_timestamp_columns=auto_timestamp_columns)
        row = self.get_by_pk(table, pk, columns=columns, get_related=get_related)
        assert row is not None
        return row

    def insert_many(
        self,
        table: str,
        data: Rows,
        *,
        auto_timestamp_columns: Union[List[str], bool] = True,
    ) -> None:
        """
        Insert multiple rows at once.

        Equivalent to::

            for row in data:
                db.insert(table, row)

        but more efficient.
        """
        if not data:
            return

        if isinstance(auto_timestamp_columns, bool):
            if auto_timestamp_columns is True:
                auto_timestamp_columns_list = self.insert_auto_timestamp_columns
            else:
                auto_timestamp_columns_list = []
        else:
            auto_timestamp_columns_list = auto_timestamp_columns

        keys = list(data[0].keys())
        placeholders = ",".join("?" for _ in range(len(keys)))

        extra_columns_list = []
        for column in auto_timestamp_columns_list:
            keys.append(column)
            extra_columns_list.append(self.current_timestamp_sql)

        if extra_columns_list:
            extra_columns = (", " if data else "") + ", ".join(extra_columns_list)
        else:
            extra_columns = ""

        sql = f"""
        INSERT INTO {quote(table)}({', '.join(map(quote, keys))})
        VALUES ({placeholders}{extra_columns});
        """
        values = [tuple(d.values()) for d in data]
        if self.debugger:
            self.debugger.executemany(sql, values)
        self.cursor.executemany(sql, values)

    def update(
        self,
        table: str,
        data: Row,
        *,
        where: str = "",
        values: Dict[str, Any] = {},
        auto_timestamp_columns: Union[List[str], bool] = True,
    ) -> int:
        """
        Update existing rows and return the number of rows updated.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param data: The columns to update, as a dictionary from column names to column
            values.
        :param where: Restrict the set of rows to update. Same as for
            ``Database.select``.
        :param values: Same as for ``Database.select``.
        :param auto_timestamp_columns: Same as for ``Database.insert``, except that if
            the same column appears in both ``values`` and ``auto_timestamp_columns``,
            the timestamp will be inserted instead of the value.
        """
        if isinstance(auto_timestamp_columns, bool):
            if auto_timestamp_columns is True:
                auto_timestamp_columns_list = self.update_auto_timestamp_columns
            else:
                auto_timestamp_columns_list = []
        else:
            auto_timestamp_columns_list = auto_timestamp_columns

        updates_list = []
        for key, value in data.items():
            if key in auto_timestamp_columns_list:
                continue

            placeholder = f"v{len(values)}"
            values[placeholder] = value
            updates_list.append(f"{quote(key)} = :{placeholder}")

        for column in auto_timestamp_columns_list:
            updates_list.append(f"{quote(column)} = {self.current_timestamp_sql}")

        if not updates_list:
            raise ISqliteError(
                "updates cannot be empty - either `data` or `auto_timestamp_columns` "
                + "must be set"
            )

        updates = ", ".join(updates_list)
        where_clause = f"WHERE {where}" if where else ""
        sql = f"UPDATE {quote(table)} SET {updates} {where_clause}"
        if self.debugger:
            self.debugger.execute(sql, values)
        self.cursor.execute(sql, values)
        return self.cursor.rowcount

    def update_by_pk(self, table: str, pk: int, data: Row, **kwargs) -> bool:
        """
        Update a single row and return whether it was updated or not.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param pk: The primary key of the row to update.
        :param data: Same as for ``Database.update``.
        :param kwargs: Additional arguments to pass on to ``Database.update``.
        """
        pk_column = f"{quote(table)}.rowid"
        return bool(
            self.update(
                table,
                data,
                where=f"{pk_column} = :pk",
                values={"pk": pk},
                **kwargs,
            )
        )

    def delete(self, table: str, *, where: str, values: Dict[str, Any] = {}) -> None:
        """
        Delete a set of rows.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param where: Same as for ``Database.select``, except that it is required, to
            avoid accidentally deleting every row in a table. If you indeed wish to
            delete every row, then pass ``where="1"``.
        :param values: Same as for ``Database.select``.
        """
        if not where:
            raise ISqliteApiError(
                "The `where` argument to `delete` cannot be empty - to delete every row"
                + 'in the table, pass `where="1"`'
            )
        self.sql(f"DELETE FROM {quote(table)} WHERE {where}", values=values)

    def delete_by_pk(self, table: str, pk: int) -> None:
        """
        Delete a single row.

        :param table: The database table. WARNING: This value is directly interpolated
            into the SQL statement. Do not pass untrusted input, to avoid SQL injection
            attacks.
        :param pk: The primary key of the row to delete.
        """
        pk_column = f"{quote(table)}.rowid"
        return self.delete(table, where=f"{pk_column} = :pk", values={"pk": pk})

    def sql(
        self,
        query: str,
        values: Dict[str, Any] = {},
        *,
        as_tuple: bool = False,
        multiple: bool = True,
    ) -> Any:
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

    def create_table(self, table_name: str, columns: List[str]) -> None:
        """
        Create a new table.

        :param table_name: The name of the table to create.
        :param columns: A list of columns, as raw SQL strings.
        """
        if isinstance(columns, str):
            raise ISqliteApiError(
                "second argument to `Database.create_table` must be a list, "
                + "not a string"
            )

        self.sql(f"CREATE TABLE {quote(table_name)}({','.join(map(str, columns))})")
        self.refresh_schema()

    def drop_table(self, table_name: str) -> None:
        """
        Drop a table.

        :param table_name: The name of the table to drop.
        """
        self.sql(f"DROP TABLE {quote(table_name)}")
        self.refresh_schema()

    def rename_table(self, old_table_name: str, new_table_name: str) -> None:
        """
        Rename a table.
        """
        self.sql(
            f"ALTER TABLE {quote(old_table_name)} RENAME TO {quote(new_table_name)}"
        )
        self.refresh_schema()

    def add_column(self, table_name: str, column_def: str) -> None:
        """
        Add a column to the table's schema.

        :param table_name: The name of the table.
        :param column_def: The definition of the column to add, as raw SQL.
        """
        self.sql(f"ALTER TABLE {quote(table_name)} ADD COLUMN {column_def}")
        self.refresh_schema()

    def drop_column(self, table_name: str, column_name: str) -> None:
        """
        Drop a column from the database.
        """
        # ALTER TABLE ... DROP COLUMN is only supported since SQLite version 3.35, so we
        # implement it by hand here.
        table_schema = self.schema[table_name]
        columns = [
            str(column) for column in table_schema.columns if column.name != column_name
        ]
        if len(columns) == len(table_schema.columns):
            raise ColumnDoesNotExistError(table_name, column_name)

        select = ", ".join(
            quote(c.name) for c in table_schema.columns if c.name != column_name
        )
        self._migrate_table(table_name, columns, select=select)
        self.refresh_schema()

    def reorder_columns(self, table_name: str, column_names: List[str]) -> None:
        """
        Reorder the columns of a database table.

        :param table_name: The table to reorder.
        :param column_names: The new order of the columns, as a list of strings. The
            column names must be the same as in the database; otherwise, an exception
            will be raised.
        """
        table_schema = self.schema[table_name]
        column_map = collections.OrderedDict((c.name, c) for c in table_schema.columns)

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

    def alter_column(self, table_name: str, column_name: str, new_column: str) -> None:
        """
        Alter the definition of a column.

        :param table_name: The table to alter.
        :param column_name: The column to alter.
        :param new_column: The new definition of the column, without the name, as a SQL
            string.
        """
        table_schema = self.schema[table_name]
        columns = []
        altered = False
        for column in table_schema.columns:
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
            select=", ".join(quote(c.name) for c in table_schema.columns),
        )
        self.refresh_schema()

    def rename_column(
        self, table_name: str, old_column_name: str, new_column_name: str
    ) -> None:
        """
        Rename a column.
        """
        # ALTER TABLE ... RENAME COLUMN is only supported since SQLite version 3.25, so
        # we implement it by hand here.
        table_schema = self.schema[table_name]

        columns_before = table_schema.columns
        columns_after = []
        altered = False
        for column in table_schema.columns:
            if column.name == old_column_name:
                columns_after.append(
                    str(rename_column(column, old_column_name, new_column_name))
                )
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

    def diff(self, schema: Schema, *, table="", detect_renaming=True) -> Diff:
        """
        Return a list of differences between the Python schema and the actual database
        schema.

        :param schema: The Python schema to compare against the database.
        :param table: The table to diff. If empty, the entire database will be diffed.
        :param detect_renaming: If true, the differ will attempt to detect renamed
            columns. Sometimes the differ does not detect renames correctly, so this
            option is available to disable renaming detection.
        """
        self.refresh_schema()
        if table:
            try:
                old_schema = self.schema[table]
                new_schema = schema[table]
            except KeyError:
                raise TableDoesNotExistError(table)

            return diff_tables(old_schema, new_schema, detect_renaming=detect_renaming)
        else:
            return diff_schemas(self.schema, schema, detect_renaming=detect_renaming)

    def apply_diff(self, diff: Diff) -> None:
        """
        Apply the diff returned by ``Database.diff`` to the database.

        WARNING: This may cause columns or entire tables to be dropped from the
        database. Make sure to examine the diff before applying it, e.g. by using the
        ``isqlite migrate`` command.

        The entire operation will occur in a transaction.

        :param diff: A list of differences, as returned by ``Database.diff``.
        """
        if self.in_transaction:
            raise ISqliteError(
                "`apply_diff` must be called outside a transaction. Did you mean to"
                + "pass `transaction=False` to the `Database` constructor?"
            )

        with self.transaction(disable_foreign_keys=True):
            for op in diff:
                if isinstance(op, migrations.CreateTableMigration):
                    self.create_table(op.table_name, op.columns)
                elif isinstance(op, migrations.DropTableMigration):
                    self.drop_table(op.table_name)
                elif isinstance(op, migrations.AlterColumnMigration):
                    self.alter_column(
                        op.table_name,
                        op.column_name,
                        op.column_definition,
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

    def migrate(self, schema: Schema, *, detect_renaming=True) -> None:
        """
        Migrate the database to match the Python schema.

        WARNING: This may cause columns or entire tables to be dropped from the
        database.

        The entire operation will occur in a transaction.

        :param schema: The Python schema to compare against the database.
        :param detect_renaming: Passed on to ``Database.diff``.
        """
        self.apply_diff(self.diff(schema, detect_renaming=detect_renaming))

    def refresh_schema(self) -> None:
        """
        Refresh the database's internal representation of the SQL schema.

        Users do not normally need to call this function, as all the schema-altering
        methods on this class already call it automatically. But if you alter the schema
        using ``Database.sql`` or in an external database connection, you may need to
        call this method for correct behavior.

        The internal schema is used by the ``get_related`` functionality of ``select``
        and ``get``.
        """
        self.schema = self._get_schema_from_database()

    def transaction(
        self, *, disable_foreign_keys: bool = False
    ) -> "TransactionContextManager":
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

    def begin_transaction(self) -> None:
        """
        Begin a new transaction.

        Most users do not need this method. Instead, they should either use the default
        transaction opened by ``Database`` as a context manager, or they should
        explicitly manage their transactions with nested ``with db.transaction()``
        statements.
        """
        self.sql("BEGIN")

    def commit(self) -> None:
        """
        Commit the current transaction.

        Most users do not need this method. See the note to
        ``Database.begin_transaction``.
        """
        self.sql("COMMIT")

    def rollback(self) -> None:
        """
        Roll back the current transaction.

        Most users do not need this method. See the note to
        ``Database.begin_transaction``.
        """
        self.sql("ROLLBACK")

    @property
    def in_transaction(self) -> bool:
        """
        Whether or not the database is currently in a transaction.
        """
        return self.connection.in_transaction

    def close(self) -> None:
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

    def _migrate_table(self, name: str, columns: List[str], *, select: str) -> None:
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

    def _get_related_columns_and_joins(
        self,
        table: str,
        columns_to_select: List[str],
        get_related: Union[List[str], bool],
    ) -> Tuple[str, str]:
        # Normalize `get_related` to a set of column names.
        table_schema = self.schema[table]
        if isinstance(get_related, bool):
            if get_related is True:
                get_related_set = {
                    column.name
                    for column in table_schema.columns
                    if is_foreign_key_column(column)
                    # Don't fetch recursive relations because this will cause 'ambiguous
                    # column' errors in the SQL query.
                    and get_foreign_key_model(column) != table
                }
            else:
                get_related_set = set()
        else:
            get_related_set = set(get_related)

        columns_list = []
        joins_list = []
        for column in table_schema.columns:
            if columns_to_select and column.name not in columns_to_select:
                continue

            if column.name in get_related_set:
                # Remove the column from the set so that we can check for any
                # non-existent columns at the end.
                get_related_set.remove(column.name)

                if not is_foreign_key_column(column):
                    raise ISqliteError(
                        f"{column.name!r} was passed in `get_related`, "
                        + "but it is not a foreign key column"
                    )

                foreign_table = get_foreign_key_model(column)
                if foreign_table is None:
                    raise ColumnDoesNotExistError(table, foreign_table)

                related_table_schema = self.schema[foreign_table]
                for related_column in related_table_schema.columns:
                    name = f"{column.name}____{related_column.name}"
                    columns_list.append(
                        f"{quote(foreign_table)}.{quote(related_column.name)} "
                        + f"AS {quote(name)}"
                    )

                joins_list.append((column.name, foreign_table))
            else:
                columns_list.append(f"{quote(table)}.{quote(column.name)}")

        # We popped columns from `get_related` as we went, so if there are any left,
        # they are not valid columns of the table.
        if get_related_set:
            random = get_related_set.pop()
            raise ColumnDoesNotExistError(table, random)

        columns = ", ".join(columns_list)
        joins = "\n".join(
            f"LEFT JOIN {quote(join_table)} ON "
            + f"{quote(table)}.{quote(join_column)} = {quote(join_table)}.id"
            for join_column, join_table in joins_list
        )
        return columns, joins

    def _get_schema_from_database(self) -> Schema:
        return Schema(
            [
                sqliteparser.parse(row["sql"])[0]
                for row in self.select(
                    "sqlite_master", where="type = 'table' AND NOT name LIKE 'sqlite_%'"
                )
            ]
        )


class TransactionContextManager:
    def __init__(self, db: Database, *, disable_foreign_keys: bool = False) -> None:
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


def ordered_dict_row_factory(cursor: sqlite3.Cursor, row: Tuple[Any]) -> Row:
    r: Row = collections.OrderedDict()

    for i, column in enumerate(cursor.description):
        name = column[0]
        value = row[i]

        # When `get_related` is passed to `Database.get` or `Database.select`, the SQL
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


def get_foreign_key_model(column: sqliteparser.ast.Column) -> Optional[str]:
    for constraint in column.definition.constraints:
        if isinstance(constraint, sqliteparser.ast.ForeignKeyConstraint):
            return constraint.foreign_table

    return None


class Debugger:
    def execute(self, sql: str, values: Any) -> None:
        self._execute("Execute", sql, values)

    def executemany(self, sql: str, values: Any) -> None:
        self._execute("Execute many", sql, values)

    def _execute(self, title: str, sql: str, values: Any) -> None:
        print()
        print("=== SQL DEBUGGER ===")
        print(f"{title}:")
        print()
        print(textwrap.indent(sql, "  "))
        print()
        print(textwrap.indent(f"Values: {values!r}", "  "))
        print()
        print("=== END SQL DEBUGGER ===")
