import collections
import functools
import sqlite3

from . import columns as isqlite_columns
from . import query as q

CURRENT_TIMESTAMP = "STRFTIME('%Y-%m-%d %H:%M:%f000+00:00', 'now')"


class Table:
    reserved_column_names = {"id", "created_at", "last_updated_at"}

    def __init__(self, name, columns):
        if name.startswith("isqlite"):
            raise ISQLiteError(
                "Table names beginning with 'isqlite' are reserved for internal use by "
                + "isqlite. Please choose a different name."
            )

        self.name = name
        self.columns = collections.OrderedDict()

        self.columns["id"] = isqlite_columns.Integer(
            "id", autoincrement=True, primary_key=True, required=True
        )
        for column in columns:
            if column.name in self.reserved_column_names:
                raise ISQLiteError(
                    f"The column name {column.name!r} is reserved for internal use by "
                    + "isqlite. Please choose a different name."
                )

            if column.name in self.columns:
                raise ISQLiteError(
                    f"Column {column.name!r} was defined multiple times."
                )

            self.columns[column.name] = column

        self.columns["created_at"] = isqlite_columns.Timestamp(
            "created_at", required=True
        )
        self.columns["last_updated_at"] = isqlite_columns.Timestamp(
            "last_updated_at", required=True
        )


class Database:
    def __init__(self, schema, connection_or_path, *, readonly=None, uri=False):
        # Validate arguments.
        if readonly is not None:
            if uri is True:
                raise ISQLiteError(
                    "The `readonly` parameter cannot be set if `uri` is True. Append "
                    + "'?mode=ro' (or omit it if you don't want your connection to be "
                    + "read-only) to your URI instead."
                )

            if not isinstance(connection_or_path, str):
                if readonly is not None:
                    raise ISQLiteError(
                        "The `readonly` parameter can only be set if a database path "
                        + "is passed, not an existing database connection."
                    )
        else:
            # Default value of `readonly` if not specified is False.
            readonly = False

        self.schema = collections.OrderedDict()
        for table in schema:
            if table.name in self.schema:
                raise ISQLiteError(f"Table {table.name!r} was defined multiple times.")

            self.schema[table.name] = table

        if isinstance(connection_or_path, str):
            if uri:
                path = connection_or_path
            else:
                if readonly is True:
                    path = f"file:{connection_or_path}?mode=ro"
                else:
                    path = f"file:{connection_or_path}"

            self.connection = sqlite3.connect(
                path, detect_types=sqlite3.PARSE_DECLTYPES, uri=True
            )
        else:
            self.connection = connection_or_path

        self.connection.row_factory = ordered_dict_row_factory
        self.connection.execute("PRAGMA foreign_keys = on")
        self.cursor = self.connection.cursor()

    def get(self, table, query=None):
        sql, values = q.to_sql(query, convert_id=True)
        return self.sql(f"SELECT * FROM {table} {sql}", values, multiple=False)

    def get_or_create(self, table, data):
        if not data:
            raise ISQLiteError(
                "The `data` parameter to `get_or_create` cannot be empty."
            )

        query = functools.reduce(
            q.And, (q.Equals(key, value) for key, value in data.items())
        )
        row = self.get(table, query)
        if row is None:
            pk = self.create(table, data)
            return self.get(table, pk)
        else:
            return row

    def list(self, table, query=None, *, limit=None):
        sql, values = q.to_sql(query)
        return self.sql(f"SELECT * FROM {table} {sql}", values)

    def create(self, table, data):
        keys = list(data.keys())
        placeholders = ",".join("?" for _ in range(len(keys)))
        values = list(data.values())

        extra = []
        if "created_at" not in data:
            keys.append("created_at")
            extra.append(CURRENT_TIMESTAMP)
        if "last_updated_at" not in data:
            keys.append("last_updated_at")
            extra.append(CURRENT_TIMESTAMP)

        if extra:
            extra = (", " if data else "") + ", ".join(extra)
        else:
            extra = ""

        self.cursor.execute(
            f"""
            INSERT INTO {table}({', '.join(keys)}) VALUES ({placeholders}{extra});
            """,
            values,
        )
        return self.cursor.lastrowid

    def create_many(self, table, data):
        raise NotImplementedError

    def update(self, table, pk, data):
        data.pop("pk", None)
        data.pop("created_at", None)
        data.pop("last_updated_at", None)

        updates = ", ".join(f"{key}=?" for key in data.keys())
        updates += (", " if data else "") + "last_updated_at = " + CURRENT_TIMESTAMP
        self.cursor.execute(
            f"UPDATE {table} SET {updates} WHERE pk = ?;", tuple(data.values()) + (pk,),
        )

    def delete(self, table, query):
        sql, values = q.to_sql(query, convert_id=True)
        self.sql(f"DELETE FROM {table} {sql}", values)

    def sql(self, query, values={}, *, multiple=True):
        if multiple:
            self.cursor.execute(query, values)
            return self.cursor.fetchall()
        else:
            self.cursor.execute(query + " LIMIT 1", values)
            return self.cursor.fetchone()

    def create_database(self):
        sql = "\n\n".join(
            generate_create_table_statement(table.name, table.columns.values())
            for table in self.schema.values()
        )
        self.cursor.execute(sql)

    def add_table(self, table):
        sql = generate_create_table_statement(table.name, table.columns.values())
        self.cursor.execute(sql)

    def drop_table(self, table):
        self.cursor.execute(f"DROP TABLE {table}")

    def add_column(self, table, column):
        self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column}")

    def drop_column(self, table, column_name):
        # ALTER TABLE ... DROP COLUMN is only supported since SQLite version 3.35, so we
        # implement it by hand here.
        remaining_columns = [
            c for c in self.schema[table].columns.values() if c.name != column_name
        ]
        select = ", ".join([c.name for c in remaining_columns])
        self._migrate_table(table, remaining_columns, select=select)

    def reorder_columns(self, table, column_names):
        table_schema = self.schema[table]
        if set(column_names) != set(table_schema.columns.keys()):
            raise ISQLiteError(
                "The set of reordered columns is not the same as the set of original "
                + "columns. Note that you must include the `id`, `created_at`, and "
                + "`last_updated_at` columns in the list."
            )
        columns = [table_schema.columns[name] for name in column_names]
        self._migrate_table(table, columns, select=", ".join(column_names))

    def alter_column(self, table, column_name, new_column):
        table_schema = self.schema[table]
        columns = [
            column if column.name != column_name else new_column
            for column in table_schema.columns.values()
        ]
        self._migrate_table(
            table, columns, select=", ".join(table_schema.columns.keys())
        )

    def rename_column(self, table, old_column_name, new_column_name):
        # ALTER TABLE ... RENAME COLUMN is only supported since SQLite version 3.25, so
        # we implement it by hand here.
        table_schema = self.schema[table]
        column = table_schema.columns[old_column_name]
        column.name = new_column_name
        self.alter_column(table, old_column_name, column)

    def _migrate_table(self, table, columns, *, select):
        # This procedure is copied from https://sqlite.org/lang_altertable.html
        self.connection.execute("PRAGMA foreign_keys = off")
        try:
            with self.connection:
                # Create the new table under a temporary name.
                tmp_table_name = f"isqlite_tmp_{table}"
                self.sql(generate_create_table_statement(tmp_table_name, columns))

                # Copy over all data from the old table into the new table using the
                # provided SELECT values.
                self.sql(f"INSERT INTO {tmp_table_name} SELECT {select} FROM {table}")

                # Drop the old table.
                self.sql(f"DROP TABLE {table}")

                # Rename the new table to the original name.
                self.sql(f"ALTER TABLE {tmp_table_name} RENAME TO {table}")

                # Check that no foreign key constraints have been violated.
                self.sql("PRAGMA foreign_key_check")
        finally:
            self.connection.execute("PRAGMA foreign_keys = on")

    def close(self):
        self.connection.commit()
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


def generate_create_table_statement(name, columns):
    builder = ["CREATE TABLE IF NOT EXISTS ", name, "(\n"]
    for i, column in enumerate(columns):
        builder.append("  ")
        builder.append(str(column))
        if i != len(columns) - 1:
            builder.append(",")
        builder.append("\n")
    builder.append(");")
    return "".join(builder)


def ordered_dict_row_factory(cursor, row):
    d = collections.OrderedDict()
    for i, column in enumerate(cursor.description):
        d[column[0]] = row[i]
    return d


class ISQLiteError(Exception):
    pass
