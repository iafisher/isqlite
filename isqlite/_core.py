import collections
import functools
import re
import sqlite3

import sqlparse

from . import columns as isqlite_columns
from . import query as q
from ._exception import ISQLiteError

CURRENT_TIMESTAMP = "STRFTIME('%Y-%m-%d %H:%M:%f000+00:00', 'now')"


class Database:
    def __init__(self, connection_or_path, *, readonly=None, uri=False):
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
        self.connection.execute("PRAGMA foreign_keys = 1")
        self.cursor = self.connection.cursor()

    def get(self, table, query=None, *, camel_case=False):
        sql, values = q.to_sql(query, convert_id=True)
        return self.sql(
            f"SELECT * FROM {table} {sql}",
            values,
            camel_case=camel_case,
            multiple=False,
        )

    def get_or_create(self, table, data, *, camel_case=False):
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
            return self.get(table, pk, camel_case=camel_case)
        else:
            return row

    def list(self, table, query=None, *, camel_case=False, limit=None, order_by=None):
        # TODO(2021-04-30): Use `limit` parameter.
        sql, values = q.to_sql(query)
        if order_by is not None:
            sql = f"{sql} ORDER BY {order_by}"
        print(sql)
        return self.sql(f"SELECT * FROM {table} {sql}", values, camel_case=camel_case)

    def count(self, table, query=None):
        sql, values = q.to_sql(query)
        result = self.sql(
            f"SELECT COUNT(*) FROM {table} {sql}", values, as_tuple=True, multiple=False
        )
        return result[0]

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
        if not data:
            return

        keys = list(data[0].keys())
        placeholders = ",".join("?" for _ in range(len(keys)))

        extra = []
        if "created_at" not in data[0]:
            keys.append("created_at")
            extra.append(CURRENT_TIMESTAMP)
        if "last_updated_at" not in data[0]:
            keys.append("last_updated_at")
            extra.append(CURRENT_TIMESTAMP)

        if extra:
            extra = (", " if data else "") + ", ".join(extra)
        else:
            extra = ""

        self.cursor.executemany(
            f"""
            INSERT INTO {table}({', '.join(keys)}) VALUES ({placeholders}{extra});
            """,
            [tuple(d.values()) for d in data],
        )

    def update(self, table, query, data):
        sql, values = q.to_sql(query, convert_id=True)

        updates = []
        for key, value in data.items():
            if key == "last_updated_at":
                continue

            placeholder = f"v{len(values)}"
            values[placeholder] = value
            updates.append(f"{key} = :{placeholder}")

        updates.append(f"last_updated_at = {CURRENT_TIMESTAMP}")
        updates = ", ".join(updates)

        self.cursor.execute(f"UPDATE {table} SET {updates} {sql}", values)

    def delete(self, table, query):
        sql, values = q.to_sql(query, convert_id=True)
        self.sql(f"DELETE FROM {table} {sql}", values)

    def sql(self, query, values={}, *, as_tuple=False, camel_case=False, multiple=True):
        if multiple:
            self.cursor.execute(query, values)
            rows = self.cursor.fetchall()
            if as_tuple:
                return [tuple(row.values()) for row in rows]

            if camel_case:
                return [row_to_camel_case(row) for row in rows]

            return rows
        else:
            self.cursor.execute(query + " LIMIT 1", values)
            row = self.cursor.fetchone()
            if row is None:
                return row

            if as_tuple:
                return tuple(row.values())

            if camel_case:
                return row_to_camel_case(row)

            return row

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.commit()
        self.connection.close()

    def create_table(self, table):
        self.sql(table.get_create_table_statement())

    def drop_table(self, table_name):
        self.sql(f"DROP TABLE {table_name}")

    def add_column(self, table_name, column):
        column = column.as_raw_column()
        self.sql(f"ALTER TABLE {table_name} ADD COLUMN {column}")

    def drop_column(self, table_name, column_name):
        # ALTER TABLE ... DROP COLUMN is only supported since SQLite version 3.35, so we
        # implement it by hand here.
        table = self._get_table(table_name)
        table.columns.pop(column_name)
        select = ", ".join(table.columns.keys())
        self._migrate_table(table, select=select)

    def reorder_columns(self, table_name, column_names):
        table = self._get_table(table_name)
        if set(column_names) != set(table.columns.keys()):
            raise ISQLiteError(
                "The set of reordered columns is not the same as the set of original "
                + "columns. Note that you must include the `id`, `created_at`, and "
                + "`last_updated_at` columns in the list if your table includes them."
            )
        table.columns = collections.OrderedDict(
            (name, table.columns[name]) for name in column_names
        )
        self._migrate_table(table, select=", ".join(column_names))

    def alter_column(self, table_name, column_name, new_column):
        table = self._get_table(table_name)
        table.columns[column_name] = new_column.as_raw_column()
        self._migrate_table(table, select=", ".join(table.columns.keys()))

    def rename_column(self, table_name, old_column_name, new_column_name):
        # ALTER TABLE ... RENAME COLUMN is only supported since SQLite version 3.25, so
        # we implement it by hand here.
        raise NotImplementedError

    def _migrate_table(self, new_table, *, select):
        name = new_table.name
        # This procedure is copied from https://sqlite.org/lang_altertable.html
        try:
            self.sql("PRAGMA foreign_keys = 0")

            # Create the new table under a temporary name.
            tmp_table_name = f"isqlite_tmp_{name}"
            new_table.name = tmp_table_name
            self.create_table(new_table)

            # Copy over all data from the old table into the new table using the
            # provided SELECT values.
            self.sql(f"INSERT INTO {tmp_table_name} SELECT {select} FROM {name}")

            # Drop the old table.
            self.sql(f"DROP TABLE {name}")

            # Rename the new table to the original name.
            self.sql(f"ALTER TABLE {tmp_table_name} RENAME TO {name}")

            # Check that no foreign key constraints have been violated.
            self.sql("PRAGMA foreign_key_check")
        except Exception:
            self.rollback()
        else:
            self.commit()
        finally:
            self.sql("PRAGMA foreign_keys = 1")

    def _get_table(self, table_name):
        sql = self.sql(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :table",
            {"table": table_name},
            as_tuple=True,
            multiple=False,
        )[0]
        return get_table_from_create_statement(table_name, sql)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


class Table:
    def __init__(
        self, name, columns, constraints=[], *, auto_columns=True, without_rowid=False
    ):
        if name.startswith("isqlite"):
            raise ISQLiteError(
                "Table names beginning with 'isqlite' are reserved for internal use by "
                + "isqlite. Please choose a different name."
            )

        self.name = name
        self.without_rowid = without_rowid
        self.columns = collections.OrderedDict()

        if auto_columns:
            reserved = {"id", "created_at", "last_updated_at"}
        else:
            reserved = set()

        if auto_columns:
            self.columns["id"] = isqlite_columns.Integer(
                "id", autoincrement=True, primary_key=True, required=True
            ).as_raw_column()

        for column in columns:
            if column.name in reserved:
                raise ISQLiteError(
                    f"The column name {column.name!r} is reserved for internal use by "
                    + "isqlite. Please choose a different name."
                )

            if column.name in self.columns:
                raise ISQLiteError(
                    f"Column {column.name!r} was defined multiple times."
                )

            self.columns[column.name] = column.as_raw_column()

        if auto_columns:
            self.columns["created_at"] = isqlite_columns.Timestamp(
                "created_at", required=True
            ).as_raw_column()
            self.columns["last_updated_at"] = isqlite_columns.Timestamp(
                "last_updated_at", required=True
            ).as_raw_column()

        self.constraints = constraints

    def get_create_table_statement(self):
        builder = ["CREATE TABLE IF NOT EXISTS ", self.name, "(\n"]

        L = list(self.columns.values()) + self.constraints
        for i, column in enumerate(L):
            builder.append("  ")
            builder.append(str(column))
            if i != len(L) - 1:
                builder.append(",")
            builder.append("\n")

        builder.append(")")
        if self.without_rowid:
            builder.append("WITHOUT ROWID")
        builder.append(";")

        return "".join(builder)


def get_table_from_create_statement(name, sql):
    tokens = [
        t.value
        for t in sqlparse.parse(sql)[0].flatten()
        if t.ttype
        not in (
            sqlparse.tokens.Whitespace,
            sqlparse.tokens.Newline,
            sqlparse.tokens.Comment.Single,
            sqlparse.tokens.Comment.Multiline,
        )
    ]
    tokens.reverse()

    while tokens and tokens[-1] != "(":
        tokens.pop()

    tokens.pop()

    # CREATE TABLE syntax defined here: https://sqlite.org/syntax/create-table-stmt.html
    columns = []
    constraints = []
    while True:
        column_or_constraint, finished = match_column(tokens)

        if isinstance(column_or_constraint, isqlite_columns.RawColumn):
            columns.append(column_or_constraint)
        elif isinstance(column_or_constraint, isqlite_columns.RawConstraint):
            constraints.append(column_or_constraint)
        else:
            raise SyntaxError(type(column_or_constraint))

        if finished:
            break

    if (
        len(tokens) >= 2
        and tokens[-1].upper() == "WITHOUT"
        and tokens[-2].upper() == "ROWID"
    ):
        without_rowid = True
    else:
        without_rowid = False

    return Table(
        name, columns, constraints, auto_columns=False, without_rowid=without_rowid
    )


def match_column(tokens):
    name = tokens.pop()
    tokens_to_keep = []
    depth = 0
    finished = False
    while tokens:
        t = tokens.pop()
        if depth == 0 and t == ",":
            break

        if t == ")":
            if depth > 0:
                depth -= 1
            else:
                finished = True
                break

        if t == "(":
            depth += 1

        tokens_to_keep.append(t)

    # Possible keywords defined here: https://sqlite.org/syntax/table-constraint.html
    if name.upper() in ("CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"):
        return (
            isqlite_columns.RawConstraint(name + " " + " ".join(tokens_to_keep)),
            finished,
        )
    else:
        # Handle quoted identifiers.
        #
        # Based on https://sqlite.org/lang_keywords.html
        if (
            (name.startswith('"') and name.endswith('"'))
            or (name.startswith("[") and name.endswith("]"))
            or (name.startswith("`") and name.endswith("`"))
            or (name.startswith("'") and name.endswith("'"))
        ):
            name = name[1:-1]

        return isqlite_columns.RawColumn(name, " ".join(tokens_to_keep)), finished


def ordered_dict_row_factory(cursor, row):
    return collections.OrderedDict(
        (column[0], row[i]) for i, column in enumerate(cursor.description)
    )


def row_to_camel_case(row):
    return collections.OrderedDict(
        (string_to_camel_case(key), value) for key, value in row.items()
    )


camel_case_pattern = re.compile(r"._([a-z])")


def string_to_camel_case(s):
    return camel_case_pattern.sub(lambda m: m.group(0)[0] + m.group(1).upper(), s)
