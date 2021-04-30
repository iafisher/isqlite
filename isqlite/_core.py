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

    def list(self, table, query=None, *, camel_case=False, limit=None):
        sql, values = q.to_sql(query)
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
        sql = generate_create_table_statement(table.name, table.columns.values())
        self.sql(sql)

    def drop_table(self, table_name):
        self.sql(f"DROP TABLE {table_name}")

    def add_column(self, table_name, column):
        if isinstance(column, isqlite_columns.BaseColumn):
            column = column.as_raw_column()
        self.sql(f"ALTER TABLE {table_name} ADD COLUMN {column}")

    def drop_column(self, table_name, column_name):
        # ALTER TABLE ... DROP COLUMN is only supported since SQLite version 3.35, so we
        # implement it by hand here.
        columns, constraints = self._get_columns(table_name)
        remaining_columns = [c for c in columns if c.name != column_name]
        select = ", ".join([c.name for c in remaining_columns])
        self._migrate_table(table_name, remaining_columns, constraints, select=select)

    def reorder_columns(self, table_name, column_names):
        columns, constraints = self._get_columns(table_name)
        if set(column_names) != set(c.name for c in columns):
            raise ISQLiteError(
                "The set of reordered columns is not the same as the set of original "
                + "columns. Note that you must include the `id`, `created_at`, and "
                + "`last_updated_at` columns in the list if your table includes them."
            )
        column_map = {c.name: c for c in columns}
        reordered_columns = [column_map[name] for name in column_names]
        self._migrate_table(
            table_name, reordered_columns, constraints, select=", ".join(column_names)
        )

    def alter_column(self, table_name, column_name, new_column):
        columns, constraints = self._get_columns(table_name)
        altered_columns = [
            column if column.name != column_name else new_column for column in columns
        ]
        self._migrate_table(
            table_name,
            altered_columns,
            constraints,
            select=", ".join(c.name for c in columns),
        )

    def rename_column(self, table_name, old_column_name, new_column_name):
        # ALTER TABLE ... RENAME COLUMN is only supported since SQLite version 3.25, so
        # we implement it by hand here.
        columns, constraints = self._get_columns(table_name)
        for column in columns:
            if column.name == old_column_name:
                column.name = new_column_name
                break

        self._migrate_table(
            table_name, columns, constraints, select=", ".join(c.name for c in columns),
        )

    def _migrate_table(self, table_name, columns, constraints, *, select):
        # This procedure is copied from https://sqlite.org/lang_altertable.html
        try:
            self.sql("PRAGMA foreign_keys = 0")

            # Create the new table under a temporary name.
            tmp_table_name = f"isqlite_tmp_{table_name}"
            self.sql(
                generate_create_table_statement(tmp_table_name, columns, constraints)
            )

            # Copy over all data from the old table into the new table using the
            # provided SELECT values.
            self.sql(f"INSERT INTO {tmp_table_name} SELECT {select} FROM {table_name}")

            # Drop the old table.
            self.sql(f"DROP TABLE {table_name}")

            # Rename the new table to the original name.
            self.sql(f"ALTER TABLE {tmp_table_name} RENAME TO {table_name}")

            # Check that no foreign key constraints have been violated.
            self.sql("PRAGMA foreign_key_check")
        except Exception:
            self.rollback()
        else:
            self.commit()
        finally:
            self.sql("PRAGMA foreign_keys = on")

    def _get_columns(self, table_name):
        sql = self.sql(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :table",
            {"table": table_name},
            as_tuple=True,
            multiple=False,
        )[0]
        return get_columns_from_create_statement(sql)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


class Table:
    def __init__(self, name, columns, constraints=[], *, auto_columns=True):
        pass


def get_columns_from_create_statement(sql):
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

    return columns, constraints


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
        return isqlite_columns.RawColumn(name, " ".join(tokens_to_keep)), finished


def generate_create_table_statement(name, columns, constraints):
    builder = ["CREATE TABLE IF NOT EXISTS ", name, "(\n"]

    for i, column in enumerate(columns + constraints):
        builder.append("  ")

        if isinstance(column, isqlite_columns.BaseColumn):
            column = column.as_raw_column()
        builder.append(str(column))

        if i != len(columns) - 1:
            builder.append(",")
        builder.append("\n")

    builder.append(");")
    return "".join(builder)


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
