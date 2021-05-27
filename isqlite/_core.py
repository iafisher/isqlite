import collections
import re
import sqlite3

import sqliteparser
from sqliteparser import quote

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

    def get(self, table, *, where="1", values={}, camel_case=False):
        return self.sql(
            f"SELECT * FROM {quote(table)} WHERE {where}",
            values,
            camel_case=camel_case,
            multiple=False,
        )

    def get_by_rowid(self, table, rowid, **kwargs):
        return self.get(
            table, where="rowid = :rowid", values={"rowid": rowid}, **kwargs
        )

    def get_or_create(self, table, data, *, camel_case=False, **kwargs):
        if not data:
            raise ISQLiteError(
                "The `data` parameter to `get_or_create` cannot be empty."
            )

        query = " AND ".join(f"{key} = :{key}" for key in data)
        row = self.get(table, where=query, values=data)
        if row is None:
            pk = self.create(table, data, **kwargs)
            return self.get_by_rowid(table, pk, camel_case=camel_case)
        else:
            return row

    def list(
        self,
        table,
        *,
        where="1",
        values={},
        camel_case=False,
        limit=None,
        order_by=None,
        descending=None,
    ):
        # TODO(2021-04-30): Use `limit` parameter.
        if order_by is not None:
            if isinstance(order_by, (tuple, list)):
                order_by = ", ".join(order_by)

            direction = "DESC" if descending is True else "ASC"
            order_clause = f"ORDER BY {quote(order_by)} {direction}"
        else:
            if descending is not None:
                raise ISQLiteError(
                    "The `descending` parameter to `list` requires the `order_by` "
                    + "parameter to be set."
                )
            order_clause = ""

        return self.sql(
            f"SELECT * FROM {quote(table)} WHERE {where} {order_clause}",
            values,
            camel_case=camel_case,
        )

    def count(self, table, *, where="1", values={}):
        result = self.sql(
            f"SELECT COUNT(*) FROM {quote(table)} WHERE {where}",
            values,
            as_tuple=True,
            multiple=False,
        )
        return result[0]

    def create(self, table, data, *, auto_timestamp=[]):
        keys = list(data.keys())
        placeholders = ",".join("?" for _ in range(len(keys)))
        values = list(data.values())

        extra = []
        for timestamp_column in auto_timestamp:
            keys.append(timestamp_column)
            extra.append(CURRENT_TIMESTAMP)

        if extra:
            extra = (", " if data else "") + ", ".join(extra)
        else:
            extra = ""

        self.cursor.execute(
            f"""
            INSERT INTO {quote(table)}({', '.join(map(quote, keys))})
            VALUES ({placeholders}{extra});
            """,
            values,
        )
        return self.cursor.lastrowid

    def create_many(self, table, data, *, auto_timestamp=[]):
        if not data:
            return

        keys = list(data[0].keys())
        placeholders = ",".join("?" for _ in range(len(keys)))

        extra = []
        for timestamp_column in auto_timestamp:
            keys.append(timestamp_column)
            extra.append(CURRENT_TIMESTAMP)

        if extra:
            extra = (", " if data else "") + ", ".join(extra)
        else:
            extra = ""

        self.cursor.executemany(
            f"""
            INSERT INTO {quote(table)}({', '.join(map(quote, keys))})
            VALUES ({placeholders}{extra});
            """,
            [tuple(d.values()) for d in data],
        )

    def update(self, table, data, *, where=None, values={}, auto_timestamp=[]):
        updates = []
        for key, value in data.items():
            if key in auto_timestamp:
                continue

            placeholder = f"v{len(values)}"
            values[placeholder] = value
            updates.append(f"{quote(key)} = :{placeholder}")

        for timestamp_column in auto_timestamp:
            updates.append(f"{quote(timestamp_column)} = {CURRENT_TIMESTAMP}")

        updates = ", ".join(updates)
        where_clause = f"WHERE {where}" if where else ""
        self.cursor.execute(
            f"UPDATE {quote(table)} SET {updates} {where_clause}", values
        )

    def update_by_rowid(self, table, rowid, data, **kwargs):
        return self.update(
            table, data, where="rowid = :rowid", values={"rowid": rowid}, **kwargs
        )

    def delete(self, table, *, where, values={}):
        self.sql(f"DELETE FROM {quote(table)} WHERE {where}", values=values)

    def delete_by_rowid(self, table, rowid, **kwargs):
        return self.delete(
            table, where="rowid = :rowid", values={"rowid": rowid}, **kwargs
        )

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

    def create_table(self, table_name, *columns, values={}):
        self.sql(f"CREATE TABLE {quote(table_name)}({','.join(columns)})", values)

    def drop_table(self, table_name):
        self.sql(f"DROP TABLE {quote(table_name)}")

    def add_column(self, table_name, column_def):
        self.sql(f"ALTER TABLE {quote(table_name)} ADD COLUMN {column_def}")

    def drop_column(self, table_name, column_name):
        # ALTER TABLE ... DROP COLUMN is only supported since SQLite version 3.35, so we
        # implement it by hand here.
        create_table_statement = self._get_create_table_statement(table_name)
        new_columns = [
            column
            for column in create_table_statement.columns
            if column.name != column_name
        ]
        if len(new_columns) == len(create_table_statement.columns):
            # TODO(2021-05-25): Test this.
            raise ISQLiteError(f"{column_name} is not a column of {table_name}.")

        create_table_statement.columns = new_columns
        select = ", ".join(c.name for c in create_table_statement.columns)
        self._migrate_table(create_table_statement, select=select)

    def reorder_columns(self, table_name, column_names):
        create_table_statement = self._get_create_table_statement(table_name)
        column_map = collections.OrderedDict(
            (c.name, c) for c in create_table_statement.columns
        )
        if set(column_names) != set(column_map.keys()):
            raise ISQLiteError(
                "The set of reordered columns is not the same as the set of original "
                + "columns."
            )
        create_table_statement.columns = [column_map[name] for name in column_names]
        self._migrate_table(create_table_statement, select=", ".join(column_names))

    def alter_column(self, table_name, column_name, new_column):
        create_table_statement = self._get_create_table_statement(table_name)
        altered = False
        for i, column in enumerate(create_table_statement.columns):
            if column.name == column_name:
                # TODO(2021-05-25): Clean up this hacky logic. A better way would be to
                # call sqliteparser to parse `new_column` into a real `ast.Column`
                # object. But currently sqliteparser does not have a public API method
                # to do so.
                create_table_statement.columns[i] = sqliteparser.ast.Column(
                    column_name, constraints=[new_column]
                )
                altered = True
                break

        if not altered:
            # TODO(2021-05-25): Test this.
            raise ISQLiteError(f"{column_name} is not a column of {table_name}.")

        self._migrate_table(
            create_table_statement,
            select=", ".join(c.name for c in create_table_statement.columns),
        )

    def rename_column(self, table_name, old_column_name, new_column_name):
        # ALTER TABLE ... RENAME COLUMN is only supported since SQLite version 3.25, so
        # we implement it by hand here.
        raise NotImplementedError

    def _migrate_table(self, new_create_table_statement, *, select):
        name = new_create_table_statement.name
        # This procedure is copied from https://sqlite.org/lang_altertable.html
        try:
            self.sql("PRAGMA foreign_keys = 0")

            # Create the new table under a temporary name.
            tmp_table_name = quote(f"isqlite_tmp_{name}")
            new_create_table_statement.name = tmp_table_name
            self.sql(str(new_create_table_statement))

            # Copy over all data from the old table into the new table using the
            # provided SELECT values.
            self.sql(f"INSERT INTO {tmp_table_name} SELECT {select} FROM {quote(name)}")

            # Drop the old table.
            self.sql(f"DROP TABLE {quote(name)}")

            # Rename the new table to the original name.
            self.sql(f"ALTER TABLE {tmp_table_name} RENAME TO {quote(name)}")

            # Check that no foreign key constraints have been violated.
            self.sql("PRAGMA foreign_key_check")
        except Exception as e:
            self.rollback()
            raise e
        else:
            self.commit()
        finally:
            self.sql("PRAGMA foreign_keys = 1")

    def _get_create_table_statement(self, table_name):
        sql = self.sql(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :table",
            {"table": table_name},
            as_tuple=True,
            multiple=False,
        )[0]

        if not sql.endswith(";"):
            sql = sql + ";"

        return sqliteparser.parse(sql)[0]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


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
