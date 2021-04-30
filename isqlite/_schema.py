import collections

from . import columns as isqlite_columns
from ._exception import ISQLiteError


class Schema:
    def __init__(self, tables):
        self.tables = collections.OrderedDict()
        for table in tables:
            if table.name in self.tables:
                raise ISQLiteError(f"Table {table.name!r} was defined multiple times.")

            self.tables[table.name] = table

    def create(self, db):
        sql = "\n\n".join(
            generate_create_table_statement(table.name, table.columns.values())
            for table in self.tables.values()
        )
        db.cursor.executescript(sql)

    def add_table(self, db, table):
        sql = generate_create_table_statement(table.name, table.columns.values())
        db.cursor.execute(sql)

    def drop_table(self, db, table):
        db.cursor.execute(f"DROP TABLE {table}")

    def add_column(self, db, table, column):
        db.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column}")

    def drop_column(self, db, table, column_name):
        # ALTER TABLE ... DROP COLUMN is only supported since SQLite version 3.35, so we
        # implement it by hand here.
        remaining_columns = [
            c for c in self.tables[table].columns.values() if c.name != column_name
        ]
        select = ", ".join([c.name for c in remaining_columns])
        self._migrate_table(db, table, remaining_columns, select=select)

    def reorder_columns(self, db, table, column_names):
        table_schema = self.tables[table]
        if set(column_names) != set(table_schema.columns.keys()):
            raise ISQLiteError(
                "The set of reordered columns is not the same as the set of original "
                + "columns. Note that you must include the `id`, `created_at`, and "
                + "`last_updated_at` columns in the list if your table includes them."
            )
        columns = [table_schema.columns[name] for name in column_names]
        self._migrate_table(db, table, columns, select=", ".join(column_names))

    def alter_column(self, db, table, column_name, new_column):
        table_schema = self.tables[table]
        columns = [
            column if column.name != column_name else new_column
            for column in table_schema.columns.values()
        ]
        self._migrate_table(
            db, table, columns, select=", ".join(table_schema.columns.keys())
        )

    def rename_column(self, db, table, old_column_name, new_column_name):
        # ALTER TABLE ... RENAME COLUMN is only supported since SQLite version 3.25, so
        # we implement it by hand here.
        table_schema = self.tables[table]
        column = table_schema.columns[old_column_name]
        column.name = new_column_name
        self.alter_column(db, table, old_column_name, column)

    def _migrate_table(self, db, table, columns, *, select):
        # This procedure is copied from https://sqlite.org/lang_altertable.html
        try:
            db.sql("PRAGMA foreign_keys = 0")

            # Create the new table under a temporary name.
            tmp_table_name = f"isqlite_tmp_{table}"
            db.sql(generate_create_table_statement(tmp_table_name, columns))

            # Copy over all data from the old table into the new table using the
            # provided SELECT values.
            db.sql(f"INSERT INTO {tmp_table_name} SELECT {select} FROM {table}")

            # Drop the old table.
            db.sql(f"DROP TABLE {table}")

            # Rename the new table to the original name.
            db.sql(f"ALTER TABLE {tmp_table_name} RENAME TO {table}")

            # Check that no foreign key constraints have been violated.
            db.sql("PRAGMA foreign_key_check")
        except Exception:
            db.rollback()
        else:
            db.commit()
        finally:
            db.sql("PRAGMA foreign_keys = on")


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
