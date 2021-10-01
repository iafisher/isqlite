import collections
import decimal
import re
import sqlite3
import textwrap
from abc import ABC

import sqliteparser
from attr import attrib, attrs
from sqliteparser import ast, quote

from .utils import StringBuilder, snake_case

CURRENT_TIMESTAMP = "STRFTIME('%Y-%m-%d %H:%M:%f000+00:00', 'now')"
AUTO_TIMESTAMP = ("created_at", "last_updated_at")
AUTO_TIMESTAMP_UPDATE_ONLY = ("last_updated_at",)


class Database:
    def __init__(
        self,
        connection_or_path,
        *,
        transaction=True,
        debugger=None,
        readonly=None,
        uri=False,
        schema_module=None,
    ):
        # Validate arguments.
        if readonly is not None:
            if uri is True:
                raise ISqliteApiError(
                    "The `readonly` parameter cannot be set if `uri` is False. Append "
                    + "'?mode=ro' (or omit it if you don't want your connection to be "
                    + "read-only) to your URI instead."
                )

            if not isinstance(connection_or_path, str):
                if readonly is not None:
                    raise ISqliteApiError(
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

            # Setting `isolation_level` to None disables quirky behavior around
            # transactions, per https://stackoverflow.com/questions/30760997/
            self.connection = sqlite3.connect(
                path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                uri=True,
                isolation_level=None,
            )
        else:
            self.connection = connection_or_path

        if debugger is True:
            debugger = PrintDebugger()
        self.debugger = debugger

        self.schema = (
            schema_module_to_dict(schema_module) if schema_module is not None else None
        )

        self.connection.row_factory = ordered_dict_row_factory
        self.cursor = self.connection.cursor()

        # This must be executed outside a transaction, according to the official
        # SQLite docs: https://sqlite.org/pragma.html#pragma_foreign_keys
        self.sql("PRAGMA foreign_keys = 1")
        if transaction:
            self.sql("BEGIN")

    def get(self, table, *, where=None, values={}, get_related=[]):
        where_clause = f"WHERE {where}" if where else ""

        if get_related:
            if self.schema is None:
                raise ISqliteApiError(
                    "get_related requires that the database was initialized with a "
                    + "schema."
                )

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

    def get_by_rowid(self, table, rowid, **kwargs):
        return self.get(
            table, where="rowid = :rowid", values={"rowid": rowid}, **kwargs
        )

    def get_or_create(self, table, data, **kwargs):
        if not data:
            raise ISqliteError(
                "The `data` parameter to `get_or_create` cannot be empty."
            )

        query = " AND ".join(f"{key} = :{key}" for key in data)
        row = self.get(table, where=query, values=data)
        if row is None:
            pk = self.create(table, data, **kwargs)
            return self.get_by_rowid(table, pk)
        else:
            return row

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
            if self.schema is None:
                raise ISqliteApiError(
                    "get_related requires that the database was initialized with a "
                    + "schema."
                )

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

    def count(self, table, *, where=None, values={}):
        where_clause = f"WHERE {where}" if where else ""
        result = self.sql(
            f"SELECT COUNT(*) FROM {quote(table)} {where_clause}",
            values,
            as_tuple=True,
            multiple=False,
        )
        return result[0]

    def create(self, table, data, *, auto_timestamp=AUTO_TIMESTAMP):
        if auto_timestamp is None:
            auto_timestamp = []

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

        sql = f"""
        INSERT INTO {quote(table)}({', '.join(map(quote, keys))})
        VALUES ({placeholders}{extra});
        """
        if self.debugger:
            self.debugger.execute(sql, values)
        self.cursor.execute(sql, values)
        return self.cursor.lastrowid

    def create_many(self, table, data, *, auto_timestamp=AUTO_TIMESTAMP):
        if auto_timestamp is None:
            auto_timestamp = []

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
        auto_timestamp=AUTO_TIMESTAMP_UPDATE_ONLY,
    ):
        if auto_timestamp is None:
            auto_timestamp = []

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
        sql = f"UPDATE {quote(table)} SET {updates} {where_clause}"
        if self.debugger:
            self.debugger.execute(sql, values)
        self.cursor.execute(sql, values)

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

    def sql(self, query, values={}, *, as_tuple=False, multiple=True):
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

    def transaction(self):
        return TransactionContextManager(self)

    def begin_transaction(self):
        self.sql("BEGIN")

    def commit(self):
        self.sql("COMMIT")

    def rollback(self):
        self.sql("ROLLBACK")

    @property
    def in_transaction(self):
        return self.connection.in_transaction

    def close(self):
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

    def _get_related_columns_and_joins(self, table, get_related):
        table_schema = self.schema[table]
        if get_related is True:
            get_related = {
                column.name
                for column in table_schema.columns.values()
                if isinstance(column, ForeignKeyColumn)
                # Don't fetch recursive relations because this will cause 'ambiguous
                # column' errors in the SQL query.
                and column.model != table
            }
        else:
            get_related = set(get_related)

        columns = []
        joins = []
        for column in table_schema.columns.values():
            if column.name in get_related:
                # Remove the column from the set so that we can check for any
                # non-existent columns at the end.
                get_related.remove(column.name)

                if not isinstance(column, ForeignKeyColumn):
                    raise ISqliteError(
                        f"{column.name!r} was passed in `get_related`, "
                        + "but it is not a foreign key column"
                    )

                related_table_schema = self.schema[column.model]
                for related_column in related_table_schema.columns.values():
                    name = f"{column.name}____{related_column.name}"
                    columns.append(
                        f"{quote(column.model)}.{quote(related_column.name)} "
                        + f"AS {quote(name)}"
                    )

                joins.append((column.name, column.model))
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


class TransactionContextManager:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.begin_transaction()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is not None:
            self.db.rollback()
        else:
            self.db.commit()


class DatabaseMigrator:
    def __init__(self, connection_or_db, *args, **kwargs):
        if isinstance(connection_or_db, Database):
            self.db = connection_or_db
        else:
            self.db = Database(connection_or_db, *args, transaction=False, **kwargs)

        self.schema = self.db.schema

    def diff(self, table=None):
        tables_in_db = {
            row["name"]: row["sql"]
            for row in self.db.list(
                "sqlite_master", where="type = 'table' AND NOT name LIKE 'sqlite_%'"
            )
        }
        tables_in_schema = (
            self.schema.values() if table is None else [self.schema[table]]
        )

        diff = collections.defaultdict(list)
        for table_in_schema in tables_in_schema:
            name = table_in_schema.name
            if name in tables_in_db:
                sql = tables_in_db.pop(table_in_schema.name)
                columns_in_database = sqliteparser.parse(sql)[0].columns
                columns_in_schema = [
                    column.as_sql() for column in table_in_schema.columns.values()
                ]
                self._diff_table(diff, name, columns_in_database, columns_in_schema)
            else:
                diff[table_in_schema.name].append(
                    CreateTableMigration(
                        table_in_schema.name,
                        [
                            str(column.as_sql())
                            for column in table_in_schema.columns.values()
                        ],
                    )
                )

        if table is None:
            for name in tables_in_db:
                diff[name].append(DropTableMigration(name))

        return diff

    def _diff_table(self, diff, table_name, columns_in_database, columns_in_schema):
        columns_in_database_map = {
            column.name: i for i, column in enumerate(columns_in_database)
        }
        renamed_columns = set()
        reordered = False
        for new_index, column in enumerate(columns_in_schema):
            old_index = columns_in_database_map.get(column.name)
            if old_index is None:
                # TODO(#567): Re-enable this.
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
                diff[table_name].append(AddColumnMigration(table_name, column))
                continue

            if old_index != new_index:
                reordered = True

            old_column = columns_in_database[old_index]
            if old_column != column:
                diff[table_name].append(AlterColumnMigration(table_name, column))

        columns_in_schema_map = {
            column.name: i for i, column in enumerate(columns_in_schema)
        }
        for column in columns_in_database:
            if (
                column.name not in columns_in_schema_map
                and column.name not in renamed_columns
            ):
                diff[table_name].append(DropColumnMigration(table_name, column.name))

        if reordered:
            diff[table_name].append(
                ReorderColumnsMigration(
                    table_name, [column.name for column in columns_in_schema]
                )
            )

        return diff

    def apply_diff(self, diff):
        for table_diff in diff.values():
            for op in table_diff:
                if isinstance(op, CreateTableMigration):
                    self.create_table(op.table_name, op.columns)
                elif isinstance(op, DropTableMigration):
                    self.drop_table(op.table_name)
                elif isinstance(op, AlterColumnMigration):
                    self.alter_column(
                        op.table_name,
                        op.column.name,
                        str(op.column.definition)
                        if op.column.definition is not None
                        else "",
                    )
                elif isinstance(op, AddColumnMigration):
                    self.add_column(op.table_name, op.column)
                elif isinstance(op, DropColumnMigration):
                    self.drop_column(op.table_name, op.column_name)
                elif isinstance(op, ReorderColumnsMigration):
                    self.reorder_columns(op.table_name, op.column_names)
                elif isinstance(op, RenameColumnMigration):
                    self.rename_column(
                        op.table_name, op.old_column_name, op.new_column_name
                    )
                else:
                    raise ISqliteError("unknown migration op type")

    def begin(self):
        # We disable foreign keys before the SAVEPOINT statement because, per the
        # SQLite docs:
        #
        #  foreign key constraint enforcement may only be enabled or disabled when
        #  there is no pending BEGIN or SAVEPOINT
        #
        # Source: https://sqlite.org/pragma.html#pragma_foreign_keys
        if self.db.in_transaction:
            raise Exception(
                "DatabaseMigrator cannot begin while database is in a transaction. "
                + "Make sure there are no pending BEGIN or SAVEPOINT statements."
            )

        self.db.sql("PRAGMA foreign_keys = 0")
        self.db.sql("BEGIN")

    def commit(self):
        if self.db.in_transaction:
            self.db.commit()
        self.db.sql("PRAGMA foreign_keys = 1")

    def rollback(self):
        if self.db.in_transaction:
            self.db.rollback()
        self.db.sql("PRAGMA foreign_keys = 1")

    def create_table(self, table_name, columns):
        if isinstance(columns, str):
            raise ISqliteApiError(
                "second argument to DatabaseMigrator.create_table must be a list, "
                + "not a string"
            )

        self.db.sql(f"CREATE TABLE {quote(table_name)}({','.join(map(str, columns))})")

    def drop_table(self, table_name):
        self.db.sql(f"DROP TABLE {quote(table_name)}")

    def rename_table(self, old_table_name, new_table_name):
        self.db.sql(
            f"ALTER TABLE {quote(old_table_name)} RENAME TO {quote(new_table_name)}"
        )

    def add_column(self, table_name, column_def):
        self.db.sql(f"ALTER TABLE {quote(table_name)} ADD COLUMN {column_def}")

    def drop_column(self, table_name, column_name):
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

    def reorder_columns(self, table_name, column_names):
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

    def alter_column(self, table_name, column_name, new_column):
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

    def rename_column(self, table_name, old_column_name, new_column_name):
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

    def _migrate_table(self, name, columns, *, select):
        # This procedure is copied from https://sqlite.org/lang_altertable.html
        # Create the new table under a temporary name.
        tmp_table_name = quote(f"isqlite_tmp_{name}")
        self.db.sql(f"CREATE TABLE {tmp_table_name}({', '.join(columns)})")

        # Copy over all data from the old table into the new table using the
        # provided SELECT values.
        self.db.sql(f"INSERT INTO {tmp_table_name} SELECT {select} FROM {quote(name)}")

        # Drop the old table.
        self.db.sql(f"DROP TABLE {quote(name)}")

        # Rename the new table to the original name.
        self.db.sql(f"ALTER TABLE {tmp_table_name} RENAME TO {quote(name)}")

        # Check that no foreign key constraints have been violated.
        self.db.sql("PRAGMA foreign_key_check")

    def _get_create_table_statement(self, table_name):
        sql = self.db.sql(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :table",
            {"table": table_name},
            as_tuple=True,
            multiple=False,
        )[0]
        return sqliteparser.parse(sql)[0]

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

        self.db.close()


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


class BaseColumn(ABC):
    def __init__(
        self, name, *, required=False, choices=[], default=None, sql_constraints=[]
    ):
        self.name = name
        self.required = required
        self.choices = choices[:]
        self.default = default
        self.sql_constraints = sql_constraints[:]

    def validate(self, v):
        if v == "":
            if self.default is not None:
                return self.default, True
            elif self.required:
                return None, False
            else:
                return None, True

        if self.choices:
            return v, v in self.choices

        return self._validate(v)

    def _validate(self, v):
        return v, True

    def as_sql(self):
        constraints = []
        if self.required:
            constraints.append(not_null_constraint())

        if self.choices:
            if self.required:
                constraints.append(ast.CheckConstraint(self._choices_as_sql()))
            else:
                constraints.append(
                    ast.CheckConstraint(
                        ast.Infix(
                            "OR",
                            ast.Infix("IS", ast.Identifier(self.name), ast.Null()),
                            self._choices_as_sql(),
                        )
                    )
                )

        constraints.extend(self.sql_constraints)
        return ast.Column(
            name=self.name,
            definition=ast.ColumnDefinition(
                type=self.type,
                default=convert_default(self.default),
                constraints=constraints,
            ),
        )

    def _choices_as_sql(self):
        return ast.Infix(
            "IN",
            ast.Identifier(self.name),
            ast.ExpressionList([convert_default(choice) for choice in self.choices]),
        )

    def __str__(self):
        return str(self.as_sql())

    def description(self):
        sb = StringBuilder()
        sb.text(self.name)
        sb.text(" (")
        sb.text(self.type.lower())
        if not self.required:
            sb.text(", optional")
        if self.choices:
            sb.text(f", choices = [{', '.join(map(repr, self.choices))}]")
        sb.text(self._extra_description())
        if self.default is not None and self.default != "":
            sb.text(f", default = {self.default!r}")
        sb.text(")")
        return sb.build()

    def _extra_description(self):
        return ""


class TextColumn(BaseColumn):
    type = "TEXT"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.required and self.default is None:
            self.default = ""

    def validate(self, *args, **kwargs):
        # Normally, subclasses of BaseColumn will override _validate instead. But
        # TextColumn needs to make sure that validate _always_ returns "" instead of
        # None, so it needs to override the default validate method instead.
        v, is_valid = super().validate(*args, **kwargs)
        if v is None and is_valid:
            return "", True
        else:
            return v, is_valid

    def as_sql(self):
        constraints = [not_null_constraint()]
        if self.required:
            constraints.append(non_empty_constraint(self.name))

        if self.choices:
            if self.required:
                constraints.append(ast.CheckConstraint(self._choices_as_sql()))
            else:
                constraints.append(
                    ast.CheckConstraint(
                        ast.Infix(
                            "OR",
                            ast.Infix("=", ast.Identifier(self.name), ast.String("")),
                            self._choices_as_sql(),
                        )
                    )
                )

        constraints.extend(self.sql_constraints)
        return ast.Column(
            name=self.name,
            definition=ast.ColumnDefinition(
                type=self.type,
                default=convert_default(self.default),
                constraints=constraints,
            ),
        )


class IntegerColumn(BaseColumn):
    type = "INTEGER"

    def __init__(self, *args, max=None, min=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.max = max
        self.min = min
        if self.max is not None:
            self.sql_constraints.append(
                check_operator_constraint(self.name, "<=", ast.Integer(self.max))
            )
        if self.min is not None:
            self.sql_constraints.append(
                check_operator_constraint(self.name, ">=", ast.Integer(self.min))
            )

    def _validate(self, v):
        try:
            v = int(v)
        except ValueError:
            return None, False

        if self.min is not None and v < self.min:
            return None, False

        if self.max is not None and v > self.max:
            return None, False

        return v, True

    def _extra_description(self):
        sb = StringBuilder()
        if self.min is not None:
            sb.text(f", min = {self.min}")
        if self.max is not None:
            sb.text(f", max = {self.max}")
        return sb.build()


class BooleanColumn(BaseColumn):
    type = "BOOLEAN"

    def _validate(self, v):
        if v == "1" or v.lower() == "true":
            return True, True
        elif v == "0" or v.lower() == "false":
            return False, True
        else:
            return None, False


class DateColumn(BaseColumn):
    type = "DATE"
    date_pattern = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

    def _validate(self, v):
        if self.date_pattern.match(v):
            return v, True
        else:
            return None, False


class TimestampColumn(BaseColumn):
    type = "TIMESTAMP"
    pattern = re.compile(
        r"^[0-9]{4}-[0-9]{2}-[0-9]{2} "
        + r"[0-9]{1,2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}\+[0-9]{2}:[0-9]{2}$"
    )

    def _validate(self, v):
        if self.pattern.match(v):
            return v, True
        else:
            return None, False


class TimeColumn(BaseColumn):
    type = "TIME"
    pattern = re.compile(r"^[0-9]{1,2}:[0-9]{2}$")

    def _validate(self, v):
        if self.pattern.match(v):
            return v, True
        else:
            return None, False


class DecimalColumn(BaseColumn):
    type = "DECIMAL"

    def _validate(self, v):
        try:
            return decimal.Decimal(v), True
        except decimal.InvalidOperation:
            return None, False


class ForeignKeyColumn(BaseColumn):
    type = "INTEGER"

    def __init__(self, *args, model, on_delete=ast.OnDelete.SET_NULL, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = model
        self.sql_constraints.append(
            ast.ForeignKeyConstraint(
                columns=[],
                foreign_table=self.model,
                foreign_columns=[],
                on_delete=on_delete,
            )
        )

    def _validate(self, v):
        try:
            return int(v), True
        except ValueError:
            return None, False

    def _extra_description(self):
        return f", foreign key = {self.model}"


class ColumnStub:
    def __init__(self, cls, args, kwargs):
        self.cls = cls
        self.args = args
        self.kwargs = kwargs


def make_column_stub_factory(cls):
    return lambda *args, **kwargs: ColumnStub(cls, args, kwargs)


BooleanColumnStub = make_column_stub_factory(BooleanColumn)
DateColumnStub = make_column_stub_factory(DateColumn)
DecimalColumnStub = make_column_stub_factory(DecimalColumn)
ForeignKeyColumnStub = make_column_stub_factory(ForeignKeyColumn)
IntegerColumnStub = make_column_stub_factory(IntegerColumn)
TextColumnStub = make_column_stub_factory(TextColumn)
TimeColumnStub = make_column_stub_factory(TimeColumn)
TimestampColumnStub = make_column_stub_factory(TimestampColumn)


def not_null_constraint():
    return ast.NotNullConstraint()


def non_empty_constraint(name):
    return check_operator_constraint(name, "!=", ast.String(""))


def check_operator_constraint(name, operator, value):
    return ast.CheckConstraint(
        expr=ast.Infix(operator=operator, left=ast.Identifier(name), right=value)
    )


class TableMeta(type):
    def __new__(cls, name, bases, dct):
        x = super().__new__(cls, name, bases, dct)

        columns = collections.OrderedDict()
        columns["id"] = IntegerColumn(
            "id",
            required=True,
            sql_constraints=[ast.PrimaryKeyConstraint(autoincrement=True)],
        )
        to_delete = []
        for key, value in dct.items():
            if isinstance(value, ColumnStub):
                columns[key] = value.cls(key, *value.args, **value.kwargs)
                to_delete.append(key)

        for key in to_delete:
            delattr(x, key)

        columns["created_at"] = TimestampColumn("created_at", required=True)
        columns["last_updated_at"] = TimestampColumn("last_updated_at", required=True)

        x.name = snake_case(x.__name__)
        x.columns = columns
        return x


class Table(metaclass=TableMeta):
    def __init__(self):
        raise Exception("Subclasses of Table cannot be instantiated.")

    @classmethod
    def as_string(cls, row):
        return str(row["id"])


def schema_module_to_dict(schema_module):
    return {
        value.name: value
        for value in schema_module.__dict__.values()
        if isinstance(value, type) and issubclass(value, Table) and value is not Table
    }


def convert_default(default):
    if default is not None:
        if isinstance(default, str):
            return ast.String(default)
        elif isinstance(default, bool):
            return ast.Integer(int(default))
        elif isinstance(default, int):
            return ast.Integer(default)

    return default


@attrs
class CreateTableMigration:
    table_name = attrib()
    columns = attrib(factory=list)

    def __str__(self):
        return f"Create table {self.table_name}"


@attrs
class DropTableMigration:
    table_name = attrib()

    def __str__(self):
        return f"Drop table {self.table_name}"


@attrs
class AlterColumnMigration:
    table_name = attrib()
    column = attrib()

    def __str__(self):
        return f"Alter column: {self.column}"


@attrs
class AddColumnMigration:
    table_name = attrib()
    column = attrib()

    def __str__(self):
        return f"Add column: {self.column}"


@attrs
class DropColumnMigration:
    table_name = attrib()
    column_name = attrib()

    def __str__(self):
        return f"Drop column {self.column_name}"


@attrs
class ReorderColumnsMigration:
    table_name = attrib()
    column_names = attrib()

    def __str__(self):
        return f"Reorder columns: {', '.join(self.column_names)}"


@attrs
class RenameColumnMigration:
    table_name = attrib()
    old_column_name = attrib()
    new_column_name = attrib()

    def __str__(self):
        return f"Rename column: {self.old_column_name} => {self.new_column_name}"


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
