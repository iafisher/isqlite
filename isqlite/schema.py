import collections
from typing import Dict, List, Union

import attr
import sqliteparser

from . import migrations
from .columns import integer as integer_column
from .columns import primary_key as primary_key_column
from .columns import timestamp as timestamp_column
from .exceptions import ColumnDoesNotExistError, TableDoesNotExistError

# Type alias
Diff = List[migrations.MigrateOperation]


class Table:
    """
    A class to represent a SQL table as part of a schema defined in Python.
    """

    name: str
    _columns: Dict[str, sqliteparser.ast.Column]

    def __init__(
        self, name: str, columns: List[Union[str, sqliteparser.ast.Column]]
    ) -> None:
        self.name = name
        self._columns = collections.OrderedDict()

        for column in columns:
            if isinstance(column, str):
                column = sqliteparser.parse_column(column)

            self._columns[column.name] = column

    @classmethod
    def from_create_table_statement(
        cls, stmt: sqliteparser.ast.CreateTableStatement
    ) -> "Table":
        return cls(stmt.name, stmt.columns)

    def __getitem__(self, key: str) -> sqliteparser.ast.Column:
        try:
            return self._columns[key]
        except KeyError:
            raise ColumnDoesNotExistError(key)

    def __contains__(self, key: str) -> bool:
        return key in self._columns

    @property
    def columns(self) -> List[sqliteparser.ast.Column]:
        """
        Returns the columns in the table as a list.
        """
        return list(self._columns.values())


class AutoTable(Table):
    """
    An extension of the ``Table`` class which automatically creates a primary-key column
    called ``id`` and timestamp columns called ``created_at`` and ``last_updated_at``.
    """

    def __init__(
        self,
        name: str,
        columns: List[Union[str, sqliteparser.ast.Column]],
        use_epoch_timestamps: bool = False,
    ) -> None:
        """
        Initialize an ``AutoTable`` object.

        :param name: The name of the table.
        :param columns: The ordered list of columns of the table.
        :param use_epoch_timestamps: Store the ``created_at`` and ``last_updated_at`` as
          seconds since the Unix epoch instead of as ISO 8601 datetime strings.
          Recommended setting is ``True``, but default is ``False`` for backwards
          compatibility.
        """
        cls = integer_column if use_epoch_timestamps else timestamp_column

        id_column = primary_key_column("id")
        created_at_column = cls("created_at", required=True)  # type: ignore
        last_updated_at_column = cls("last_updated_at", required=True)  # type: ignore
        columns = [id_column] + columns + [created_at_column, last_updated_at_column]
        super().__init__(name, columns)

    @classmethod
    def from_create_table_statement(
        cls, stmt: sqliteparser.ast.CreateTableStatement
    ) -> "Table":
        raise NotImplementedError


class Schema:
    """
    A class to represent an entire database schema.
    """

    _tables: Dict[str, Table]

    def __init__(self, tables: List[Table]) -> None:
        self._tables = collections.OrderedDict((table.name, table) for table in tables)

    def __getitem__(self, key: str) -> Table:
        try:
            return self._tables[key]
        except KeyError:
            raise TableDoesNotExistError(key)

    def __contains__(self, key: str) -> bool:
        return key in self._tables

    @property
    def tables(self) -> List[Table]:
        """
        Returns the tables in the schema as a list.
        """
        return list(self._tables.values())

    @property
    def table_names(self) -> List[str]:
        """
        Returns the names of the tables in the schema as a list.
        """
        return list(self._tables.keys())


def diff_schemas(
    old_schema: Schema, new_schema: Schema, *, detect_renaming=True
) -> Diff:
    tables_in_old_schema = set(old_schema.table_names)
    tables_in_new_schema = set(new_schema.table_names)

    diff: Diff = []

    tables_to_create = tables_in_new_schema - tables_in_old_schema
    for table_name in tables_to_create:
        table = new_schema[table_name]
        diff.append(
            migrations.CreateTableMigration(
                table_name,
                [str(column) for column in table.columns],
            )
        )

    tables_to_drop = tables_in_old_schema - tables_in_new_schema
    for table_name in tables_to_drop:
        diff.append(migrations.DropTableMigration(table_name))

    tables_to_alter = tables_in_old_schema & tables_in_new_schema
    for table_name in tables_to_alter:
        old_table = old_schema[table_name]
        new_table = new_schema[table_name]
        diff.extend(diff_tables(old_table, new_table, detect_renaming=detect_renaming))

    return diff


def diff_tables(old_table: Table, new_table: Table, *, detect_renaming=True) -> Diff:
    # TODO(2021-10-17): Clean up this implementation, similar to `diff_schemas`.
    table_name = new_table.name
    diff: Diff = []

    old_columns_to_index_map = {
        column.name: i for i, column in enumerate(old_table.columns)
    }
    renamed_columns = set()
    reordered = False
    for new_index, column in enumerate(new_table.columns):
        old_index = old_columns_to_index_map.get(column.name)
        if old_index is None:
            if (
                # Renaming detection must be on...
                detect_renaming
                # ...the index must be valid in the old table...
                and new_index < len(old_table.columns)
                # ...the column at the index in the old table must be gone...
                and old_table.columns[new_index].name not in new_table
                # ...and the old and new columns at that index must otherwise
                # correspond.
                and is_renamed_column(column, old_table.columns[new_index])
            ):
                old_column_name = old_table.columns[new_index].name
                renamed_columns.add(old_column_name)
                diff.append(
                    migrations.RenameColumnMigration(
                        table_name, old_column_name, column.name
                    )
                )
            else:
                diff.append(migrations.AddColumnMigration(table_name, str(column)))
            continue

        if old_index != new_index:
            reordered = True

        old_column = old_table.columns[old_index]
        if old_column != column:
            diff.append(
                migrations.AlterColumnMigration(
                    table_name, column.name, str(column.definition)
                )
            )

    new_columns_to_index_map = {
        column.name: i for i, column in enumerate(new_table.columns)
    }
    dropped_columns = set()
    for column in old_table.columns:
        if (
            column.name not in new_columns_to_index_map
            and column.name not in renamed_columns
        ):
            dropped_columns.add(column.name)
            diff.append(migrations.DropColumnMigration(table_name, column.name))

    if reordered:
        reordered_columns = [column.name for column in new_table.columns]
        old_columns_except_dropped = [
            column.name
            for column in old_table.columns
            if column.name not in dropped_columns
        ]
        if reordered_columns != old_columns_except_dropped:
            diff.append(
                migrations.ReorderColumnsMigration(table_name, reordered_columns)
            )

    return diff


def is_renamed_column(
    column_in_schema: sqliteparser.ast.Column,
    column_in_database: sqliteparser.ast.Column,
) -> bool:
    return column_in_schema == rename_column(
        column_in_database, column_in_database.name, column_in_schema.name
    )


def rename_column(
    column: sqliteparser.ast.Column,
    old_name: str,
    new_name: str,
) -> sqliteparser.ast.ColumnDefinition:
    renamer = ColumnRenamer(old_name, new_name)
    return renamer.rename(column)


class ColumnRenamer:
    """
    A class implementing the visitor pattern which renames all instances of a column's
    name in the column's definition.
    """

    def __init__(self, old_name, new_name):
        self.old_name = old_name
        self.new_name = new_name

    def rename(self, node):
        if node is None:
            return None

        return node.accept(self)

    def visit_column(self, node):
        return attr.evolve(
            node,
            name=self.new_name if node.name == self.old_name else node.name,
            definition=self.rename(node.definition),
        )

    def visit_column_definition(self, node):
        return attr.evolve(node, constraints=list(map(self.rename, node.constraints)))

    def visit_check_constraint(self, node):
        return attr.evolve(node, expr=self.rename(node.expr))

    def visit_named_constraint(self, node):
        return attr.evolve(node, constraint=self.rename(node.constraint))

    def visit_foreign_key_constraint(self, node):
        columns = [
            self.new_name if column == self.old_name else column
            for column in node.columns
        ]
        return attr.evolve(node, columns=columns)

    def visit_generated_column_constraint(self, node):
        return attr.evolve(node, expression=self.rename(node.expression))

    def visit_infix(self, node):
        return attr.evolve(
            node, left=self.rename(node.left), right=self.rename(node.right)
        )

    def visit_expression_list(self, node):
        return attr.evolve(node, values=list(map(self.rename, node.values)))

    def visit_identifier(self, node):
        if node.value == self.old_name:
            return sqliteparser.ast.Identifier(self.new_name)
        else:
            return node

    def visit_default(self, node):
        return node
