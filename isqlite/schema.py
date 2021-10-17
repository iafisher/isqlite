import collections
from typing import List, Union

import sqliteparser

from .columns import primary_key as primary_key_column
from .columns import timestamp as timestamp_column


class Table:
    """
    A class to represent a SQL table as part of a schema defined in Python.
    """

    def __init__(
        self, name: str, columns: List[Union[str, sqliteparser.ast.Column]]
    ) -> None:
        self.name = name
        self.columns = collections.OrderedDict()

        for column in columns:
            if isinstance(column, str):
                column = sqliteparser.parse_column(column)

            self.columns[column.name] = column

    @classmethod
    def from_create_table_statement(
        cls, stmt: sqliteparser.ast.CreateTableStatement
    ) -> "Table":
        return cls(stmt.name, stmt.columns)


class AutoTable(Table):
    """
    An extension of the ``Table`` class which automatically creates a primary-key column
    called ``id`` and timestamp columns called ``created_at`` and ``last_updated_at``.
    """

    def __init__(
        self, name: str, columns: List[Union[str, sqliteparser.ast.Column]]
    ) -> None:
        id_column = primary_key_column("id")
        created_at_column = timestamp_column("created_at", required=True)
        last_updated_at_column = timestamp_column("last_updated_at", required=True)
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

    def __init__(self, tables: List[Table]) -> None:
        self._tables = collections.OrderedDict((table.name, table) for table in tables)

    def __getitem__(self, key: str) -> Table:
        return self._tables[key]

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
