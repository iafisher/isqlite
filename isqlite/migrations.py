from abc import ABC
from typing import List

from attr import attrs


class MigrateOperation(ABC):
    pass


@attrs(auto_attribs=True)
class CreateTableMigration(MigrateOperation):
    table_name: str
    columns: List[str]

    def __str__(self):
        return f"Create table {self.table_name}"


@attrs(auto_attribs=True)
class DropTableMigration(MigrateOperation):
    table_name: str

    def __str__(self):
        return f"Drop table {self.table_name}"


@attrs(auto_attribs=True)
class AddColumnMigration(MigrateOperation):
    table_name: str
    column: str

    def __str__(self):
        return f"Add column: {self.column}"


@attrs(auto_attribs=True)
class AlterColumnMigration(MigrateOperation):
    table_name: str
    column_name: str
    column_definition: str

    def __str__(self):
        return f"Alter column: {self.column_name} {self.column_definition}"


@attrs(auto_attribs=True)
class DropColumnMigration(MigrateOperation):
    table_name: str
    column_name: str

    def __str__(self):
        return f"Drop column {self.column_name}"


@attrs(auto_attribs=True)
class RenameColumnMigration(MigrateOperation):
    table_name: str
    old_column_name: str
    new_column_name: str

    def __str__(self):
        return f"Rename column: {self.old_column_name} => {self.new_column_name}"


@attrs(auto_attribs=True)
class ReorderColumnsMigration(MigrateOperation):
    table_name: str
    column_names: List[str]

    def __str__(self):
        return f"Reorder columns: {', '.join(self.column_names)}"
