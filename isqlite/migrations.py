from attr import attrib, attrs


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


@attrs
class CreateTableMigration:
    table_name = attrib()
    columns = attrib(factory=list)

    def __str__(self):
        return f"Create table {self.table_name}"
