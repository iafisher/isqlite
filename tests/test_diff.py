import unittest

from isqlite import AutoTable, Schema, Table, columns
from isqlite.migrations import (
    AddColumnMigration,
    AlterColumnMigration,
    CreateTableMigration,
    DropColumnMigration,
    DropTableMigration,
    RenameColumnMigration,
    ReorderColumnsMigration,
)
from isqlite.schema import diff_schemas, diff_tables


class DiffTests(unittest.TestCase):
    def test_diff_column_renamed(self):
        table_before = Table(
            "employees",
            [
                columns.text("name", required=True),
            ],
        )
        table_after = Table(
            "employees",
            [
                columns.text("legal_name", required=True),
            ],
        )

        diff = diff_tables(table_before, table_after)

        self.assertEqual(
            diff, [RenameColumnMigration("employees", "name", "legal_name")]
        )

    def test_diff_column_renamed_with_two_possible_candidates(self):
        table_before = Table(
            "employees",
            [
                columns.text("first_name", required=True),
                columns.text("last_name", required=True),
            ],
        )
        table_after = Table(
            "employees",
            [
                columns.text("legal_name", required=True),
            ],
        )

        diff = diff_tables(table_before, table_after)

        self.assertEqual(
            diff,
            [
                RenameColumnMigration("employees", "first_name", "legal_name"),
                DropColumnMigration("employees", "last_name"),
            ],
        )

    def test_diff_column_added(self):
        table_before = AutoTable(
            "events",
            [
                "start DATE",
            ],
        )
        table_after = AutoTable(
            "events",
            [
                "start DATE",
                "end DATE",
            ],
        )

        diff = diff_tables(table_before, table_after)

        self.assertEqual(
            diff,
            [
                AddColumnMigration("events", '"end"  DATE'),
                ReorderColumnsMigration(
                    "events",
                    ["id", "start", "end", "created_at", "last_updated_at"],
                ),
            ],
        )

    def test_diff_column_altered(self):
        table_before = AutoTable(
            "t",
            [
                columns.date("start", required=False),
            ],
        )
        table_after = AutoTable(
            "t",
            [
                columns.date("start", required=True),
            ],
        )

        diff = diff_tables(table_before, table_after)

        self.assertEqual(
            diff,
            [
                AlterColumnMigration("t", "start", " DATE NOT NULL"),
            ],
        )

    def test_diff_column_dropped(self):
        table_before = AutoTable(
            "t",
            [
                columns.text("title"),
                columns.text("description"),
            ],
        )
        table_after = AutoTable(
            "t",
            [
                columns.text("title"),
            ],
        )

        diff = diff_tables(table_before, table_after)

        self.assertEqual(
            diff,
            [
                DropColumnMigration("t", "description"),
            ],
        )

    def test_diff_columns_reordered(self):
        table_before = AutoTable(
            "t",
            [
                columns.text("title"),
                columns.text("description"),
                columns.date("start"),
            ],
        )
        table_after = AutoTable(
            "t",
            [
                columns.date("start"),
                columns.text("description"),
                columns.text("title"),
            ],
        )

        diff = diff_tables(table_before, table_after)

        self.assertEqual(
            diff,
            [
                ReorderColumnsMigration(
                    "t",
                    [
                        "id",
                        "start",
                        "description",
                        "title",
                        "created_at",
                        "last_updated_at",
                    ],
                )
            ],
        )

    def test_diff_table_dropped(self):
        schema_before = Schema([Table("x", ["bar TEXT"]), Table("y", ["foo TEXT"])])
        schema_after = Schema([Table("y", ["foo TEXT"])])

        diff = diff_schemas(schema_before, schema_after)

        self.assertEqual(
            diff,
            [
                DropTableMigration("x"),
            ],
        )

    def test_diff_table_created(self):
        schema_before = Schema([Table("y", ["foo TEXT"])])
        schema_after = Schema([Table("x", ["bar TEXT"]), Table("y", ["foo TEXT"])])

        diff = diff_schemas(schema_before, schema_after)

        self.assertEqual(
            diff,
            [
                CreateTableMigration("x", ['"bar"  TEXT']),
            ],
        )
