import unittest

from isqlite import AutoTable, Table, columns
from isqlite.migrations import (
    AddColumnMigration,
    RenameColumnMigration,
    ReorderColumnsMigration,
)
from isqlite.schema import diff_tables


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
