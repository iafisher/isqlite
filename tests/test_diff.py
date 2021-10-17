import unittest

from isqlite import AutoTable, Database, Schema, Table, columns
from isqlite.migrations import (
    AddColumnMigration,
    RenameColumnMigration,
    ReorderColumnsMigration,
)


class DiffTests(unittest.TestCase):
    def test_diff_column_renamed(self):
        schema_before = Schema(
            [
                Table(
                    "employees",
                    [
                        columns.text("name", required=True),
                    ],
                ),
            ]
        )
        schema_after = Schema(
            [
                Table(
                    "employees",
                    [
                        columns.text("legal_name", required=True),
                    ],
                ),
            ]
        )

        with Database(":memory:", transaction=False) as db:
            db.migrate(schema_before)

            diff = db.diff(schema_after)

            self.assertEqual(
                diff, [RenameColumnMigration("employees", "name", "legal_name")]
            )

    def test_diff_column_added(self):
        schema_before = Schema(
            [
                AutoTable(
                    "events",
                    [
                        "start DATE",
                    ],
                ),
            ]
        )
        schema_after = Schema(
            [
                AutoTable(
                    "events",
                    [
                        "start DATE",
                        "end DATE",
                    ],
                ),
            ]
        )

        with Database(":memory:", transaction=False) as db:
            db.migrate(schema_before)

            diff = db.diff(schema_after)

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
