import unittest

from isqlite import Database, columns

from .common import create_test_data, get_schema


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.db = Database(":memory:")
        self.schema = get_schema()
        self.schema.create(self.db)
        create_test_data(self.db)

    def test_add_column(self):
        self.schema.add_column(
            self.db, "professors", columns.Integer("year_of_hire", required=False)
        )

        professor = self.db.get("professors")
        self.assertEqual(
            list(professor.keys()),
            [
                "id",
                "first_name",
                "last_name",
                "department",
                "tenured",
                "retired",
                "created_at",
                "last_updated_at",
                "year_of_hire",
            ],
        )
        self.assertIsNone(professor["year_of_hire"])

    def test_drop_column(self):
        self.schema.drop_column(self.db, "professors", "retired")

        professor = self.db.get("professors")
        self.assertEqual(
            list(professor.keys()),
            [
                "id",
                "first_name",
                "last_name",
                "department",
                "tenured",
                "created_at",
                "last_updated_at",
            ],
        )
        self.assertEqual(self.db.sql("PRAGMA foreign_keys", as_tuple=True)[0][0], 1)
