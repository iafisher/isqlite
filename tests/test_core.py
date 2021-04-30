import time
import unittest

from isqlite import Database
from isqlite import columns as isqlite_columns
from isqlite import query as q
from isqlite._core import get_columns_from_create_statement, string_to_camel_case

from .common import create_test_data, get_schema


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.db = Database(":memory:")
        self.schema = get_schema()
        self.schema.create(self.db)
        create_test_data(self.db)

    def test_count(self):
        self.assertEqual(self.db.count("departments"), 2)
        self.assertEqual(self.db.count("professors"), 2)
        self.assertEqual(self.db.count("courses"), 2)
        self.assertEqual(self.db.count("students"), 2)
        self.assertEqual(self.db.count("students", q.Equals("first_name", "Helga")), 1)
        self.assertEqual(
            self.db.count(
                "students",
                q.LessThan("graduation_year", 2020)
                | q.Equals("first_name", "Kingsley"),
            ),
            0,
        )

    def test_get(self):
        professor = self.db.get("professors", q.Equals("last_name", "Knuth"))
        self.assertEqual(professor["first_name"], "Donald")
        # Make sure the keys are listed in the correct order.
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
            ],
        )

        department = self.db.get("departments", professor["department"])
        self.assertEqual(department["name"], "Computer Science")

        non_existent = self.db.get("professors", q.Equals("first_name", "Bob"))
        self.assertIsNone(non_existent)

    def test_get_with_camel_case(self):
        professor = self.db.get(
            "professors", q.Equals("last_name", "Knuth"), camel_case=True
        )
        self.assertEqual(
            list(professor.keys()),
            [
                "id",
                "firstName",
                "lastName",
                "department",
                "tenured",
                "retired",
                "createdAt",
                "lastUpdatedAt",
            ],
        )

    def test_update_with_pk(self):
        professor = self.db.get("professors", q.Equals("last_name", "Knuth"))
        self.assertFalse(professor["retired"])
        self.db.update("professors", professor["id"], {"retired": True})
        professor = self.db.get("professors", professor["id"])
        self.assertTrue(professor["retired"])

    def test_update_with_query(self):
        self.assertEqual(
            self.db.count("students", q.GreaterThan("graduation_year", 2025)), 0
        )
        self.db.update(
            "students", q.LessThan("graduation_year", 2025), {"graduation_year": 2026}
        )
        self.assertEqual(
            self.db.count("students", q.GreaterThan("graduation_year", 2025)), 2
        )

    def test_update_with_full_object(self):
        professor = self.db.get("professors", q.Equals("last_name", "Knuth"))
        self.assertFalse(professor["retired"])

        professor["retired"] = True
        time.sleep(0.1)
        self.db.update("professors", professor["id"], professor)

        updated_professor = self.db.get("professors", professor["id"])
        self.assertTrue(updated_professor["retired"])
        self.assertEqual(professor["id"], updated_professor["id"])
        self.assertEqual(professor["created_at"], updated_professor["created_at"])
        self.assertLess(
            professor["last_updated_at"], updated_professor["last_updated_at"]
        )

    def test_delete(self):
        self.db.delete("students", q.GreaterThan("graduation_year", 2022))
        student = self.db.get("students", q.GreaterThan("graduation_year", 2022))
        self.assertIsNone(student)

    def test_list(self):
        for i in range(100):
            self.db.create(
                "students",
                {
                    "student_id": i,
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "major": None,
                    "graduation_year": 2025,
                },
            )

        students = self.db.list(
            "students",
            q.Equals("graduation_year", 2025) & q.Equals("first_name", "Jane"),
        )
        self.assertEqual(len(students), 100)
        self.assertEqual(students[0]["first_name"], "Jane")

    def test_add_column(self):
        self.db.add_column(
            "professors", isqlite_columns.Integer("year_of_hire", required=False)
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
        self.db.drop_column("professors", "retired")

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

    # TODO: Test read-only parameter.
    # TODO: Test create statement with explicit created_at column.
    # TODO: Test create statement with explicit id column.
    # TODO: Test migrations.

    def tearDown(self):
        self.db.close()


class UtilsTests(unittest.TestCase):
    def test_string_to_camel_case(self):
        self.assertEqual(string_to_camel_case("last_updated_at"), "lastUpdatedAt")
        self.assertEqual(string_to_camel_case("_abc"), "_abc")

    def test_get_columns_on_simple_create_statement(self):
        sql = """
          CREATE TABLE people(
            name TEXT,
            age INTEGER
          )
        """
        columns, constraints = get_columns_from_create_statement(sql)
        self.assertEqual(
            columns,
            [
                isqlite_columns.RawColumn("name", "TEXT"),
                isqlite_columns.RawColumn("age", "INTEGER"),
            ],
        )
        self.assertEqual(constraints, [])

    def test_get_columns_on_simple_create_statement_with_semicolon(self):
        sql = """
          CREATE TABLE people(
            name TEXT,
            age INTEGER
          );
        """
        columns, constraints = get_columns_from_create_statement(sql)
        self.assertEqual(
            columns,
            [
                isqlite_columns.RawColumn("name", "TEXT"),
                isqlite_columns.RawColumn("age", "INTEGER"),
            ],
        )
        self.assertEqual(constraints, [])

    def test_get_columns_on_more_complex_create_statement(self):
        sql = """
          CREATE TABLE people(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL CHECK(name != ','),
            age INTEGER DEFAULT 1 + 2,
            retired BOOLEAN
          );
        """
        columns, constraints = get_columns_from_create_statement(sql)
        self.assertEqual(
            columns,
            [
                isqlite_columns.RawColumn("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                isqlite_columns.RawColumn(
                    "name", "TEXT NOT NULL CHECK ( name != ',' )"
                ),
                isqlite_columns.RawColumn("age", "INTEGER DEFAULT 1 + 2"),
                isqlite_columns.RawColumn("retired", "BOOLEAN"),
            ],
        )
        self.assertEqual(constraints, [])

    def test_get_columns_on_create_statement_with_comments(self):
        sql = """
          CREATE TABLE people(
            -- A single line comment
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            /*
            A multi-line comment
            */
            name TEXT NOT NULL CHECK(name != ','),
            age INTEGER DEFAULT 1 + 2,
            retired BOOLEAN
          );
        """
        columns, constraints = get_columns_from_create_statement(sql)
        self.assertEqual(
            columns,
            [
                isqlite_columns.RawColumn("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                isqlite_columns.RawColumn(
                    "name", "TEXT NOT NULL CHECK ( name != ',' )"
                ),
                isqlite_columns.RawColumn("age", "INTEGER DEFAULT 1 + 2"),
                isqlite_columns.RawColumn("retired", "BOOLEAN"),
            ],
        )
        self.assertEqual(constraints, [])

    def test_get_columns_on_create_statement_with_constraints(self):
        sql = """
          CREATE TABLE people(
            id INTEGER NOT NULL,
            name TEXT,
            age INTEGER,
            manager INTEGER NOT NULL,
            FOREIGN KEY(manager) REFERENCES managers,
            PRIMARY KEY (id, manager)
          );
        """
        columns, constraints = get_columns_from_create_statement(sql)
        self.assertEqual(
            columns,
            [
                isqlite_columns.RawColumn("id", "INTEGER NOT NULL"),
                isqlite_columns.RawColumn("name", "TEXT"),
                isqlite_columns.RawColumn("age", "INTEGER"),
                isqlite_columns.RawColumn("manager", "INTEGER NOT NULL"),
            ],
        )
        self.assertEqual(
            constraints,
            [
                isqlite_columns.RawConstraint(
                    "FOREIGN KEY ( manager ) REFERENCES managers"
                ),
                isqlite_columns.RawConstraint("PRIMARY KEY ( id , manager )"),
            ],
        )
