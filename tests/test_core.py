import decimal
import sqlite3
import time
import unittest

from isqlite import Database, Table
from isqlite import columns as isqlite_columns
from isqlite import query as q
from isqlite._core import get_table_from_create_statement, string_to_camel_case


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.db = Database(":memory:")
        self.db.create_table(
            Table(
                "departments",
                [
                    isqlite_columns.Text("name", required=True),
                    isqlite_columns.Text("abbreviation", required=True),
                ],
            )
        )

        self.db.create_table(
            Table(
                "professors",
                [
                    isqlite_columns.Text("first_name", required=True),
                    isqlite_columns.Text("last_name", required=True),
                    isqlite_columns.ForeignKey(
                        "department", "departments", required=True
                    ),
                    isqlite_columns.Boolean("tenured", required=True),
                    isqlite_columns.Boolean("retired", required=True),
                ],
            )
        )

        self.db.create_table(
            Table(
                "courses",
                [
                    isqlite_columns.Integer("course_number", required=True),
                    isqlite_columns.ForeignKey(
                        "department", "departments", required=True
                    ),
                    isqlite_columns.ForeignKey(
                        "instructor", "professors", required=True
                    ),
                    isqlite_columns.Text("title", required=True),
                    isqlite_columns.Decimal("credits", required=True),
                ],
            )
        )

        self.db.create_table(
            Table(
                "students",
                [
                    isqlite_columns.Integer("student_id", required=True),
                    isqlite_columns.Text("first_name", required=True),
                    isqlite_columns.Text("last_name", required=True),
                    isqlite_columns.ForeignKey("major", "departments", required=False),
                    isqlite_columns.Integer("graduation_year", required=True),
                ],
            )
        )

        cs_department = self.db.create(
            "departments", {"name": "Computer Science", "abbreviation": "CS"}
        )
        ling_department = self.db.create(
            "departments", {"name": "Linguistics", "abbreviation": "LING"}
        )
        donald_knuth = self.db.create(
            "professors",
            {
                "first_name": "Donald",
                "last_name": "Knuth",
                "department": cs_department,
                "tenured": True,
                "retired": False,
            },
        )
        noam_chomsky = self.db.create(
            "professors",
            {
                "first_name": "Noam",
                "last_name": "Chomsky",
                "department": ling_department,
                "tenured": True,
                "retired": True,
            },
        )
        self.db.create_many(
            "courses",
            [
                {
                    "course_number": 399,
                    "department": cs_department,
                    "instructor": donald_knuth,
                    "title": "Algorithms",
                    "credits": decimal.Decimal(2.0),
                },
                {
                    "couse_number": 101,
                    "department": ling_department,
                    "instructor": noam_chomsky,
                    "title": "Intro to Linguistics",
                    "credits": decimal.Decimal(1.0),
                },
            ],
        )
        self.db.create_many(
            "students",
            [
                {
                    "student_id": 123456,
                    "first_name": "Helga",
                    "last_name": "Heapsort",
                    "major": cs_department,
                    "graduation_year": 2023,
                },
                {
                    "student_id": 456789,
                    "first_name": "Philip",
                    "last_name": "Phonologist",
                    "major": ling_department,
                    "graduation_year": 2022,
                },
            ],
        )
        self.db.commit()

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
        self.db.add_column("professors", "year_of_hire", "INTEGER")

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

    def test_alter_column(self):
        professor_before = self.db.get("professors")

        self.db.alter_column("professors", "tenured", "INTEGER NOT NULL")

        professor_after = self.db.get("professors")
        # This assertion is a little tricky, because True == 1 and isinstance(True, int)
        # are both true, so e.g.
        #
        #     self.assertEqual(professor["tenured"], 1)
        #
        # wouldn't work because it would be true even if the alter column operation
        # didn't work.
        #
        # Instead, we assert that professor["tenured"] is NOT a boolean.
        self.assertFalse(isinstance(professor_after["tenured"], bool))
        # Sanity check that we didn't mess up other columns.
        self.assertEqual(professor_before, professor_after)

    def test_reorder_columns(self):
        # We convert the rows to a regular dictionary because the OrderedDicts won't
        # compare equal after the reordering operation.
        before = [dict(row) for row in self.db.list("departments", order_by="name")]

        reordered = ["id", "abbreviation", "name", "created_at", "last_updated_at"]
        self.db.reorder_columns("departments", reordered)

        self.assertEqual(list(self.db.get("departments").keys()), reordered)
        after = [dict(row) for row in self.db.list("departments", order_by="name")]
        self.assertEqual(before, after)

    def test_cannot_modify_read_only_database(self):
        db = Database(":memory:", readonly=True)
        with self.assertRaises(sqlite3.OperationalError):
            db.create_table(
                Table("people", [isqlite_columns.Text("name", required=True)])
            )

    # TODO: Test create statement with explicit created_at column.
    # TODO: Test create statement with explicit id column.
    # TODO: Test migrations.

    def tearDown(self):
        self.db.close()


class UtilsTests(unittest.TestCase):
    def test_string_to_camel_case(self):
        self.assertEqual(string_to_camel_case("last_updated_at"), "lastUpdatedAt")
        self.assertEqual(string_to_camel_case("_abc"), "_abc")

    def test_get_table_on_simple_create_statement(self):
        sql = """
          CREATE TABLE people(
            name TEXT,
            age INTEGER
          )
        """
        table = get_table_from_create_statement("people", sql)
        self.assertEqual(
            list(table.columns.values()),
            [
                isqlite_columns.RawColumn("name", "TEXT"),
                isqlite_columns.RawColumn("age", "INTEGER"),
            ],
        )
        self.assertEqual(table.constraints, [])
        self.assertFalse(table.without_rowid)

    def test_get_table_on_simple_create_statement_with_semicolon(self):
        sql = """
          CREATE TABLE people(
            name TEXT,
            age INTEGER
          );
        """
        table = get_table_from_create_statement("people", sql)
        self.assertEqual(
            list(table.columns.values()),
            [
                isqlite_columns.RawColumn("name", "TEXT"),
                isqlite_columns.RawColumn("age", "INTEGER"),
            ],
        )
        self.assertEqual(table.constraints, [])
        self.assertFalse(table.without_rowid)

    def test_get_table_on_more_complex_create_statement(self):
        sql = """
          CREATE TABLE people(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL CHECK(name != ','),
            age INTEGER DEFAULT 1 + 2,
            retired BOOLEAN
          );
        """
        table = get_table_from_create_statement("people", sql)
        self.assertEqual(
            list(table.columns.values()),
            [
                isqlite_columns.RawColumn("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                isqlite_columns.RawColumn(
                    "name", "TEXT NOT NULL CHECK ( name != ',' )"
                ),
                isqlite_columns.RawColumn("age", "INTEGER DEFAULT 1 + 2"),
                isqlite_columns.RawColumn("retired", "BOOLEAN"),
            ],
        )
        self.assertEqual(table.constraints, [])
        self.assertFalse(table.without_rowid)

    def test_get_table_on_create_statement_with_comments(self):
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
        table = get_table_from_create_statement("people", sql)
        self.assertEqual(
            list(table.columns.values()),
            [
                isqlite_columns.RawColumn("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
                isqlite_columns.RawColumn(
                    "name", "TEXT NOT NULL CHECK ( name != ',' )"
                ),
                isqlite_columns.RawColumn("age", "INTEGER DEFAULT 1 + 2"),
                isqlite_columns.RawColumn("retired", "BOOLEAN"),
            ],
        )
        self.assertEqual(table.constraints, [])
        self.assertFalse(table.without_rowid)

    def test_get_table_on_create_statement_with_constraints(self):
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
        table = get_table_from_create_statement("people", sql)
        self.assertEqual(
            list(table.columns.values()),
            [
                isqlite_columns.RawColumn("id", "INTEGER NOT NULL"),
                isqlite_columns.RawColumn("name", "TEXT"),
                isqlite_columns.RawColumn("age", "INTEGER"),
                isqlite_columns.RawColumn("manager", "INTEGER NOT NULL"),
            ],
        )
        self.assertEqual(
            table.constraints,
            [
                isqlite_columns.RawConstraint(
                    "FOREIGN KEY ( manager ) REFERENCES managers"
                ),
                isqlite_columns.RawConstraint("PRIMARY KEY ( id , manager )"),
            ],
        )
        self.assertFalse(table.without_rowid)

    def test_get_table_on_without_rowid_table(self):
        sql = """
          CREATE TABLE people(
            name TEXT,
            age INTEGER
          ) WITHOUT ROWID
        """
        table = get_table_from_create_statement("people", sql)
        self.assertEqual(
            list(table.columns.values()),
            [
                isqlite_columns.RawColumn("name", "TEXT"),
                isqlite_columns.RawColumn("age", "INTEGER"),
            ],
        )
        self.assertEqual(table.constraints, [])
        self.assertTrue(table.without_rowid)

    def test_get_table_with_quoted_names(self):
        # Based on https://sqlite.org/lang_keywords.html
        sql = """
          CREATE TABLE people(
            "name" TEXT,
            [age] INTEGER,
            `rank` INTEGER,
            /*
            Strict SQL doesn't allow single quotes for quoted identifiers, but SQLite
            accepts it for backwards compatibility.
            */
            'pay' DECIMAL
          )
        """
        table = get_table_from_create_statement("people", sql)
        self.assertEqual(
            list(table.columns.values()),
            [
                isqlite_columns.RawColumn("name", "TEXT"),
                isqlite_columns.RawColumn("age", "INTEGER"),
                isqlite_columns.RawColumn("rank", "INTEGER"),
                isqlite_columns.RawColumn("pay", "DECIMAL"),
            ],
        )
        self.assertEqual(table.constraints, [])
        self.assertFalse(table.without_rowid)
