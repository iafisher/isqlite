import decimal
import sqlite3
import time
import unittest

from isqlite import Database
from isqlite._core import string_to_camel_case


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.db = Database(":memory:")
        self.db.create_table(
            "departments",
            "id INTEGER PRIMARY KEY NOT NULL",
            "name TEXT NOT NULL",
            "abbreviation TEXT NOT NULL",
        )

        self.db.create_table(
            "professors",
            "id INTEGER PRIMARY KEY NOT NULL",
            "first_name TEXT NOT NULL",
            "last_name TEXT NOT NULL",
            "department INTEGER NOT NULL REFERENCES departments",
            "tenured BOOLEAN NOT NULL",
            "retired BOOLEAN NOT NULL",
        )

        self.db.create_table(
            "courses",
            "id INTEGER PRIMARY KEY NOT NULL",
            "course_number INTEGER NOT NULL",
            "department INTEGER NOT NULL REFERENCES departments",
            "instructor INTEGER NOT NULL REFERENCES professors",
            "title TEXT NOT NULL",
            "credits DECIMAL NOT NULL",
        )

        self.db.create_table(
            "students",
            "id INTEGER PRIMARY KEY NOT NULL",
            "student_id INTEGER NOT NULL",
            "first_name TEXT NOT NULL",
            "last_name TEXT NOT NULL",
            "major INTEGER REFERENCES departments",
            "graduation_year INTEGER NOT NULL",
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
        self.assertEqual(
            self.db.count(
                "students", where="first_name = :name", values={"name": "Helga"}
            ),
            1,
        )
        self.assertEqual(
            self.db.count(
                "students", where="graduation_year < 2020 OR first_name = 'Kingsley'",
            ),
            0,
        )

    def test_get(self):
        professor = self.db.get(
            "professors", where="last_name = :last_name", values={"last_name": "Knuth"}
        )
        self.assertEqual(professor["first_name"], "Donald")
        # Make sure the keys are listed in the correct order.
        self.assertEqual(
            list(professor.keys()),
            ["id", "first_name", "last_name", "department", "tenured", "retired"],
        )

        department = self.db.get_by_rowid("departments", professor["department"])
        self.assertEqual(department["name"], "Computer Science")

        non_existent = self.db.get("professors", where="first_name = 'Bob'")
        self.assertIsNone(non_existent)

    def test_get_with_camel_case(self):
        professor = self.db.get(
            "professors", where="last_name = 'Knuth'", camel_case=True
        )
        self.assertEqual(
            list(professor.keys()),
            ["id", "firstName", "lastName", "department", "tenured", "retired"],
        )

    def test_update_with_pk(self):
        professor = self.db.get("professors", where="last_name = 'Knuth'")
        self.assertFalse(professor["retired"])
        self.db.update_by_rowid("professors", professor["id"], {"retired": True})
        professor = self.db.get_by_rowid("professors", professor["id"])
        self.assertTrue(professor["retired"])

    def test_update_with_query(self):
        self.assertEqual(self.db.count("students", where="graduation_year > 2025"), 0)
        self.db.update(
            "students", {"graduation_year": 2026}, where="graduation_year < 2025",
        )
        self.assertEqual(self.db.count("students", where="graduation_year > 2025"), 2)

    def test_update_with_full_object(self):
        professor = self.db.get("professors", where="last_name = 'Knuth'")
        self.assertFalse(professor["retired"])

        professor["retired"] = True
        time.sleep(0.1)
        self.db.update_by_rowid("professors", professor["id"], professor)

        updated_professor = self.db.get_by_rowid("professors", professor["id"])
        self.assertTrue(updated_professor["retired"])
        self.assertEqual(professor["id"], updated_professor["id"])

    def test_delete(self):
        self.db.delete("students", where="graduation_year > 2022")
        student = self.db.get("students", where="graduation_year > 2022")
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
            "students", where="graduation_year = 2025 AND first_name = 'Jane'"
        )
        self.assertEqual(len(students), 100)
        self.assertEqual(students[0]["first_name"], "Jane")

    def test_add_column(self):
        self.db.add_column("professors", "year_of_hire INTEGER")

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
                "year_of_hire",
            ],
        )
        self.assertIsNone(professor["year_of_hire"])

    def test_drop_column(self):
        self.db.drop_column("professors", "retired")

        professor = self.db.get("professors")
        self.assertEqual(
            list(professor.keys()),
            ["id", "first_name", "last_name", "department", "tenured"],
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

        reordered = ["id", "abbreviation", "name"]
        self.db.reorder_columns("departments", reordered)

        self.assertEqual(list(self.db.get("departments").keys()), reordered)
        after = [dict(row) for row in self.db.list("departments", order_by="name")]
        self.assertEqual(before, after)

    def test_cannot_modify_read_only_database(self):
        db = Database(":memory:", readonly=True)
        with self.assertRaises(sqlite3.OperationalError):
            db.create_table(
                "people", "name TEXT NOT NULL",
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
