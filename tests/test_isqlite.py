import decimal
import time
import unittest

from isqlite import Database, Schema, Table, columns
from isqlite import query as q
from isqlite._core import string_to_camel_case


class DatabaseTests(unittest.TestCase):
    schema = Schema(
        [
            Table(
                "departments",
                [
                    columns.Text("name", required=True),
                    columns.Text("abbreviation", required=True),
                ],
            ),
            Table(
                "professors",
                [
                    columns.Text("first_name", required=True),
                    columns.Text("last_name", required=True),
                    columns.ForeignKey("department", "departments", required=True),
                    columns.Boolean("tenured", required=True),
                    columns.Boolean("retired", required=True),
                ],
            ),
            Table(
                "courses",
                [
                    columns.Integer("course_number", required=True),
                    columns.ForeignKey("department", "departments", required=True),
                    columns.ForeignKey("instructor", "professors", required=True),
                    columns.Text("title", required=True),
                    columns.Decimal("credits", required=True),
                ],
            ),
            Table(
                "students",
                [
                    columns.Integer("student_id", required=True),
                    columns.Text("first_name", required=True),
                    columns.Text("last_name", required=True),
                    columns.ForeignKey("major", "departments", required=False),
                    columns.Integer("graduation_year", required=True),
                ],
            ),
        ]
    )

    def setUp(self):
        self.db = Database(":memory:")
        self.schema.create(self.db)
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

    # TODO: Test read-only parameter.
    # TODO: Test create statement with explicit created_at column.
    # TODO: Test create statement with explicit id column.
    # TODO: Test migrations.

    def tearDown(self):
        self.db.close()


class UtilsTests(unittest.TestCase):
    def test_string_to_camel_case(self):
        self.assertEqual(string_to_camel_case("last_updated_at"), "lastUpdatedAt")
