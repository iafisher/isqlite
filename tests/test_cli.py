import tempfile
import unittest

from isqlite import Database, cli


class MigrateTests(unittest.TestCase):
    def setUp(self):
        _, self.db_file_path = tempfile.mkstemp()

    def test_migration(self):
        # Initial migration to populate the database schema.
        cli.main_migrate(
            self.db_file_path,
            "tests/schema.py",
            None,
            write=True,
            no_backup=True,
            debug=False,
        )

        # Create some database rows.
        with Database(self.db_file_path) as db:
            department_id = db.create(
                "departments", {"name": "Computer Science", "abbreviation": "CS"}
            )
            db.create(
                "students",
                {
                    "student_id": 123,
                    "first_name": "Maggie",
                    "last_name": "Mathematician",
                    "major": department_id,
                    "graduation_year": 2023,
                },
            )
            professor_id = db.create(
                "professors",
                {
                    "first_name": "Barbara",
                    "last_name": "Liskov",
                    "department": department_id,
                    "tenured": True,
                    "retired": True,
                    "manager": None,
                },
            )
            db.create(
                "courses",
                {
                    "course_number": 101,
                    "department": department_id,
                    "instructor": professor_id,
                    "title": "Object-Oriented Programming",
                    "credits": 1.0,
                },
            )

            # Sanity check.
            self.assertEqual(db.count("departments"), 1)
            self.assertEqual(db.count("students"), 1)
            self.assertEqual(db.count("professors"), 1)
            self.assertEqual(db.count("courses"), 1)

        # Migrate to a new schema.
        cli.main_migrate(
            self.db_file_path,
            "tests/schema2.py",
            None,
            write=True,
            no_backup=True,
            debug=False,
        )

        # Test the database to make sure the migration succeeded.
        with Database(self.db_file_path) as db:
            # Should still have all the old rows.
            self.assertEqual(db.count("departments"), 1)
            self.assertEqual(db.count("students"), 1)
            self.assertEqual(db.count("professors"), 1)
            self.assertEqual(db.count("courses"), 1)

            self.assertEqual(
                list(db.get("professors").keys()),
                [
                    "id",
                    "first_name",
                    "last_name",
                    "department",
                    "retired",
                    "manager",
                    "created_at",
                    "last_updated_at",
                ],
            )

            self.assertEqual(
                list(db.get("students").keys()),
                [
                    "id",
                    "student_id",
                    "first_name",
                    "last_name",
                    "major",
                    "graduation_year",
                    "dormitory",
                    "created_at",
                    "last_updated_at",
                ],
            )

            student = db.get("students")
            self.assertEqual(student["student_id"], 123)
            self.assertEqual(student["first_name"], "Maggie")
            self.assertEqual(student["last_name"], "Mathematician")
            self.assertEqual(student["major"], department_id)
            self.assertEqual(student["graduation_year"], 2023)

            professor = db.get("professors")
            self.assertEqual(professor["first_name"], "Barbara")
            self.assertEqual(professor["last_name"], "Liskov")
            self.assertEqual(professor["department"], department_id)
            self.assertEqual(professor["retired"], True)
            self.assertEqual(professor["manager"], None)
