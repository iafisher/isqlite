import io
import tempfile
import textwrap
import unittest

from click.testing import CliRunner

from isqlite import Database, cli


class ClearableStringIO(io.StringIO):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clear_index = 0

    def getvalue(self):
        v = super().getvalue()
        return v[self.clear_index :]

    def clear(self):
        v = super().getvalue()
        self.clear_index = len(v)


class TemporaryFileTestCase(unittest.TestCase):
    def setUp(self):
        _, self.db_file_path = tempfile.mkstemp()
        self.runner = CliRunner()

    def create_table(self, *, with_data=False):
        self.invoke(
            cli.main_create_table,
            [self.db_file_path, "books", "title TEXT NOT NULL", "author TEXT NOT NULL"],
        )

        if with_data:
            self.invoke(
                cli.main_create,
                [
                    self.db_file_path,
                    "books",
                    "--no-auto-timestamp",
                    "title=Blood Meridian",
                    "author=Cormac McCarthy",
                ],
            )

    def invoke(self, cli_function, args, *, exit_code=0):
        result = self.runner.invoke(cli_function, args, catch_exceptions=False)

        if exit_code is not None:
            self.assertEqual(
                result.exit_code, exit_code, msg=f"Command output: {result.output!r}"
            )

        return result.output


class MigrateTests(TemporaryFileTestCase):
    def test_migration(self):
        # Initial migration to populate the database schema.
        self.invoke(
            cli.main_migrate,
            [
                self.db_file_path,
                "tests/schema.py",
                "--no-confirm",
                "--no-backup",
            ],
        )

        # Create some database rows.
        with Database(self.db_file_path) as db:
            department_id = db.insert(
                "departments", {"name": "Computer Science", "abbreviation": "CS"}
            )
            db.insert(
                "students",
                {
                    "student_id": 123,
                    "first_name": "Maggie",
                    "last_name": "Mathematician",
                    "major": department_id,
                    "graduation_year": 2023,
                },
            )
            professor_id = db.insert(
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
            db.insert(
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
        self.invoke(
            cli.main_migrate,
            [
                self.db_file_path,
                "tests/schema_altered.py",
                "--no-confirm",
                "--no-backup",
            ],
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


class OtherCommandsTests(TemporaryFileTestCase):
    def test_add_column(self):
        self.create_table(with_data=True)

        output = self.invoke(
            cli.main_add_column, [self.db_file_path, "books", "pages INTEGER"]
        )

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(
            output,
            S(
                """
            title           author           pages
            --------------  ---------------  -------
            Blood Meridian  Cormac McCarthy

            1 row.
            """
            ),
        )

    def test_alter_column(self):
        self.create_table(with_data=True)

        self.invoke(
            cli.main_alter_column,
            [
                self.db_file_path,
                "books",
                "author TEXT NOT NULL DEFAULT 'unknown'",
            ],
        )

        self.invoke(
            cli.main_create,
            [self.db_file_path, "books", "title=Beowulf", "--no-auto-timestamp"],
        )

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(
            output,
            S(
                """
            title           author
            --------------  ---------------
            Blood Meridian  Cormac McCarthy
            Beowulf         unknown

            2 rows.
            """
            ),
        )

    def test_count(self):
        self.create_table(with_data=True)

        output = self.invoke(cli.main_count, [self.db_file_path, "books"])

        self.assertEqual(output, "1\n")

    def test_delete(self):
        self.create_table(with_data=True)

        self.invoke(cli.main_delete, [self.db_file_path, "books", "1", "--no-confirm"])

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(output, "No row founds in table 'books'.\n")

    def test_drop_column(self):
        self.create_table(with_data=True)

        self.invoke(
            cli.main_drop_column,
            [
                self.db_file_path,
                "books",
                "author",
                "--no-confirm",
            ],
        )

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(
            output,
            S(
                """
            title
            --------------
            Blood Meridian

            1 row.
            """
            ),
        )

    def test_drop_table(self):
        self.create_table()

        self.invoke(cli.main_drop_table, [self.db_file_path, "books", "--no-confirm"])

        output = self.invoke(cli.main_schema, [self.db_file_path])
        self.assertEqual(output, "")

    def test_get(self):
        self.create_table(with_data=True)

        output = self.invoke(cli.main_get, [self.db_file_path, "books", "1"])
        self.assertEqual(
            output,
            S(
                """
                ------  ---------------
                title   Blood Meridian
                author  Cormac McCarthy
                ------  ---------------
            """
            ),
        )

    def test_rename_column(self):
        self.create_table(with_data=True)

        self.invoke(
            cli.main_rename_column, [self.db_file_path, "books", "author", "authors"]
        )

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(
            output,
            S(
                """
            title           authors
            --------------  ---------------
            Blood Meridian  Cormac McCarthy

            1 row.
            """
            ),
        )

    def test_rename_table(self):
        self.create_table(with_data=True)

        self.invoke(cli.main_rename_table, [self.db_file_path, "books", "books_v2"])

        output = self.invoke(cli.main_schema, [self.db_file_path])
        self.assertEqual(output, "books_v2\n")

    def test_reorder_columns(self):
        self.create_table(with_data=True)

        self.invoke(
            cli.main_reorder_columns, [self.db_file_path, "books", "author", "title"]
        )

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(
            output,
            S(
                """
            author           title
            ---------------  --------------
            Cormac McCarthy  Blood Meridian

            1 row.
            """
            ),
        )

    def test_schema(self):
        self.create_table()

        output = self.invoke(cli.main_schema, [self.db_file_path])
        self.assertEqual(output, "books\n")

    def test_search(self):
        self.create_table(with_data=True)

        output = self.invoke(cli.main_search, [self.db_file_path, "books", "cormac"])
        self.assertEqual(
            output,
            S(
                """
            title           author
            --------------  ---------------
            Blood Meridian  Cormac McCarthy

            1 row.
            """
            ),
        )

    def test_update(self):
        self.create_table(with_data=True)

        self.invoke(
            cli.main_update,
            [
                self.db_file_path,
                "books",
                "1",
                "author=C. McCarthy",
                "--no-auto-timestamp",
            ],
        )

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(
            output,
            S(
                """
            title           author
            --------------  -----------
            Blood Meridian  C. McCarthy

            1 row.
            """
            ),
        )

    def test_update_with_equals_sign_in_value(self):
        self.create_table(with_data=True)

        self.invoke(
            cli.main_update,
            [
                self.db_file_path,
                "books",
                "1",
                "author=Non = Sense",
                "--no-auto-timestamp",
            ],
        )

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(
            output,
            S(
                """
            title           author
            --------------  -----------
            Blood Meridian  Non = Sense

            1 row.
            """
            ),
        )

    def test_create_with_equals_sign_in_value(self):
        self.create_table()

        self.invoke(
            cli.main_create,
            [
                self.db_file_path,
                "books",
                "--no-auto-timestamp",
                "title=A=B",
                "author=Anon",
            ],
        )

        output = self.invoke(cli.main_select, [self.db_file_path, "books"])
        self.assertEqual(
            output,
            S(
                """
            title    author
            -------  --------
            A=B      Anon

            1 row.
            """
            ),
        )


def S(s):
    return textwrap.dedent(s).lstrip("\n")
