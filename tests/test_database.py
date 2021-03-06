import decimal
import sqlite3
import time
import unittest

from isqlite import (
    AutoTable,
    ColumnDoesNotExistError,
    Database,
    ISqliteError,
    Schema,
    Table,
    columns,
)

from .schema import SCHEMA


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.db = Database(":memory:", transaction=False)
        self.db.migrate(SCHEMA)

        self.db.begin_transaction()
        cs_department_pk = self.db.insert(
            "departments",
            {"name": "Computer Science", "abbreviation": "CS"},
        )
        ling_department_pk = self.db.insert(
            "departments",
            {"name": "Linguistics", "abbreviation": "LING"},
        )
        donald_knuth_pk = self.db.insert(
            "professors",
            {
                "first_name": "Donald",
                "last_name": "Knuth",
                "department": cs_department_pk,
                "tenured": True,
                "retired": False,
            },
        )
        # No particular need to get the full row for Noam Chomsky, just want to make
        # sure that ``insert_and_get`` works.
        noam_chomsky = self.db.insert_and_get(
            "professors",
            {
                "first_name": "Noam",
                "last_name": "Chomsky",
                "department": ling_department_pk,
                "tenured": True,
                "retired": True,
            },
        )
        self.db.insert(
            "professors",
            {
                "first_name": "Larry",
                "last_name": "Logician",
                "department": cs_department_pk,
                "tenured": False,
                "retired": False,
                "manager": donald_knuth_pk,
            },
        )
        self.db.insert_many(
            "courses",
            [
                {
                    "course_number": 399,
                    "department": cs_department_pk,
                    "instructor": donald_knuth_pk,
                    "title": "Algorithms",
                    "credits": decimal.Decimal(2.0),
                },
                {
                    "couse_number": 101,
                    "department": ling_department_pk,
                    "instructor": noam_chomsky["id"],
                    "title": "Intro to Linguistics",
                    "credits": decimal.Decimal(1.0),
                },
            ],
        )
        self.db.insert_many(
            "students",
            [
                {
                    "student_id": 123456,
                    "first_name": "Helga",
                    "last_name": "Heapsort",
                    "major": cs_department_pk,
                    "graduation_year": 2023,
                },
                {
                    "student_id": 456789,
                    "first_name": "Philip",
                    "last_name": "Phonologist",
                    "major": ling_department_pk,
                    "graduation_year": 2022,
                },
                {
                    "student_id": 654321,
                    "first_name": "Ursula",
                    "last_name": "Unsure",
                    "major": None,
                    "graduation_year": 2024,
                },
            ],
        )
        self.db.commit()

    def tearDown(self):
        self.db.close()

    def test_count(self):
        self.assertEqual(self.db.count("departments"), 2)
        self.assertEqual(self.db.count("professors"), 3)
        self.assertEqual(self.db.count("courses"), 2)
        self.assertEqual(self.db.count("students"), 3)
        self.assertEqual(self.db.count("students", distinct="first_name"), 3)
        self.assertEqual(
            self.db.count(
                "students", where="first_name = :name", values={"name": "Helga"}
            ),
            1,
        )
        self.assertEqual(
            self.db.count(
                "students",
                where="first_name = :name",
                values={"name": "Helga"},
                distinct="first_name",
            ),
            1,
        )
        self.assertEqual(
            self.db.count(
                "students",
                where="graduation_year < 2020 OR first_name = 'Kingsley'",
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
            [
                "id",
                "first_name",
                "last_name",
                "department",
                "tenured",
                "retired",
                "manager",
            ],
        )

        department = self.db.get_by_pk("departments", professor["department"])
        self.assertEqual(department["name"], "Computer Science")

        non_existent = self.db.get("professors", where="first_name = 'Bob'")
        self.assertIsNone(non_existent)

    def test_get_with_certain_columns(self):
        professor = self.db.get(
            "professors",
            where="last_name = :last_name",
            values={"last_name": "Knuth"},
            columns=["department"],
        )

        department = self.db.get_by_pk("departments", professor["department"])
        self.assertEqual(department["name"], "Computer Science")

        self.assertEqual(list(professor.keys()), ["department"])

    def test_get_with_non_existent_table(self):
        with self.assertRaises(sqlite3.OperationalError):
            self.db.get("deans")

    def test_get_related(self):
        course = self.db.get(
            "courses", where="course_number = 399", get_related=["department"]
        )
        self.assertEqual(course["department"]["name"], "Computer Science")
        self.assertTrue(isinstance(course["instructor"], int))

        course = self.db.get("courses", where="course_number = 399", get_related=True)
        self.assertEqual(course["department"]["name"], "Computer Science")
        self.assertEqual(course["instructor"]["first_name"], "Donald")
        self.assertEqual(course["instructor"]["last_name"], "Knuth")

    def test_get_related_with_recursive_foreign_key(self):
        larry = self.db.get(
            "professors", where="first_name = 'Larry'", get_related=True
        )
        # The `manager` column should be an integer rather than an object because
        # getting related columns on the same table is not supported.
        self.assertIsInstance(larry["manager"], int)

    def test_get_related_with_null_foreign_key(self):
        student = self.db.get(
            "students", where="first_name = 'Ursula'", get_related=["major"]
        )
        self.assertEqual(student["major"], None)

    def test_get_related_with_non_foreign_key_column(self):
        with self.assertRaises(ISqliteError):
            self.db.get(
                "students", where="first_name = 'Ursula'", get_related=["first_name"]
            )

    def test_get_related_with_non_existent_column(self):
        with self.assertRaises(ColumnDoesNotExistError):
            self.db.get("students", where="first_name = 'Ursula'", get_related=["nope"])

    def test_insert_and_get_related(self):
        any_department = self.db.get("departments")
        student = self.db.insert_and_get(
            "students",
            {
                "student_id": 123,
                "first_name": "John",
                "last_name": "Doe",
                "major": any_department["id"],
                "graduation_year": 2021,
            },
            get_related=True,
        )

        self.assertEqual(student["major"], any_department)

    def test_get_by_pk_with_get_related(self):
        # Regression test for https://github.com/iafisher/isqlite/issues/51
        course = self.db.get_by_pk("courses", 1, get_related=True)
        self.assertEqual(course["department"]["name"], "Computer Science")

    def test_get_or_insert_with_existing_row(self):
        student = self.db.get_by_pk("students", 1)
        new_student = self.db.get_or_insert("students", student)
        self.assertEqual(student["id"], new_student["id"])
        self.assertFalse(new_student.inserted)

    def test_get_or_insert_with_new_row(self):
        n = self.db.count("students")
        student = self.db.get_or_insert(
            "students",
            {
                "student_id": 123,
                "first_name": "John",
                "last_name": "Doe",
                "major": None,
                "graduation_year": 2021,
            },
        )

        self.assertEqual(student["first_name"], "John")
        self.assertTrue(student.inserted)

        self.assertEqual(self.db.count("students"), n + 1)

    def test_update_with_pk(self):
        professor = self.db.get("professors", where="last_name = 'Knuth'")
        self.assertFalse(professor["retired"])
        updated = self.db.update_by_pk(
            "professors",
            professor["id"],
            {"retired": True},
        )
        professor = self.db.get_by_pk("professors", professor["id"])

        self.assertTrue(updated)
        self.assertTrue(professor["retired"])

    def test_update_with_query(self):
        self.assertEqual(self.db.count("students", where="graduation_year > 2025"), 0)
        n = self.db.update(
            "students",
            {"graduation_year": 2026},
            where="graduation_year < 2025",
        )

        self.assertEqual(n, 3)
        self.assertEqual(self.db.count("students", where="graduation_year > 2025"), 3)

    def test_update_with_full_object(self):
        professor = self.db.get("professors", where="last_name = 'Knuth'")
        self.assertFalse(professor["retired"])

        professor["retired"] = True
        time.sleep(0.1)
        updated = self.db.update_by_pk(
            "professors",
            professor["id"],
            professor,
        )

        updated_professor = self.db.get_by_pk("professors", professor["id"])
        self.assertTrue(updated)
        self.assertTrue(updated_professor["retired"])
        self.assertEqual(professor["id"], updated_professor["id"])

    def test_foreign_key_enforcement(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.insert(
                "professors",
                {
                    "first_name": "Jack",
                    "last_name": "Black",
                    "department": 999,
                    "tenured": True,
                    "retired": True,
                },
            )

    def test_delete(self):
        self.db.delete("students", where="graduation_year > 2022")
        student = self.db.get("students", where="graduation_year > 2022")
        self.assertIsNone(student)

    def test_delete_by_pk(self):
        student = self.db.get("students")
        pk = student["id"]
        self.db.delete_by_pk("students", pk)

        student = self.db.get_by_pk("students", pk)
        self.assertIsNone(student)

    def test_delete_many_by_pks(self):
        student_ids = [row["id"] for row in self.db.select("students")]
        one_not_to_delete = student_ids.pop()
        # Make sure we are in fact deleting multiple rows.
        self.assertGreater(len(student_ids), 1)

        self.db.delete_many_by_pks("students", student_ids)

        self.assertEqual(self.db.count("students"), 1)
        self.assertIsNotNone(self.db.get_by_pk("students", one_not_to_delete))
        self.assertIsNone(self.db.get_by_pk("students", student_ids[0]))

    def test_delete_many_by_pks_with_empty_input(self):
        count_before = self.db.count("students")
        self.db.delete_many_by_pks("students", [])
        count_after = self.db.count("students")

        self.assertEqual(count_before, count_after)

    def test_select(self):
        for i in range(100):
            self.db.insert(
                "students",
                {
                    "student_id": i,
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "major": None,
                    "graduation_year": 2025,
                },
            )

        students = self.db.select(
            "students", where="graduation_year = 2025 AND first_name = 'Jane'"
        )
        self.assertEqual(len(students), 100)
        self.assertEqual(students[0]["first_name"], "Jane")

        self.assertEqual(len(self.db.select("students", limit=5)), 5)

    def test_select_with_certain_columns(self):
        for i in range(100):
            self.db.insert(
                "students",
                {
                    "student_id": i,
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "major": None,
                    "graduation_year": 2025,
                },
            )

        students = self.db.select(
            "students",
            where="graduation_year = 2025 AND first_name = 'Jane'",
            columns=["first_name", "last_name"],
        )
        self.assertEqual(len(students), 100)
        self.assertEqual(students[0]["first_name"], "Jane")
        self.assertEqual(students[0]["last_name"], "Doe")
        self.assertTrue(
            all(list(s.keys()) == ["first_name", "last_name"] for s in students)
        )

    def test_select_with_get_related(self):
        courses = self.db.select("courses", get_related=True, order_by="course_number")
        self.assertEqual(len(courses), 2)
        self.assertEqual(courses[0]["department"]["name"], "Linguistics")
        self.assertEqual(courses[0]["instructor"]["first_name"], "Noam")
        self.assertEqual(courses[0]["instructor"]["last_name"], "Chomsky")
        self.assertEqual(courses[1]["department"]["name"], "Computer Science")
        self.assertEqual(courses[1]["instructor"]["first_name"], "Donald")
        self.assertEqual(courses[1]["instructor"]["last_name"], "Knuth")

    def test_select_with_columns_and_get_related(self):
        courses = self.db.select(
            "courses",
            columns=["course_number", "department"],
            get_related=["department"],
            order_by="course_number",
        )

        self.assertEqual(len(courses), 2)
        self.assertEqual(courses[0]["department"]["name"], "Linguistics")
        self.assertEqual(courses[1]["department"]["name"], "Computer Science")

        self.assertEqual(list(courses[0].keys()), ["course_number", "department"])
        self.assertEqual(list(courses[1].keys()), ["course_number", "department"])

    def test_select_with_order_by(self):
        courses = self.db.select("courses", order_by="course_number")
        self.assertEqual(len(courses), 2)
        self.assertEqual(courses[0]["course_number"], 101)
        self.assertEqual(courses[1]["course_number"], 399)

    def test_select_with_multiple_order_by(self):
        profs = self.db.select("professors", order_by=("retired", "first_name"))
        self.assertEqual(len(profs), 3)
        self.assertEqual(profs[0]["first_name"], "Donald")
        self.assertEqual(profs[0]["last_name"], "Knuth")
        self.assertEqual(profs[1]["first_name"], "Larry")
        self.assertEqual(profs[1]["last_name"], "Logician")
        self.assertEqual(profs[2]["first_name"], "Noam")
        self.assertEqual(profs[2]["last_name"], "Chomsky")

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
                "manager",
                "year_of_hire",
            ],
        )
        self.assertIsNone(professor["year_of_hire"])

    def test_drop_column(self):
        with self.db.transaction(disable_foreign_keys=True):
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
                "manager",
            ],
        )
        self.assertEqual(self.db.sql("PRAGMA foreign_keys", as_tuple=True)[0][0], 1)

    def test_drop_column_with_keyword_name(self):
        self.db.create_table("test", ["name TEXT", "age INTEGER", '"order" INTEGER'])

        self.db.insert("test", {"name": "John Doe", "age": 24})

        self.db.drop_column("test", "age")

        self.assertEqual(self.db.get("test"), {"name": "John Doe", "order": None})

    def test_drop_column_with_non_existent_column(self):
        with self.assertRaises(ColumnDoesNotExistError):
            self.db.drop_column("students", "dormitory")

    def test_alter_column(self):
        professor_before = self.db.get("professors")

        with self.db.transaction(disable_foreign_keys=True):
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

    def test_alter_column_with_non_existent_column(self):
        with self.assertRaises(ColumnDoesNotExistError):
            self.db.alter_column("students", "dormitory", "TEXT NOT NULL")

    def test_rename_column(self):
        professor_before = self.db.get("professors")

        with self.db.transaction(disable_foreign_keys=True):
            self.db.rename_column("professors", "tenured", "is_tenured")

        professor_after = self.db.get("professors")
        self.assertNotIn("tenured", professor_after)
        self.assertEqual(professor_before["tenured"], professor_after["is_tenured"])
        # Sanity check that we didn't mess up other columns.
        professor_before.pop("tenured")
        professor_after.pop("is_tenured")
        self.assertEqual(professor_before, professor_after)

    def test_rename_column_with_constraint(self):
        # Test renaming a column that also has a CHECK constraint with the column's
        # name:
        #
        #     first_name TEXT NOT NULL CHECK(first_name != '')
        #
        # to
        #
        #     legal_name TEXT NOT NULL CHECK(legal_name != '')
        professor_before = self.db.get("professors")

        with self.db.transaction(disable_foreign_keys=True):
            self.db.rename_column("professors", "first_name", "given_name")

        professor_after = self.db.get("professors")
        self.assertNotIn("first_name", professor_after)
        self.assertEqual(professor_before["first_name"], professor_after["given_name"])

        with self.assertRaises(sqlite3.IntegrityError):
            # This should raise an IntegrityError because the CHECK constraint should
            # prevent `given_name` from being empty.
            self.db.insert(
                "professors",
                {
                    "given_name": "",
                    "last_name": "Wadsworth",
                    "department": 1,
                    "tenured": False,
                    "retired": False,
                },
            )

    def test_rename_column_with_non_existent_column(self):
        with self.assertRaises(ColumnDoesNotExistError):
            self.db.alter_column("students", "dormitory", "dormitory_name")

    def test_reorder_columns(self):
        # We convert the rows to a regular dictionary because the OrderedDicts won't
        # compare equal after the reordering operation.
        before = [dict(row) for row in self.db.select("departments", order_by="name")]

        reordered = ["id", "abbreviation", "name"]
        with self.db.transaction(disable_foreign_keys=True):
            self.db.reorder_columns("departments", reordered)

        self.assertEqual(list(self.db.get("departments").keys()), reordered)
        after = [dict(row) for row in self.db.select("departments", order_by="name")]
        self.assertEqual(before, after)

    def test_cannot_modify_read_only_database(self):
        db = Database(":memory:", readonly=True)
        with self.assertRaises(sqlite3.OperationalError):
            db.create_table(
                "people",
                ["name TEXT NOT NULL"],
            )

    def test_create_table_with_quoted_name(self):
        table = 'a"b'
        self.db.create_table(table, ['"c""d" TEXT NOT NULL'])

        self.assertEqual(self.db.count(table), 0)
        self.assertEqual(self.db.get(table), None)
        self.assertEqual(self.db.select(table), [])

        column = 'c"d'
        pk = self.db.insert(table, {column: "Lorem ipsum"})
        row = self.db.get_by_pk(table, pk)
        self.assertEqual(row, {column: "Lorem ipsum"})

        self.db.update_by_pk(table, pk, {column: ""})
        row = self.db.get_by_pk(table, pk)
        self.assertEqual(row, {column: ""})

    def test_create_table_with_incorrect_arguments(self):
        with self.assertRaises(ISqliteError):
            # The second argument should be a list, not a string.
            self.db.create_table("test_table", "name TEXT NOT NULL")

    def test_migration_is_rolled_back_after_error(self):
        schema_before = Schema([Table("t", ["name TEXT"])])
        schema_after = Schema([Table("t", ["name TEXT NOT NULL"])])
        with Database(":memory:", transaction=False) as db:
            db.migrate(schema_before)
            pk = db.insert("t", {"name": None})

            # This will fail because the new schema requires that the `name` column be
            # not null, but we've already inserted a row with a null `name`.
            with self.assertRaises(sqlite3.IntegrityError):
                db.migrate(schema_after)

            # Make sure our data is still there.
            row = db.get_by_pk("t", pk)
            self.assertIsNone(row["name"])

    def test_unique_constraint(self):
        schema = Schema([Table("t", [columns.text("name", unique=True)])])
        with Database(":memory:", transaction=False) as db:
            db.migrate(schema)
            db.insert("t", {"name": "John"})
            with self.assertRaises(sqlite3.IntegrityError):
                db.insert("t", {"name": "John"})

    def test_insert_with_auto_epoch_timestamps(self):
        current_time = int(time.time())
        schema = Schema([AutoTable("t", [], use_epoch_timestamps=True)])
        with Database(
            ":memory:",
            transaction=False,
            insert_auto_timestamp_columns=["created_at", "last_updated_at"],
            use_epoch_timestamps=True,
        ) as db:
            db.migrate(schema)
            row = db.insert_and_get("t", {})

            # Make sure the timestamps calculated in SQL are within a minute of the Unix
            # epoch calculated in Python.
            self.assertGreater(row["created_at"], current_time - 60)
            self.assertLess(row["created_at"], current_time + 60)
            self.assertGreater(row["last_updated_at"], current_time - 60)
            self.assertLess(row["last_updated_at"], current_time + 60)
            self.assertEqual(row["created_at"], row["last_updated_at"])
