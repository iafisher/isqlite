import decimal
import unittest

from sqliteparser import ast

# We import from isqlite._core because we need access to the raw Column classes, instead
# of the wrapped classes that isqlite exports. External packages should not do this!
from isqlite._core import (
    BooleanColumn,
    DateColumn,
    DecimalColumn,
    ForeignKeyColumn,
    IntegerColumn,
    TextColumn,
    TimeColumn,
    TimestampColumn,
)


class ColumnToSqlTests(unittest.TestCase):
    def test_text_column_to_sql(self):
        self.assertEqual(
            TextColumn("name").as_sql(),
            ast.Column(
                name="name",
                definition=ast.ColumnDefinition(
                    type="TEXT",
                    default=ast.String(""),
                    constraints=[ast.NotNullConstraint()],
                ),
            ),
        )

        self.assertEqual(
            TextColumn("name", required=True).as_sql(),
            ast.Column(
                name="name",
                definition=ast.ColumnDefinition(
                    type="TEXT",
                    constraints=[
                        ast.NotNullConstraint(),
                        ast.CheckConstraint(
                            ast.Infix("!=", ast.Identifier("name"), ast.String(""))
                        ),
                    ],
                ),
            ),
        )

    def test_integer_column_to_sql(self):
        self.assertEqual(
            IntegerColumn("age", max=100).as_sql(),
            ast.Column(
                name="age",
                definition=ast.ColumnDefinition(
                    type="INTEGER",
                    constraints=[
                        ast.CheckConstraint(
                            ast.Infix("<=", ast.Identifier("age"), ast.Integer(100))
                        )
                    ],
                ),
            ),
        )

    def test_boolean_column_to_sql(self):
        self.assertEqual(
            BooleanColumn("is_admin").as_sql(),
            ast.Column(
                name="is_admin", definition=ast.ColumnDefinition(type="BOOLEAN")
            ),
        )

    def test_date_column_to_sql(self):
        self.assertEqual(
            DateColumn("date_of_birth", required=True).as_sql(),
            ast.Column(
                name="date_of_birth",
                definition=ast.ColumnDefinition(
                    type="DATE", constraints=[ast.NotNullConstraint()]
                ),
            ),
        )

    def test_timestamp_column_to_sql(self):
        self.assertEqual(
            TimestampColumn("date_of_birth", required=True).as_sql(),
            ast.Column(
                name="date_of_birth",
                definition=ast.ColumnDefinition(
                    type="TIMESTAMP",
                    constraints=[ast.NotNullConstraint()],
                ),
            ),
        )

    def test_time_column_to_sql(self):
        self.assertEqual(
            TimeColumn("meeting_time", required=True).as_sql(),
            ast.Column(
                name="meeting_time",
                definition=ast.ColumnDefinition(
                    type="TIME", constraints=[ast.NotNullConstraint()]
                ),
            ),
        )

    def test_decimal_column_to_sql(self):
        self.assertEqual(
            DecimalColumn("price", required=True).as_sql(),
            ast.Column(
                name="price",
                definition=ast.ColumnDefinition(
                    type="DECIMAL", constraints=[ast.NotNullConstraint()]
                ),
            ),
        )

    def test_foreign_key_column_to_sql(self):
        self.assertEqual(
            ForeignKeyColumn("project", model="projects", required=True).as_sql(),
            ast.Column(
                name="project",
                definition=ast.ColumnDefinition(
                    type="INTEGER",
                    constraints=[
                        ast.NotNullConstraint(),
                        ast.ForeignKeyConstraint(
                            columns=[],
                            foreign_table="projects",
                            foreign_columns=[],
                            on_delete=ast.OnDelete.SET_NULL,
                        ),
                    ],
                ),
            ),
        )


class ColumnToStringTests(unittest.TestCase):
    def test_text_column_to_string(self):
        # This functions as a test of the BaseColumn.__str__ implementation since
        # TextColumn does not override it.
        self.assertEqual(TextColumn("name").description(), "name (text, optional)")
        self.assertEqual(TextColumn("name", required=True).description(), "name (text)")
        self.assertEqual(
            TextColumn("name", required=True, default="John").description(),
            "name (text, default = 'John')",
        )
        self.assertEqual(
            TextColumn("name", required=True, choices=("John", "Jill")).description(),
            "name (text, choices = ['John', 'Jill'])",
        )
        self.assertEqual(
            TextColumn(
                "name", required=True, choices=("John", "Jill"), default="Jill"
            ).description(),
            "name (text, choices = ['John', 'Jill'], default = 'Jill')",
        )

    def test_integer_column_to_string(self):
        self.assertEqual(IntegerColumn("age").description(), "age (integer, optional)")
        self.assertEqual(
            IntegerColumn("age", required=True).description(), "age (integer)"
        )
        self.assertEqual(
            IntegerColumn(
                "age", required=True, min=18, max=65, default=40
            ).description(),
            "age (integer, min = 18, max = 65, default = 40)",
        )

    def test_boolean_column_to_string(self):
        self.assertEqual(
            BooleanColumn("is_admin", default=False).description(),
            "is_admin (boolean, optional, default = False)",
        )

    def test_date_column_to_string(self):
        self.assertEqual(
            DateColumn("date_of_birth").description(), "date_of_birth (date, optional)"
        )

    def test_timestamp_column_to_string(self):
        self.assertEqual(
            TimestampColumn("date_of_birth").description(),
            "date_of_birth (timestamp, optional)",
        )

    def test_time_column_to_string(self):
        self.assertEqual(
            TimeColumn("meeting_time").description(), "meeting_time (time, optional)"
        )

    def test_decimal_column_to_string(self):
        self.assertEqual(
            DecimalColumn("price").description(), "price (decimal, optional)"
        )

    def test_foreign_key_column_to_string(self):
        self.assertEqual(
            ForeignKeyColumn("project", model="projects").description(),
            "project (integer, optional, foreign key = projects)",
        )


class ColumnValidationTests(unittest.TestCase):
    def test_validate_text_column(self):
        v, is_valid = TextColumn("name").validate("John")
        self.assertTrue(is_valid)
        self.assertEqual(v, "John")

        v, is_valid = TextColumn("name").validate("")
        self.assertTrue(is_valid)
        self.assertEqual(v, "")

        v, is_valid = TextColumn("name", required=True).validate("")
        self.assertFalse(is_valid)

        v, is_valid = TextColumn("name", required=True).validate("John")
        self.assertTrue(is_valid)
        self.assertEqual(v, "John")

        v, is_valid = TextColumn("name", default="John").validate("")
        self.assertTrue(is_valid)
        self.assertEqual(v, "John")

        v, is_valid = TextColumn("name", choices=("John", "Jill")).validate("Jack")
        self.assertFalse(is_valid)

        v, is_valid = TextColumn("name", choices=("John", "Jill")).validate("Jill")
        self.assertTrue(is_valid)
        self.assertEqual(v, "Jill")

    def test_validate_integer_column(self):
        v, is_valid = IntegerColumn("age").validate("abc")
        self.assertFalse(is_valid)

        v, is_valid = IntegerColumn("age").validate("123")
        self.assertTrue(is_valid)
        self.assertEqual(v, 123)

        column = IntegerColumn("age", min=18, max=65)
        v, is_valid = column.validate("25")
        self.assertTrue(is_valid)
        self.assertEqual(v, 25)

        v, is_valid = column.validate("18")
        self.assertTrue(is_valid)
        self.assertEqual(v, 18)

        v, is_valid = column.validate("65")
        self.assertTrue(is_valid)
        self.assertEqual(v, 65)

        v, is_valid = column.validate("17")
        self.assertFalse(is_valid)

        v, is_valid = column.validate("1000")
        self.assertFalse(is_valid)

    def test_validate_boolean_column(self):
        column = BooleanColumn("is_admin")
        v, is_valid = column.validate("true")
        self.assertTrue(is_valid)
        self.assertEqual(v, True)

        v, is_valid = column.validate("false")
        self.assertTrue(is_valid)
        self.assertEqual(v, False)

        v, is_valid = column.validate("1")
        self.assertTrue(is_valid)
        self.assertEqual(v, True)

        v, is_valid = column.validate("0")
        self.assertTrue(is_valid)
        self.assertEqual(v, False)

        v, is_valid = column.validate("yes")
        self.assertFalse(is_valid)

    def test_validate_date_column(self):
        column = DateColumn("date_of_birth")
        v, is_valid = column.validate("2020-01-01")
        self.assertTrue(is_valid)
        self.assertEqual(v, "2020-01-01")

        v, is_valid = column.validate("January 1, 2020")
        self.assertFalse(is_valid)

    def test_validate_timestamp_column(self):
        column = TimestampColumn("date_of_birth")
        v, is_valid = column.validate("2020-01-01 11:00:00.000000+00:00")
        self.assertTrue(is_valid)
        self.assertEqual(v, "2020-01-01 11:00:00.000000+00:00")

        v, is_valid = column.validate("January 1, 2020")
        self.assertFalse(is_valid)

    def test_validate_time_column(self):
        column = TimeColumn("meeting_time")
        v, is_valid = column.validate("11:00")
        self.assertTrue(is_valid)
        self.assertEqual(v, "11:00")

        v, is_valid = column.validate("11 o'clock")
        self.assertFalse(is_valid)

    def test_validate_decimal_column(self):
        column = DecimalColumn("price")
        v, is_valid = column.validate("2.3")
        self.assertTrue(is_valid)
        self.assertEqual(v, decimal.Decimal("2.3"))

        v, is_valid = column.validate("abc")
        self.assertFalse(is_valid)

    def test_validate_foreign_key_column(self):
        column = ForeignKeyColumn("project", model="projects")
        v, is_valid = column.validate("23")
        self.assertTrue(is_valid)
        self.assertEqual(v, 23)

        v, is_valid = column.validate("abc")
        self.assertFalse(is_valid)
