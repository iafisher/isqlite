import unittest

from sqliteparser import ast

# We import from isqlite.core because we need access to the raw Column classes, instead
# of the wrapped classes that isqlite exports. External packages should not do this!
from isqlite.core import (
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
            ForeignKeyColumn(
                "project", foreign_table="projects", required=True
            ).as_sql(),
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
