import unittest

from sqliteparser import ast

from isqlite import columns


class ColumnToSqlTests(unittest.TestCase):
    def test_text_column_to_sql(self):
        self.assertEqual(
            columns.text("name"),
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
            columns.text("name", required=True),
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
            columns.integer("age", max=100),
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
            columns.boolean("is_admin"),
            ast.Column(
                name="is_admin", definition=ast.ColumnDefinition(type="BOOLEAN")
            ),
        )

    def test_date_column_to_sql(self):
        self.assertEqual(
            columns.date("date_of_birth", required=True),
            ast.Column(
                name="date_of_birth",
                definition=ast.ColumnDefinition(
                    type="DATE", constraints=[ast.NotNullConstraint()]
                ),
            ),
        )

    def test_timestamp_column_to_sql(self):
        self.assertEqual(
            columns.timestamp("date_of_birth", required=True),
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
            columns.time("meeting_time", required=True),
            ast.Column(
                name="meeting_time",
                definition=ast.ColumnDefinition(
                    type="TIME", constraints=[ast.NotNullConstraint()]
                ),
            ),
        )

    def test_decimal_column_to_sql(self):
        self.assertEqual(
            columns.decimal("price", required=True),
            ast.Column(
                name="price",
                definition=ast.ColumnDefinition(
                    type="DECIMAL", constraints=[ast.NotNullConstraint()]
                ),
            ),
        )

    def test_foreign_key_column_to_sql(self):
        self.assertEqual(
            columns.foreign_key("project", foreign_table="projects", required=True),
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
