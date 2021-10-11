from abc import ABC

from sqliteparser import ast


class BaseColumn(ABC):
    def __init__(
        self, name, *, required=False, choices=[], default=None, sql_constraints=[]
    ):
        self.name = name
        self.required = required
        self.choices = choices[:]
        self.default = default
        self.sql_constraints = sql_constraints[:]

    def as_sql(self):
        constraints = []
        if self.required:
            constraints.append(not_null_constraint())

        if self.choices:
            if self.required:
                constraints.append(ast.CheckConstraint(self._choices_as_sql()))
            else:
                constraints.append(
                    ast.CheckConstraint(
                        ast.Infix(
                            "OR",
                            ast.Infix("IS", ast.Identifier(self.name), ast.Null()),
                            self._choices_as_sql(),
                        )
                    )
                )

        constraints.extend(self.sql_constraints)
        return ast.Column(
            name=self.name,
            definition=ast.ColumnDefinition(
                type=self.type,
                default=convert_default(self.default),
                constraints=constraints,
            ),
        )

    def _choices_as_sql(self):
        return ast.Infix(
            "IN",
            ast.Identifier(self.name),
            ast.ExpressionList([convert_default(choice) for choice in self.choices]),
        )

    def __str__(self):
        return str(self.as_sql())


class TextColumn(BaseColumn):
    type = "TEXT"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.required and self.default is None:
            self.default = ""

    def as_sql(self):
        constraints = [not_null_constraint()]
        if self.required:
            constraints.append(non_empty_constraint(self.name))

        if self.choices:
            if self.required:
                constraints.append(ast.CheckConstraint(self._choices_as_sql()))
            else:
                constraints.append(
                    ast.CheckConstraint(
                        ast.Infix(
                            "OR",
                            ast.Infix("=", ast.Identifier(self.name), ast.String("")),
                            self._choices_as_sql(),
                        )
                    )
                )

        constraints.extend(self.sql_constraints)
        return ast.Column(
            name=self.name,
            definition=ast.ColumnDefinition(
                type=self.type,
                default=convert_default(self.default),
                constraints=constraints,
            ),
        )


class IntegerColumn(BaseColumn):
    type = "INTEGER"

    def __init__(self, *args, max=None, min=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.max = max
        self.min = min
        if self.max is not None:
            self.sql_constraints.append(
                check_operator_constraint(self.name, "<=", ast.Integer(self.max))
            )
        if self.min is not None:
            self.sql_constraints.append(
                check_operator_constraint(self.name, ">=", ast.Integer(self.min))
            )


class BooleanColumn(BaseColumn):
    type = "BOOLEAN"


class DateColumn(BaseColumn):
    type = "DATE"


class TimestampColumn(BaseColumn):
    type = "TIMESTAMP"


class TimeColumn(BaseColumn):
    type = "TIME"


class DecimalColumn(BaseColumn):
    type = "DECIMAL"


class ForeignKeyColumn(BaseColumn):
    type = "INTEGER"

    def __init__(self, *args, foreign_table, on_delete=ast.OnDelete.SET_NULL, **kwargs):
        super().__init__(*args, **kwargs)
        self.foreign_table = foreign_table
        self.sql_constraints.append(
            ast.ForeignKeyConstraint(
                columns=[],
                foreign_table=self.foreign_table,
                foreign_columns=[],
                on_delete=on_delete,
            )
        )


class PrimaryKeyColumn(IntegerColumn):
    def __init__(self, name):
        super().__init__(
            name,
            required=True,
            sql_constraints=[ast.PrimaryKeyConstraint(autoincrement=True)],
        )


def not_null_constraint():
    return ast.NotNullConstraint()


def non_empty_constraint(name):
    return check_operator_constraint(name, "!=", ast.String(""))


def check_operator_constraint(name, operator, value):
    return ast.CheckConstraint(
        expr=ast.Infix(operator=operator, left=ast.Identifier(name), right=value)
    )


def convert_default(default):
    if default is not None:
        if isinstance(default, str):
            return ast.String(default)
        elif isinstance(default, bool):
            return ast.Integer(int(default))
        elif isinstance(default, int):
            return ast.Integer(default)

    return default
