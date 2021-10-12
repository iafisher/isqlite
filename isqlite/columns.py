from sqliteparser import ast


def base(name, type, *, required=False, choices=[], default=None, constraints=[]):
    if required:
        constraints = [_not_null_constraint()] + constraints

    if choices:
        constraints.append(_choices_constraint(name, choices, required=required))

    return ast.Column(
        name=name,
        definition=ast.ColumnDefinition(
            type=type,
            default=_convert_default(default),
            constraints=constraints,
        ),
    )


def text(name, *, required=False, choices=[], default=None):
    if not required and default is None:
        default = ""

    constraints = [_not_null_constraint()]

    if required:
        constraints.append(_not_empty_constraint(name))

    if choices:
        constraints.append(_choices_constraint(name, choices, required=required))

    return ast.Column(
        name=name,
        definition=ast.ColumnDefinition(
            type="TEXT",
            default=_convert_default(default),
            constraints=constraints,
        ),
    )


def integer(name, *, required=False, choices=[], default=None, max=None, min=None):
    constraints = []
    if max is not None:
        constraints.append(_check_operator_constraint(name, "<=", ast.Integer(max)))

    if min is not None:
        constraints.append(_check_operator_constraint(name, ">=", ast.Integer(min)))

    return base(
        name,
        "INTEGER",
        required=required,
        choices=choices,
        default=default,
        constraints=constraints,
    )


def boolean(name, *, required=False, default=None):
    return base(name, "BOOLEAN", required=required, default=default)


def date(name, *, required=False, default=None):
    return base(name, "DATE", required=required, default=default)


def timestamp(name, *, required=False, default=None):
    return base(name, "TIMESTAMP", required=required, default=default)


def time(name, *, required=False, default=None):
    return base(name, "TIME", required=required, default=default)


def decimal(name, *, required=False, default=None):
    return base(name, "DECIMAL", required=required, default=default)


def foreign_key(
    name, foreign_table, *, required=False, on_delete=ast.OnDelete.SET_NULL
):
    constraints = [
        ast.ForeignKeyConstraint(
            columns=[],
            foreign_table=foreign_table,
            foreign_columns=[],
            on_delete=on_delete,
        )
    ]
    return base(name, "INTEGER", required=required, constraints=constraints)


def primary_key(name, *, autoincrement=True):
    constraints = [
        ast.PrimaryKeyConstraint(autoincrement=autoincrement),
    ]
    return base(name, "INTEGER", required=True, constraints=constraints)


def _not_null_constraint():
    return ast.NotNullConstraint()


def _not_empty_constraint(name):
    return _check_operator_constraint(name, "!=", ast.String(""))


def _check_operator_constraint(name, operator, value):
    return ast.CheckConstraint(
        expr=ast.Infix(operator=operator, left=ast.Identifier(name), right=value)
    )


def _choices_constraint(name, choices, *, required):
    if not required:
        return ast.CheckConstraint(_choices_as_sql(name, choices))
    else:
        return ast.CheckConstraint(
            ast.Infix(
                "OR",
                ast.Infix("IS", ast.Identifier(name), ast.Null()),
                _choices_as_sql(name, choices),
            )
        )


def _choices_as_sql(name, choices):
    return ast.Infix(
        "IN",
        ast.Identifier(name),
        ast.ExpressionList([_convert_default(choice) for choice in choices]),
    )


def _convert_default(default):
    if default is not None:
        if isinstance(default, str):
            return ast.String(default)
        elif isinstance(default, bool):
            return ast.Integer(int(default))
        elif isinstance(default, int):
            return ast.Integer(default)

    return default
