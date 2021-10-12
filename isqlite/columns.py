from sqliteparser import ast


def _base(name, type, *, required=True, choices=[], default=None, constraints=[]):
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


def boolean(name, *, required=True, default=None):
    """
    A ``BOOLEAN`` column.

    Note that SQLite lacks a built-in boolean type, and instead represents boolean
    values as ``0`` or ``1``.
    """
    return _base(name, "BOOLEAN", required=required, default=default)


def date(name, *, required=True, default=None):
    """
    A ``DATE`` column for values in ISO 8601 format, e.g. ``2021-01-01``.
    """
    return _base(name, "DATE", required=required, default=default)


def decimal(name, *, required=True, default=None):
    """
    A ``DECIMAL`` column.
    """
    return _base(name, "DECIMAL", required=required, default=default)


def foreign_key(name, foreign_table, *, required=True, on_delete=ast.OnDelete.SET_NULL):
    """
    A foreign key column.
    """
    constraints = [
        ast.ForeignKeyConstraint(
            columns=[],
            foreign_table=foreign_table,
            foreign_columns=[],
            on_delete=on_delete,
        )
    ]
    return _base(name, "INTEGER", required=required, constraints=constraints)


def integer(name, *, required=True, choices=[], default=None, max=None, min=None):
    """
    An ``INTEGER`` column.
    """
    constraints = []
    if max is not None:
        constraints.append(_check_operator_constraint(name, "<=", ast.Integer(max)))

    if min is not None:
        constraints.append(_check_operator_constraint(name, ">=", ast.Integer(min)))

    return _base(
        name,
        "INTEGER",
        required=required,
        choices=choices,
        default=default,
        constraints=constraints,
    )


def primary_key(name, *, autoincrement=True):
    """
    A primary key column.
    """
    constraints = [
        ast.PrimaryKeyConstraint(autoincrement=autoincrement),
    ]
    return _base(name, "INTEGER", required=True, constraints=constraints)


def text(name, *, required=True, choices=[], default=None):
    """
    A ``TEXT`` column.

    There are two possible "empty" values for a ``TEXT`` column: the empty string and
    ``NULL``. To avoid confusion, this function always returns a ``NOT NULL`` column
    so that the only possible empty value is the empty string.
    """
    if not required and default is None:
        default = ""

    constraints = [_not_null_constraint()]

    if required:
        constraints.append(_not_empty_constraint(name))

    if choices:
        constraints.append(
            _choices_constraint(name, choices, required=required, text=True)
        )

    return ast.Column(
        name=name,
        definition=ast.ColumnDefinition(
            type="TEXT",
            default=_convert_default(default),
            constraints=constraints,
        ),
    )


def time(name, *, required=True, default=None):
    """
    A ``TIME`` column for values in HH:MM:SS format
    """
    return _base(name, "TIME", required=required, default=default)


def timestamp(name, *, required=True, default=None):
    """
    A ``TIMESTAMP`` column for values in ISO 8601 format, e.g.
    ``2021-01-01 01:00:00.00``.
    """
    return _base(name, "TIMESTAMP", required=required, default=default)


def _not_null_constraint():
    return ast.NotNullConstraint()


def _not_empty_constraint(name):
    return _check_operator_constraint(name, "!=", ast.String(""))


def _check_operator_constraint(name, operator, value):
    return ast.CheckConstraint(
        expr=ast.Infix(operator=operator, left=ast.Identifier(name), right=value)
    )


def _choices_constraint(name, choices, *, required, text=False):
    if required:
        return ast.CheckConstraint(_choices_as_sql(name, choices))
    else:
        if text:
            null_condition = ast.Infix("=", ast.Identifier(name), ast.String(""))
        else:
            null_condition = ast.Infix("IS", ast.Identifier(name), ast.Null())

        return ast.CheckConstraint(
            ast.Infix("OR", null_condition, _choices_as_sql(name, choices))
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
