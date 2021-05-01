from ._exception import ISQLiteError


class BaseColumn:
    def __init__(self, name, *, required, extra=""):
        self.name = name
        self.extra = extra
        self.required = required

    def as_raw_column(self):
        return RawColumn(self.name, " ".join(self._tokens()))

    def _tokens(self):
        tokens = [self.sql_type]
        if self.required:
            tokens.append("NOT NULL")

        if self.extra:
            tokens.append(self.extra)

        return tokens


class Text(BaseColumn):
    sql_type = "TEXT"

    def _tokens(self):
        tokens = [self.sql_type, "NOT NULL"]
        if self.required:
            tokens.append(f"CHECK({self.name} != '')")

        if self.extra:
            tokens.append(self.extra)

        return tokens


class Integer(BaseColumn):
    sql_type = "INTEGER"

    def __init__(self, *args, autoincrement=False, primary_key=False, **kwargs):
        super().__init__(*args, **kwargs)
        if autoincrement is True and primary_key is False:
            raise ISQLiteError(
                "`autoincrement` may only be True when `primary_key` is True."
            )

        self.autoincrement = autoincrement
        self.primary_key = primary_key

    def _tokens(self):
        tokens = super()._tokens()
        if self.primary_key:
            tokens.append("PRIMARY KEY")

        if self.autoincrement:
            tokens.append("AUTOINCREMENT")

        return tokens


class Real(BaseColumn):
    sql_type = "REAL"


class Blob(BaseColumn):
    sql_type = "BLOB"


class Timestamp(BaseColumn):
    sql_type = "TIMESTAMP"


class Boolean(BaseColumn):
    sql_type = "BOOLEAN"


class Decimal(BaseColumn):
    sql_type = "DECIMAL"


class Time(BaseColumn):
    sql_type = "TIME"


class ForeignKey(BaseColumn):
    sql_type = "INTEGER"

    def __init__(self, name, other_table, **kwargs):
        super().__init__(name, **kwargs)
        self.other_table = other_table

    def _tokens(self):
        tokens = super()._tokens()
        tokens.append("REFERENCES")
        tokens.append(self.other_table)
        return tokens


class RawColumn(BaseColumn):
    def __init__(self, name, sql):
        self.name = name
        self.sql = sql

    def as_raw_column(self):
        return self

    def __eq__(self, other):
        if not isinstance(other, RawColumn):
            return NotImplemented

        return self.name == other.name and self.sql == other.sql

    def __str__(self):
        return f"{self.name} {self.sql}"

    def __repr__(self):
        return f"RawColumn({self.name!r}, {self.sql!r})"


class RawConstraint:
    def __init__(self, sql):
        self.sql = sql

    def __eq__(self, other):
        if not isinstance(other, RawConstraint):
            return NotImplemented

        return self.sql == other.sql

    def __str__(self):
        return self.sql

    def __repr__(self):
        return f"RawConstraint({self.sql!r})"
