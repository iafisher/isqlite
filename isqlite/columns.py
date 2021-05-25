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
