class BaseQuery:
    def to_sql(self, values):
        raise NotImplementedError

    def __and__(self, other):
        # A & B
        return And(self, other)

    def __or__(self, other):
        # A | B
        return Or(self, other)

    def __invert__(self):
        # ~A
        return Not(self)


class Sql(BaseQuery):
    def __init__(self, sql, values):
        self.sql = sql
        self.values = values

    def to_sql(self, values):
        for key, value in self.values.items():
            values[key] = value

        return self.sql


class BaseComparator(BaseQuery):
    def __init__(self, column, value):
        self.column = column
        self.value = value

    def to_sql(self, values):
        key = register_key(values, self.value)
        return f"{self.column} {self.operator} :{key}"


class Equals(BaseComparator):
    operator = "="


class GreaterThan(BaseComparator):
    operator = ">"


class LessThan(BaseComparator):
    operator = "<"


class GreaterThanOrEquals(BaseComparator):
    operator = ">="


class LessThanOrEquals(BaseComparator):
    operator = "<="


class Like(BaseComparator):
    operator = "LIKE"


class IsNull(BaseQuery):
    def __init__(self, column):
        self.column = column

    def to_sql(self, values):
        return f"{self.column} IS NULL"


class Between(BaseQuery):
    def __init__(self, column, left, right):
        self.column = column
        self.left = left
        self.right = right

    def to_sql(self, values):
        left_key = register_key(values, self.left)
        right_key = register_key(values, self.right)
        return f"{self.column} BETWEEN :{left_key} AND :{right_key}"


class And(BaseQuery):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def to_sql(self, values):
        left_sql = self.left.to_sql(values)
        right_sql = self.right.to_sql(values)
        return f"({left_sql}) AND ({right_sql})"


class Or(BaseQuery):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def to_sql(self, values):
        left_sql = self.left.to_sql(values)
        right_sql = self.right.to_sql(values)
        return f"({left_sql}) OR ({right_sql})"


class Not(BaseQuery):
    def __init__(self, q):
        self.q = q

    def to_sql(self, values):
        sql = self.q.to_sql(values)
        return f"NOT ({sql})"


def register_key(values, value):
    key = f"v{len(values)}"
    values[key] = value
    return key


def to_sql(query, *, convert_id=False):
    if convert_id and isinstance(query, int):
        query = Equals("id", query)

    values = {}
    if query is None:
        return "", values
    else:
        sql = query.to_sql(values)
        return f"WHERE {sql}", values
