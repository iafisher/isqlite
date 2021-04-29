class BaseQuery:
    def __and__(self, other):
        # A & B
        return And(self, other)

    def __or__(self, other):
        # A | B
        return Or(self, other)

    def __invert__(self):
        # ~A
        return Not(self)


class Equals(BaseQuery):
    def __init__(self, column, value):
        self.column = column
        self.value = value

    def to_sql(self, values):
        key = register_key(values, self.value)
        return f"{self.column} = :{key}"


class GreaterThan(BaseQuery):
    def __init__(self, column, value):
        self.column = column
        self.value = value

    def to_sql(self, values):
        key = register_key(values, self.value)
        return f"{self.column} > :{key}"


class LessThan(BaseQuery):
    def __init__(self, column, value):
        self.column = column
        self.value = value

    def to_sql(self, values):
        key = register_key(values, self.value)
        return f"{self.column} < :{key}"


class Like(BaseQuery):
    def __init__(self, column, value):
        self.column = column
        self.value = value

    def to_sql(self, values):
        key = register_key(values, self.value)
        return f"{self.column} LIKE :{key}"


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
