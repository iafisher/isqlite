import unittest

from isqlite import query as q


class QueryToSQLTests(unittest.TestCase):
    def test_equals_query_to_sql(self):
        values = {}
        sql = q.Equals("a", "a").to_sql(values)
        self.assertEqual(sql, "a = :v0")
        self.assertEqual(values, {"v0": "a"})

    def test_is_null_query_to_sql(self):
        values = {}
        sql = q.IsNull("a").to_sql(values)
        self.assertEqual(sql, "a IS NULL")
        self.assertEqual(values, {})

    def test_between_query_to_sql(self):
        values = {}
        sql = q.Between("n", 0, 100).to_sql(values)
        self.assertEqual(sql, "n BETWEEN :v0 AND :v1")
        self.assertEqual(values, {"v0": 0, "v1": 100})

    def test_and_query_to_sql(self):
        values = {}
        sql = (q.Equals("first_name", "John") & q.Equals("last_name", "Doe")).to_sql(
            values
        )
        self.assertEqual(sql, "(first_name = :v0) AND (last_name = :v1)")
        self.assertEqual(values, {"v0": "John", "v1": "Doe"})

    def test_or_query_to_sql(self):
        values = {}
        sql = (q.Equals("name", "John") | q.Equals("name", "Jane")).to_sql(values)
        self.assertEqual(sql, "(name = :v0) OR (name = :v1)")
        self.assertEqual(values, {"v0": "John", "v1": "Jane"})

    def test_not_query_to_sql(self):
        values = {}
        sql = (~q.Equals("name", "John")).to_sql(values)
        self.assertEqual(sql, "NOT (name = :v0)")
        self.assertEqual(values, {"v0": "John"})

    def test_raw_sql_query_to_sql(self):
        values = {}
        sql = q.Sql("name = :name", {"name": "John"}).to_sql(values)
        self.assertEqual(sql, "name = :name")
        self.assertEqual(values, {"name": "John"})
