isqlite is a wrapper around Python's SQLite library. It has a more convenient API, support for automated schema changes, and a command-line interface.

**WARNING:** isqlite is in beta. Use with caution, and back up your data.

```python
from isqlite import Database, Schema, Table, columns
from isqlite import query as q

# Note that in addition to the columns declared explicitly, isqlite will automatically
# create `id`, `created_at` and `last_updated_at` columns.
schema = Schema([
  Table("teams", [
    columns.Text("name", required=True),
  ]),
  Table("employees", [
    columns.Text("name", required=True),
    columns.Integer("age", required=False),
    columns.ForeignKey("team", "teams", required=False),
  ]),
])

with Database("db.sqlite3") as db:
    # Create the tables defined by the schema in the database. This only needs to be
    # done once.
    schema.create(db)

    # Create a new row in the database.
    pk = db.create("employees", {"name": "John Doe", "age": 30})

    # Retrieve the row as an OrderedDict.
    person = db.get("employees", pk)
    print(person["name"], person["age"])

    # Update the row.
    db.update("employees", pk, {"age": 35})

    # Delete the row.
    db.delete("employees", pk)

    # Filter rows with a query.
    employees = db.list("employees", q.Like("name", "John%") and q.GreaterThan("age", 40))

    # Use raw SQL if necessary.
    pairs = db.sql(
        """
        SELECT
          teams.name, employees.name
        FROM
          employees
        INNER JOIN
          teams
        ON
          employees.team = teams.id
        """
    )
```


## Features
- A more convenient API.
    - e.g., `db.create("people", {"name": "John Doe"})` instead of `cursor.execute("INSERT INTO people VALUES ('John Doe')")`
    - Rows are returned as `OrderedDict` objects instead of tuples.
    - Helper methods to simplify common patterns, e.g. `get_or_create`.
- Easy schema changes: adding, removing, altering and reordering columns.
- Support for `decimal.Decimal`, `datetime.time` and `bool` database columns.
- A command-line interface.


## What isqlite is not
- isqlite is not an ORM. It does not map database rows to native objects. It just returns them as ordered dictionaries.
- isqlite is not a high-performance database engine. If you need maximal SQLite performance, you should use Python's built-in `sqlite3` library instead as isqlite imposes some overhead on top of it.
- isqlite is not a generic database wrapper. It only supports and will only ever support SQLite as the underlying database engine.

isqlite is highly suitable for applications that use SQLite as an [application file format](https://sqlite.org/appfileformat.html). It is less suitable for circumstances in which traditional database engines are used, because if you eventually decide that you need to migrate from SQLite to a full-scale RDMS like MySQL or Postgres, you will have to rewrite all the code that uses isqlite.


## API documentation
TODO
