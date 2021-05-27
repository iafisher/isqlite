isqlite is an improved Python interface to SQLite. It has a more convenient API, support for database migrations, and a command-line interface.

**WARNING:** isqlite is in beta. Not all features described here have been implemented yet. If you want to try it out, back up your data first.

```python
from isqlite import Database

with Database(":memory:") as db:
    # Create the tables defined in the database. This only needs to be done once.
    db.create_table("teams", "id INTEGER NOT NULL PRIMARY KEY", "name TEXT NOT NULL")
    db.create_table(
        "employees",
        "id INTEGER NOT NULL PRIMARY KEY",
        "name TEXT NOT NULL",
        "age INTEGER",
        "team INTEGER REFERENCES teams",
    )

    # Create a new row in the database.
    pk = db.create("employees", {"name": "John Doe", "age": 30})

    # Retrieve the row as an OrderedDict.
    person = db.get_by_rowid("employees", pk)
    print(person["name"], person["age"])

    # Update the row.
    db.update_by_rowid("employees", pk, {"age": 35})

    # Delete the row.
    db.delete_by_rowid("employees", pk)

    # Filter rows with a query.
    employees = db.list(
        "employees",
        where="name LIKE :name_pattern AND age > 40",
        values={"name_pattern": "John%"},
    )

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
- Automated database migrations: adding, removing, altering and reordering columns.
- Support for `decimal.Decimal`, `datetime.time` and `bool` database columns.
- A command-line interface.

isqlite is highly suitable for applications that use SQLite as an [application file format](https://sqlite.org/appfileformat.html), and for *ad hoc* operations and migrations on existing SQLite databases. It is less suitable for circumstances in which traditional database engines are used (e.g., web applications), because if you eventually decide that you need to migrate from SQLite to a full-scale RDMS like MySQL or Postgres, you will have to rewrite all the code that uses isqlite.

### Compared to SQLAlchemy
[SQLAlchemy](https://www.sqlalchemy.org/) is a Python SQL toolkit and ORM and one of the most popular standalone SQL libraries for Python.

- isqlite aims to be a replacement for Python's sqlite3 standard library, not a general-purpose database wrapper like SQLAlchemy. It does not support and will never support any database engine other than SQLite.
- isqlite has a small and easy-to-understand API.
- isqlite supports database migrations out of the box, while SQLAlchemy requires using an extension like [Alembic](https://alembic.sqlalchemy.org/en/latest/).
- isqlite is not an object relational mapper (ORM). It does not map database row to native Python objects. It just returns them as regular ordered dictionaries.
    - Note that SQLAlchemy includes an ORM but does not require that you use it.
- isqlite comes with a command-line interface.


## API documentation
TODO
