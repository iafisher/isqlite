# isqlite
isqlite is an improved Python interface to SQLite. It has a more convenient API, support for database migrations, and a command-line interface.


## Features
- An improved Python API.
    - e.g., `db.create("people", {"name": "John Doe"})` instead of `cursor.execute("INSERT INTO people VALUES ('John Doe')")`
    - Rows are returned as `OrderedDict` objects instead of tuples.
    - Helper methods to simplify common patterns, e.g. `get_or_create`.
- Database migrations.
    - Automatically diff the database against a schema defined in Python and apply the results.
    - Or, manually alter the database schema from the command line using commands like `isqlite drop-table` and `isqlite rename-column`.
- A command-line interface.


## Usage
### Python interface
```python
from isqlite import Database

with Database(":memory:") as db:
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


### Database migrations
#### Automated
In `schema.py` (the exact name of the file does not matter):

```python
from base.sql import ForeignKeyColumn, IntegerColumn, Table, TextColumn

class Book(Table):
    title = TextColumn(required=True)
    author = ForeignKeyColumn(model="authors", required=True)
    pages = IntegerColumn(required=False)


class Authors(Table):
    name = TextColumn(required=True)
```

On the command-line (assuming your database is in `db.sqlite3`):

```shell
$ isqlite --db db.sqlite3 --schema schema.py migrate
```

The `isqlite migrate` command will compare the database file to the Python schema, and print out the changes required to make the database match the schema. To apply the changes, run `isqlite migrate` again with the `--write` flag.

#### Manual
The `isqlite` command-line tool also supports a set of self-explanatory manual migration commands:

- `isqlite add-column`
- `isqlite alter-column`
- `isqlite create-table`
- `isqlite drop-column`
- `isqlite drop-table`
- `isqlite rename-column`
- `isqlite rename-table`
- `isqlite reorder-columns`


## Limitations
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
API documentation is available at <https://isqlite.readthedocs.io/en/latest/>.
