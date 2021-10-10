# isqlite
isqlite is an improved Python interface to SQLite. It has a more convenient API, support for database migrations, and a command-line interface.


```python
from isqlite import Database

with Database(":memory:") as db:
    pk = db.create("employees", {"name": "John Doe", "age": 30})

    person = db.get_by_pk("employees", pk)
    print(person["name"], person["age"])

    db.update_by_pk("employees", pk, {"age": 35})

    employees = db.list(
        "employees",
        where="name LIKE :name_pattern AND age > 40",
        values={"name_pattern": "John%"},
    )

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
- A convenient Python API
- Database migrations
- A command-line interface


## Installation
Install isqlite with Pip:

```shell
$ pip install isqlite
```


## Documentation
Comprehensive documentation, including the API reference, is available at <https://isqlite.readthedocs.io/en/latest/>.
