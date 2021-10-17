# isqlite
isqlite is an improved Python interface to SQLite. It has a more convenient API, support for schema diffing and migrations, and a command-line interface.


```python
from isqlite import Database

with Database(":memory:") as db:
    pk = db.insert("employees", {"name": "John Doe", "age": 30})

    person = db.get_by_pk("employees", pk)
    print(person["name"], person["age"])

    db.update_by_pk("employees", pk, {"age": 35})

    employees = db.select(
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
- Database migrations with automatic schema diffing
- A command-line interface


## Installation
Install isqlite with Pip:

```shell
$ pip install isqlite
```


## Documentation
Comprehensive documentation, including an API reference, is available at <https://isqlite.readthedocs.io/en/stable/>.


## Version history
Version 1.0.0 of isqlite was released on October 17, 2021, after six months of development. isqlite adheres to [semantic versioning](https://semver.org/spec/v2.0.0.html), and detailed information about individual releases can be viewed in the [change log](/CHANGELOG.md).
