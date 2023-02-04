isqlite documentation
=====================

**WARNING: This project is no longer maintained. It is recommended that you use a proper ORM like [SQLAlchemy](https://www.sqlalchemy.org/) or [Django's ORM](https://docs.djangoproject.com/en/4.1/) instead.**

isqlite is an improved Python interface to SQLite. It has a more convenient API, support for schema diffing and migrations, and a command-line interface. It was written by `Ian Fisher <https://iafisher.com>`_ and the source code is available `on GitHub <https://github.com/iafisher/isqlite>`_.

Features
--------
* An improved Python API.
  * e.g., ``db.insert("people", {"name": "John Doe"})`` instead of ``cursor.execute("INSERT INTO people VALUES ('John Doe')")``.
  * Rows are returned as ``OrderedDict`` objects instead of tuples.
  * Helper methods to simplify common patterns, e.g. ``get_or_insert`` and ``insert_many``.
* Database migrations with automatic schema diffing.
  * Automatically diff the database against a schema defined in Python and apply the results.
  * Or, manually alter the database schema from the command-line using commands like ``isqlite drop-table`` and ``isqlite rename-column``.
* A command-line interface.


Usage
-----

isqlite includes a convenient Python API that greatly simplifies working with SQL::

    from isqlite import Database

    with Database(":memory:") as db:
        # Insert a new row into the database.
        pk = db.insert("employees", {"name": "John Doe", "age": 30})
    
        # Retrieve the row as an OrderedDict.
        person = db.get_by_pk("employees", pk)
        print(person["name"], person["age"])
    
        # Update the row.
        db.update_by_pk("employees", pk, {"age": 35})
    
        # Delete the row.
        db.delete_by_pk("employees", pk)
    
        # Filter rows with a query.
        employees = db.select(
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


Contents
--------

.. toctree::
   :maxdepth: 2

   installation
   howto
   schemas
   cli
   limitations
   security
   api


Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`
