Limitations
===========

isqlite is highly suitable for applications that use SQLite as an `application file format <https://sqlite.org/appfileformat.html>`_, and for *ad hoc* operations and migrations on existing SQLite databases. It is less suitable for circumstances in which traditional database engines are used (e.g., web applications), because if you eventually decide that you need to migrate from SQLite to a full-scale RDMS like MySQL or Postgres, you will have to rewrite all the code that uses isqlite.


Compared to SQLAlchemy
----------------------
`SQLAlchemy <https://www.sqlalchemy.org/>`_ is a Python SQL toolkit and ORM and one of the most popular standalone SQL libraries for Python.

- isqlite aims to be a replacement for Python's ``sqlite3`` standard module, not a general-purpose database wrapper like SQLAlchemy. It does not support and will never support any database engine other than SQLite.
- isqlite has a small and easy-to-understand API.
- isqlite supports database migrations out of the box, while SQLAlchemy requires using an extension like `Alembic <https://alembic.sqlalchemy.org/en/latest/>`_.
- isqlite is not an object relational mapper (ORM). It does not map database row to native Python objects. It just returns them as regular ordered dictionaries.
    - Note that SQLAlchemy includes an ORM but does not require that you use it.
- isqlite comes with a command-line interface.
