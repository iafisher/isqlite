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


Migration limitations
---------------------

There are some cases in migrations which isqlite cannot handle cleanly.

Renaming columns
^^^^^^^^^^^^^^^^

The automated ``isqlite migrate`` command is not able to detect renamed columns. Instead, it will interpret a renaming as dropping the original column and adding a new column with the new name, which will cause all the data in the column to be lost.

To rename a column, use the ``isqlite rename-column`` command and then update the Python schema to use the new name.


Renaming columns and tables named elsewhere
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A column may be named by a SQL constraint attached to a different column, e.g.:

.. code-block:: sql

   CREATE TABLE employees(
     name TEXT,
     salary INTEGER,
     bonus INTEGER CHECK(salary + bonus < 100000),
   );

And similarly, a table may be named in a different table by a foreign-key constraint.

The ``rename-column`` and ``rename-table`` commands do not check the entire schema for instances of the name to be changed, so any instances that occur outside where the name is defined must be renamed manually (e.g., by ``alter-column`` commands).

Note that isqlite *will* rename all instances of a column in that column's definition. For the ``employees`` table defined below, ``isqlite db.sqlite3 rename-column name legal_name`` would change both the name of the column and the ``CHECK`` constraint to use ``legal_name`` instead of ``name``.

.. code-block:: sql

   CREATE TABLE employees(
     name TEXT NOT NULL CHECK(name != ''),
   );


SQL views
^^^^^^^^^

isqlite is not aware of `SQL views <https://sqlite.org/lang_createview.html>`_ and will neither create, alter, nor drop them.
