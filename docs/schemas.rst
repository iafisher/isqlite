Database schemas
================

.. warning::
    This project is **no longer maintained** as of February 2023. It is recommended that you use a proper ORM like `SQLAlchemy <https://www.sqlalchemy.org/>`_ or `Django's ORM <https://docs.djangoproject.com/en/4.1/>`_ instead.

Schema-changing operations like creating a new table, altering a column, or dropping a column altogether are common in SQL databases. isqlite makes schema changes easy with a migration system inspired by Django. All you need to do is define your desired schema in Python, and isqlite will compare it against the database's actual schema and make the necessary changes so that they match, while keeping your data intact. You can also directly make schema-changing operations from the command-line, without having to write a schema in Python.


Defining a schema in Python
---------------------------

A schema in Python is defined using ``Schema`` and ``Table`` objects::

   from isqlite import Schema, Table, columns

   SCHEMA = Schema([
     Table(
       "authors",
       [
         "name TEXT",
       ]
     ),
     Table(
       "books",
       [
         columns.text("title"),
         columns.foreign_key("author", foreign_table="authors", required=False),
       ],
     ),
   ])

The ``Table`` constructor accepts the verbatim name of the table, and a list of columns. The columns may either be raw SQL strings, or use the macros provided by ``isqlite.columns``.

For command-line migrations to work, the schema must be defined in a variable called ``SCHEMA`` (case-sensitive).


Migrating the database
----------------------

Once you've written the Python schema, you can migrate the database to it by running ``isqlite migrate <database> <schema>``, where ``<schema>`` is the path to the Python file containing the schema. This command will print the list of changes and prompt you for confirmation before enacting them.

.. note::

   isqlite migrations have some limitations. See :doc:`the "Limitations" page <limitations>` for details.

You can call migrate the database programmatically using ``Database.diff(schema)`` to get a list of the necessary changes to bring the database schema in line with the Python schema, and then ``Database.apply_diff(diff)`` to apply the changes, or else ``Database.migrate(schema)`` to do it all in one step.

.. warning::

   Migrations can cause columns or entire tables to be dropped, so it is **highly** recommended that you use the command-line instead of ``Database.migrate`` or ``Database.apply_diff``.


Manually changing the schema from the command line
--------------------------------------------------

isqlite's command-line interface includes a number of commands to change the database's schema without needing to write a schema in Python, including ``add-column``, ``drop-table``, and more. See the :doc:`CLI reference <cli>` for details.
