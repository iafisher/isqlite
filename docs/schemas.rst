Database schemas
================

Schema-changing operations like creating a new table, altering a column, or dropping a column altogether are common in SQL databases. isqlite makes schema changes easy with a migration system inspired by Django. All you need to do is define your desired schema in Python, and isqlite will compare it against the database's actual schema and make the necessary changes so that they match, while keeping your data intact. You can also directly make schema-changing operations from the command-line, without having to write a schema in Python.


Defining a schema in Python
---------------------------

A schema in Python is a list of ``Table`` objects::

   from isqlite import Table, columns

   SCHEMA = [
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
   ]

The ``Table`` constructor accepts the verbatim name of the table, and a list of columns. The columns may either be raw SQL strings, or use the macros provided by ``isqlite.columns``.


Migrating to the Python schema
------------------------------

From Python code, you can call ``Database.diff(schema)`` to get a list of the necessary changes to bring the database schema in line with the Python schema, and then ``Database.apply_diff(diff)`` to apply the changes, or else ``Database.migrate(schema)`` to do it all in one step, although be **warned** that migrations can cause data loss, so you should always review the changes before applying them. In particular, isqlite will **drop every table** that is in the database but not in the Python schema.

You can also do the migration from the command line with ``isqlite migrate path/to/db.sql path/to/schema.py``. This command will print a list of changes without applying them. To apply them, re-run with the ``--write`` flag.

Note that the ``migrate`` command requires that the schema be defined in a variable called ``SCHEMA`` in whatever Python file is passed.


Changing the schema from the command line
-----------------------------------------

isqlite's command-line interface includes a number of commands to change the database's schema without needing to write a schema in Python, including ``add-column``, ``drop-table``, and more. See the :doc:`CLI reference <cli>` for details.
