Command-line interface
======================

The isqlite Python library comes with a command-line program called ``isqlite`` that allows you to query and change SQLite databases from the command line. This page describes the basics of each command supported by ``isqlite``. For full information, run ``isqlite --help`` or ``isqlite <subcommand> --help``.


``add-column``
--------------

Usage::

   isqlite add-column <database> <table>


``alter-column``
----------------

Usage::

   isqlite alter-column <database> <table> <new column definition>


``count``
---------

Usage::

   isqlite count <database> <table> --where <constraint>


``create``
----------

Usage::

   isqlite create <database> <table> <col1>=<val1> <col2>=<val2> ...

``isqlite insert`` is an alias for this subcommand.


``create-table``
----------------

Usage::

   isqlite create-table <database> <table> <col1> <col2> ...


``delete``
----------

Usage::

   isqlite delete <database> <table> <pk>

Unlike ``Database.delete``, the ``delete`` subcommand only supports deletion by primary key.


``drop-column``
---------------

Usage::

   isqlite drop-column <database> <table> <column>


``drop-table``
--------------

Usage::

   isqlite drop-table <database> <table>


``get``
-------

Usage::

   isqlite get <database> <table> <pk>

Unlike ``Database.get``, the ``get`` subcommand only supports fetching by primary key.


``icreate``
-----------

Usage::

    isqlite icreate <database> <table>

The ``icreate`` subcommand will interactively prompt for each column in the table.


``iupdate``
-----------

Usage::

    isqlite iupdate <database> <table> <pk>

The ``iupdate`` subcommand will interactively prompt for each column in the table, displaying the current value of the column if it is already set.


``list``
----------

Usage::

   isqlite list <database> <table>
   isqlite list <database> <table> --where <constraint>
   isqlite list <database> <table> --search <search query>

``isqlite select`` is an alias for this subcommand.


``migrate``
-----------

Usage::

   isqlite migrate <database> <schema>

The Python file at ``schema`` must define the schema in a variable named ``SCHEMA``. See the :doc:`schema docs <schemas>` for details.


``rename-column``
-----------------

Usage::

   isqlite rename-column <database> <table> <old column name> <new column name>


``rename-table``
----------------

Usage::

   isqlite rename-table <database> <old table name> <new table name>


``reorder-columns``
-------------------

Usage::

   isqlite reorder-columns <database> <table> <col1 name> <col2 name> ...


``schema``
----------

Usage::

   isqlite schema <database>
   isqlite schema <database> <table>

If ``table`` is passed, the ``CREATE TABLE`` statement for that table is printed. Otherwise, the list of tables in the database is printed.


``search``
----------

Usage::

   isqlite search <database> <table> <search query>

Alias of ``isqlite list <database> <table> --search <search query>``


``sql``
-------

Usage::

   isqlite sql <database> <raw SQL>


``update``
----------

Usage::

   isqlite update <database> <table> <pk> <col1>=<val1> <col2>=<val2> ...
