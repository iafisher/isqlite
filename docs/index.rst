isqlite documentation
=====================

isqlite is an improved Python interface to SQLite. It has a more convenient API, support for database migrations, and a command-line interface. It was written by `Ian Fisher <https://iafisher.com>`_ and the source code is available `on GitHub <https://github.com/iafisher/isqlite>`_.

Features
--------
* An improved Python API.
  * e.g., ``db.create("people", {"name": "John Doe"})`` instead of ``cursor.execute("INSERT INTO people VALUES ('John Doe')")``.
  * Rows are returned as ``OrderedDict`` objects instead of tuples.
  * Helper methods to simplify common patterns, e.g. ``get_or_create``.
* Database migrations.
  * Automatically diff the database against a schema defined in Python and apply the results.
  * Or, manually alter the database schema from the command-line using commands like ``isqlite drop-table`` and ``isqlite rename-column``.
* A command-line interface.


Contents
--------

.. toctree::
   :maxdepth: 2

   api


Indices and tables
------------------

* :ref:`genindex`
* :ref:`search`
