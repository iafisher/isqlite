How-to guide
============

Opening a database
------------------

A connection to a SQLite database is opened with the ``Database`` class, which is designed to be used as a context manager::

    with Database("db.sqlite3") as db:
        ...

This is equivalent to::

    db = Database("db.sqlite3")
    try:
        ...
    except Exception as e:
        db.rollback()
        raise e
    else:
        db.commit()
    finally:
        db.close()

Notice that the scope of the ``with`` statement is also the scope of the database transaction, which is either committed at the end or rolled back if an exception occurs. This ensures that either all of the database operations in the ``with`` statement are applied, or none of them are, protecting you from leaving the database in an inconsistent state.

Like ``sqlite3.connect``, the ``Database`` constructor accepts ``":memory:"`` as a path argument to open an in-memory database. You may also pass ``readonly=True`` to open the database in read-only mode.


Reading rows
------------

To retrieve a list of rows from the database, use ``Database.list``::

    rows = db.list("employees")

This will return every row in the table. isqlite returns rows as ``OrderedDict`` objects, so if your table has an ``id`` column, you could access it as ``row["id"]``. Since the dictionaries are ordered, you can iterate over the keys in the same order that they appear in the database schema.

To filter the list of rows, use the ``where`` parameter to ``Database.list``::

    rows = db.list("employees", where="salary > 50000")

If you need to interpolate values from Python into the ``where`` parameter, use the ``values`` parameter::

    rows = db.list("employees", where="salary > :min_salary", values={"min_salary": 50000})

Do not directly put values into the ``where`` string using Python string interpolation as this will leave you vulnerable to a `SQL injection <https://en.wikipedia.org/wiki/SQL_injection>`_ attack.

``Database.list`` supports several additional parameters such as ``order_by``, ``limit``, and ``offset``, which are described in the :doc:`API reference <api>`.

To retrieve a single row, use ``Database.get``::

   row = db.get("employees", where="salary > 50000")

``Database.get`` works exactly like ``Database.list`` except that it only returns a single row, or ``None`` if no matching rows were found.

To retrieve a row by its primary key, use ``Database.get_by_pk``::

   row = db.get_by_pk("employees", 123)

To count the number of rows, use ``Database.count``::

   n = db.count("employees", where="salary > 50000")

This is equivalent to ``len(db.list(...))``, but it is more efficient because it does not retrieve the actual contents of the rows.


Creating and updating rows
--------------------------

``Database.create`` is used to insert a new row into the database::

   pk = db.create("employees", {"name": "John Doe", "salary": 75000})

``Database.create`` returns the primary key of the row that was inserted, which can then be retrieved with ``Database.get_by_pk``.

To update an existing row or rows, use ``Database.update``::

   db.update("employees", {"yearly_bonus": 1000}, where="tenure > 5")

Like ``Database.list``, ``Database.update`` accepts ``where`` and ``values`` parameters to control which rows to update. To update a single row by its primary key, use ``Database.update_by_pk``::

   db.update_by_pk("employees", 123, {"yearly_bonus": 500})

Multiple rows can be inserted efficiently using ``Database.create_many``::

   db.create_many("employees", [{"name": "John Doe"}, {"name": "Jane Doe"}])

This is the same as::

   for row in data:
       db.create(table, row)

But it uses a single SQL statements instead of N statements.

A common pattern is to query for a particular row and create it if it doesn't exist. isqlite supports this with ``Database.get_or_create``::

   row = db.get_or_create("employees", {"name": "John Doe"})

This will query the ``employees`` table for a row with the name ``John Doe`` and either return it or create it and return it if it does not exist.


Deleting rows
-------------

isqlite provides two methods to delete rows: ``Database.delete`` and ``Database.delete_by_pk``. Like ``Database.list`` and ``Database.update``, ``Database.delete`` accepts ``where`` and ``values`` parameters::

   db.delete("employees", where="tenure > 100")

The ``where`` parameter is required, to prevent you from accidentally deleting every row in the table with ``db.delete(table)``. If you do actually wish to delete every row in the table, you can do ``db.delete(table, where="1")``.


Fetching related rows
---------------------

Often when fetching rows from the database, you also wish to fetch related rows from another table. isqlite makes this easy and efficient with the ``get_related`` option to ``Database.list`` and ``Database.get``.

Imagine you have two database tables defined as follows:

.. code-block:: sql

   CREATE TABLE authors(
       name TEXT,
   );

   CREATE TABLE books(
       title TEXT,
       author INTEGER REFERENCES authors,
   );

Let's say that you want to fetch both a book and its author at the same time. You can do so with ``get_related=["author"]``::

   book = db.get_by_pk("books", 123, get_related=["author"])
   print(book["author"]["name"])

The corresponding row from the ``authors`` table will be fetched and embedded into the returned ``OrderedDict`` object.

``Database.list`` supports the same parameter::

   for books in db.list("books", get_related=["author"]):
       print(book["title"], book["author"]["name"])

If you want to fetch every foreign-key row, you can use ``get_related=True``.

Under the hood, ``get_related`` uses SQL joins to ensure that each operation still only requires a single SQL query.


Using raw SQL
-------------

Sometimes, you may need to write more advanced or fine-tuned SQL queries than the built-in ``Database`` methods support. In such cases, you can execute raw SQL using ``Database.sql``::

   db.sql("SELECT * FROM employees WHERE salary > :salary", values={"salary": 50000})

If you need access to the underlying ``sqlite3.Connection`` object, e.g. for advanced functionality like ``Connection.set_progress_handler``, it is available as ``Database.connection``.


Controlling transactions
------------------------

By default, the ``Database`` object will open a transaction immediately and commit it when the database is closed. More fine-grained control of transactions is available through the ``Database.transaction`` method::

   with Database("db.sqlite3", transaction=False) as db:
       with db.transaction():
           ...

       with db.transaction():
           ...

Each ``with`` statement represents a separate transaction. ``transaction=False`` tells the ``Database`` object to not open a transaction immediately. This means that any statements run outside of a ``Database.transaction()`` block will be committed immediately.

``Datbase.transaction`` is solely intended to be used as a context manager. Its return value should be ignored.


Converters and adapters
-----------------------

`Converters and adapters <https://docs.python.org/3/library/sqlite3.html#sqlite-and-python-types>`_ are Python functions that translate values between Python and SQL. In addition to the default ``datetime.date``/``DATE`` and ``datetime.datetime``/``TIMESTAMP`` functions that Python's ``sqlite3`` module registers, isqlite automatically registers converters and adapters for ``BOOLEAN``, ``DECIMAL``, and ``TIME`` columns, for Python ``bool``, ``decimal.Decimal``, and ``datetime.time`` objects, respectively.

.. note::

   Since ``sqlite3`` converters and adapters are registered globally, importing ``isqlite`` will affect the behavior of all ``sqlite3`` connections in your application, even those that use the ``sqlite3`` module directly.
