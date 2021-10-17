Security
========

Protecting against SQL injection
--------------------------------

`SQL injection <https://en.wikipedia.org/wiki/SQL_injection>`_ is when an attacker is able to execute their own SQL statements against the database due to flawed processing of user input. isqlite takes steps to prevent SQL injection attacks, but it does require the programmer to use it correctly.

There are a number of places where Python string interpolation (e.g., ``f"x = {y}"``, ``"x = {}".format(y)``, ``"x = %s" % y``, ``"x = " + y``, or any other operation that directly inserts a value into a string) **MUST NOT** be used:

- In the ``where`` argument to ``Database.list``, ``Database.get``, etc.
- In the names of tables (usually the first argument to ``Database.*`` methods).
- In any other string parameter to a ``Database`` method, such as the ``distinct`` parameter to ``Database.count``.

If you need to interpolate values into a ``where`` argument (as you often do), you can do so safely using the companion ``values`` argument::

   rows = db.list("books", where="title LIKE :title_pattern", values={"title_pattern": "Lord of the Rings%"})

Table and column names should normally be hard-coded. If you really need to accept untrusted user input for the name of a table or a column, you **MUST** use an external library to sanitize the input before passing it to isqlite. Note that the ``quote`` function in isqlite **DOES NOT** make an untrusted string safe to use. It merely ensures that a trusted string is syntatically valid.
