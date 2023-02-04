API reference
=============

.. warning::
    This project is **no longer maintained** as of February 2023. It is recommended that you use a proper ORM like `SQLAlchemy <https://www.sqlalchemy.org/>`_ or `Django's ORM <https://docs.djangoproject.com/en/4.1/>`_ instead.

``Database`` class
------------------

.. autoclass:: isqlite.Database
    :members:
    :special-members: __init__
    :member-order: bysource


Schema definitions
------------------

.. autoclass:: isqlite.Table

.. autoclass:: isqlite.AutoTable

.. autoclass:: isqlite.Schema
    :members:


Column functions
----------------

.. automodule:: isqlite.columns
    :members:
