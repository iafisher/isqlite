import datetime
import decimal
import sqlite3

# Re-export some names from sqliteparser.
from sqliteparser.ast import OnConflict, OnDelete, OnUpdate

from . import columns, migrations
from .database import (
    AutoTable,
    ColumnDoesNotExistError,
    Database,
    ISqliteApiError,
    ISqliteError,
    PrintDebugger,
    Table,
    TableDoesNotExistError,
)


def sqlite3_convert_boolean(b):
    return b != b"0"


def sqlite3_convert_decimal(b):
    return decimal.Decimal(b.decode("utf8"))


def sqlite3_adapt_decimal(d):
    return str(d)


def sqlite3_convert_time(b):
    parts = b.decode("utf8").split(":", maxsplit=2)
    if len(parts) == 3:
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(parts[2])
    else:
        hour = int(parts[0])
        minute = int(parts[1])
        second = 0

    return datetime.time(hour, minute, second)


def sqlite3_adapt_time(t):
    return str(t)


sqlite3.register_converter("BOOLEAN", sqlite3_convert_boolean)
sqlite3.register_converter("DECIMAL", sqlite3_convert_decimal)
sqlite3.register_adapter(decimal.Decimal, sqlite3_adapt_decimal)
sqlite3.register_converter("TIME", sqlite3_convert_time)
sqlite3.register_adapter(datetime.time, sqlite3_adapt_time)

del sqlite3_convert_boolean
del sqlite3_convert_decimal
del sqlite3_adapt_decimal
del sqlite3_convert_time
del sqlite3_adapt_time
