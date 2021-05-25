import datetime
import decimal
import sqlite3

from ._core import Database
from ._exception import ISQLiteError


def sqlite3_convert_boolean(b):
    return b != b"0"


def sqlite3_convert_decimal(b):
    return decimal.Decimal(b.decode("utf8"))


def sqlite3_adapt_decimal(d):
    return str(d)


def sqlite3_convert_time(b):
    hour, minute, second = b.decode("utf8").split(":")
    return datetime.time(int(hour), int(minute), int(second))


def sqlite3_adapt_time(t):
    return str(t)


sqlite3.register_converter("BOOLEAN", sqlite3_convert_boolean)
sqlite3.register_converter("DECIMAL", sqlite3_convert_decimal)
sqlite3.register_adapter(decimal.Decimal, sqlite3_adapt_decimal)
sqlite3.register_converter("TIME", sqlite3_convert_time)
sqlite3.register_adapter(datetime.time, sqlite3_adapt_time)
