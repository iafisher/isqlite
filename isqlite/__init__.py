import datetime
import decimal
import sqlite3

from ._core import AddColumnMigration, AlterColumnMigration
from ._core import BooleanColumnStub as BooleanColumn
from ._core import (
    ColumnDoesNotExistError,
    CreateTableMigration,
    Database,
    DatabaseMigrator,
)
from ._core import DateColumnStub as DateColumn
from ._core import DecimalColumnStub as DecimalColumn
from ._core import DropColumnMigration, DropTableMigration
from ._core import ForeignKeyColumnStub as ForeignKeyColumn
from ._core import IntegerColumnStub as IntegerColumn
from ._core import (
    ISqliteApiError,
    ISqliteError,
    PrintDebugger,
    ReorderColumnsMigration,
    Table,
    TableDoesNotExistError,
)
from ._core import TextColumnStub as TextColumn
from ._core import TimeColumnStub as TimeColumn
from ._core import TimestampColumnStub as TimestampColumn
from ._core import schema_module_to_dict


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
