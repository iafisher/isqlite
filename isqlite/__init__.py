import datetime
import decimal
import sqlite3

# Re-export some names from sqliteparser.
from sqliteparser.ast import OnConflict, OnDelete, OnUpdate

from .core import AddColumnMigration, AlterColumnMigration
from .core import BooleanColumnStub as BooleanColumn
from .core import (
    ColumnDoesNotExistError,
    CreateTableMigration,
    Database,
    DatabaseMigrator,
)
from .core import DateColumnStub as DateColumn
from .core import DecimalColumnStub as DecimalColumn
from .core import DropColumnMigration, DropTableMigration
from .core import ForeignKeyColumnStub as ForeignKeyColumn
from .core import IntegerColumnStub as IntegerColumn
from .core import (
    ISqliteApiError,
    ISqliteError,
    PrintDebugger,
    ReorderColumnsMigration,
    Table,
    TableDoesNotExistError,
)
from .core import TextColumnStub as TextColumn
from .core import TimeColumnStub as TimeColumn
from .core import TimestampColumnStub as TimestampColumn
from .core import schema_module_to_dict


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
