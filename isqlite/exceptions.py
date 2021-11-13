class ISqliteError(Exception):
    pass


class ISqliteApiError(ISqliteError):
    pass


class ColumnDoesNotExistError(ISqliteError):
    pass


class TableDoesNotExistError(ISqliteError):
    pass
