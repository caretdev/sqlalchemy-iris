try:
    import iris

    class Cursor(iris.irissdk.dbapiCursor):
        pass

    class DataRow(iris.irissdk.dbapiDataRow):
        pass

except ImportError:
    pass


def connect(*args, **kwargs):
    return iris.connect(*args, **kwargs)


# globals
apilevel = "2.0"
threadsafety = 0
paramstyle = "qmark"

Binary = bytes
STRING = str
BINARY = bytes
NUMBER = float
ROWID = str


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
