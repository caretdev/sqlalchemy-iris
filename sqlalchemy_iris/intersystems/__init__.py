import re
import pkg_resources
from ..base import IRISDialect
from sqlalchemy import text, util
from ..base import IRISExecutionContext
from . import dbapi
from .dbapi import connect, Cursor
from .cursor import InterSystemsCursorFetchStrategy
from .dbapi import IntegrityError, OperationalError, DatabaseError


def remap_exception(func):
    def wrapper(cursor, *args, **kwargs):
        attempt = 0
        while attempt < 3:
            attempt += 1
            try:
                cursor.sqlcode = 0
                return func(cursor, *args, **kwargs)
            except RuntimeError as ex:
                # [SQLCODE: <-119>:...
                message = ex.args[0]
                if '<LIST ERROR>' in message:
                    # just random error happens in the driver, try again
                    continue
                sqlcode = re.findall(r"^\[SQLCODE: <(-\d+)>:", message)
                if not sqlcode:
                    raise Exception(message)
                sqlcode = int(sqlcode[0])
                if abs(sqlcode) in [108, 119, 121, 122]:
                    raise IntegrityError(message)
                if abs(sqlcode) in [1, 12]:
                    raise OperationalError(message)
                raise DatabaseError(message)

    return wrapper


class InterSystemsExecutionContext(IRISExecutionContext):
    cursor_fetch_strategy = InterSystemsCursorFetchStrategy()

    def create_cursor(self):
        cursor = self._dbapi_connection.cursor()
        cursor.sqlcode = 0
        return cursor


class IRISDialect_intersystems(IRISDialect):
    driver = "intersystems"

    execution_ctx_cls = InterSystemsExecutionContext

    supports_statement_cache = True

    supports_cte = False

    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    insert_returning = False
    insert_executemany_returning = False

    logfile = None

    def __init__(self, logfile: str = None, **kwargs):
        self.logfile = logfile
        IRISDialect.__init__(self, **kwargs)

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def connect(self, *cargs, **kwarg):
        host = kwarg.get("hostname", "localhost")
        port = kwarg.get("port", 1972)
        namespace = kwarg.get("namespace", "USER")
        username = kwarg.get("username", "_SYSTEM")
        password = kwarg.get("password", "SYS")
        timeout = kwarg.get("timeout", 10)
        sharedmemory = kwarg.get("sharedmemory", False)
        logfile = kwarg.get("logfile", self.logfile)
        sslconfig = kwarg.get("sslconfig", False)
        autoCommit = kwarg.get("autoCommit", False)
        isolationLevel = kwarg.get("isolationLevel", 1)
        return connect(
            host,
            port,
            namespace,
            username,
            password,
            timeout,
            sharedmemory,
            logfile,
            sslconfig,
            autoCommit,
            isolationLevel,
        )

    def create_connect_args(self, url):
        opts = {}

        opts["application_name"] = "sqlalchemy"
        opts["host"] = url.host
        opts["port"] = int(url.port) if url.port else 1972
        opts["namespace"] = url.database if url.database else "USER"
        opts["username"] = url.username if url.username else ""
        opts["password"] = url.password if url.password else ""

        opts["autoCommit"] = False

        if opts["host"] and "@" in opts["host"]:
            _h = opts["host"].split("@")
            opts["password"] += "@" + _h[0 : len(_h) - 1].join("@")
            opts["host"] = _h[len(_h) - 1]

        return ([], opts)

    def _get_server_version_info(self, connection):
        # get the wheel version from iris module
        try:
            return tuple(
                map(
                    int,
                    pkg_resources.get_distribution(
                        "intersystems_irispython"
                    ).version.split("."),
                )
            )
        except:  # noqa
            return None

    def _get_option(self, connection, option):
        with connection.cursor() as cursor:
            cursor.execute("SELECT %SYSTEM_SQL.Util_GetOption(?)", (option,))
            row = cursor.fetchone()
            if row:
                return row[0]
        return None

    def set_isolation_level(self, connection, level_str):
        if level_str == "AUTOCOMMIT":
            connection.autocommit = True
        else:
            connection.autocommit = False
            if level_str not in ["READ COMMITTED", "READ VERIFIED"]:
                level_str = "READ UNCOMMITTED"
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL " + level_str)

    @remap_exception
    def do_execute(self, cursor, query, params, context=None):
        if query.endswith(";"):
            query = query[:-1]
        self._debug(query, params)
        cursor.execute(query, params)

    @remap_exception
    def do_executemany(self, cursor, query, params, context=None):
        if query.endswith(";"):
            query = query[:-1]
        self._debug(query, params, many=True)
        if params and (len(params[0]) <= 1):
            params = [param[0] if len(param) else None for param in params]
        cursor.executemany(query, params)


dialect = IRISDialect_intersystems
