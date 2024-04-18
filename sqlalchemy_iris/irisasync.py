from .base import IRISDialect


class IRISDialect_irisasync(IRISDialect):
    driver = "irisasync"

    is_async = True
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        import intersystems_iris.dbapi._DBAPI as dbapi

        return dbapi


dialect = IRISDialect_irisasync
