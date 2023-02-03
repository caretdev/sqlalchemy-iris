from .base import IRISDialect


class IRISDialect_iris(IRISDialect):
    driver = "iris"

    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        import intersystems_iris.dbapi._DBAPI as dbapi
        return dbapi


dialect = IRISDialect_iris
