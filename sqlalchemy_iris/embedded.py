from .base import IRISDialect


class IRISDialect_emb(IRISDialect):
    driver = "emb"

    embedded = True
    
    supports_statement_cache = True

    def _get_option(self, connection, option):
        return connection.iris.cls('%SYSTEM.SQL.Util').GetOption(option)

    def _set_option(self, connection, option, value):
        return connection.iris.cls('%SYSTEM.SQL.Util').SetOption(option)

    @classmethod
    def import_dbapi(cls):
        import intersystems_iris.dbapi._DBAPI as dbapi
        return dbapi


dialect = IRISDialect_emb
