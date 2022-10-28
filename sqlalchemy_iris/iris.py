from .base import IRISDialect


class IRISDialect_iris(IRISDialect):
    driver = "iris"

    supports_statement_cache = True


dialect = IRISDialect_iris
