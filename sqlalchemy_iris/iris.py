from .base import IRISDialect


class IRISDialect_iris(IRISDialect):
    driver = "iris"

    def create_connect_args(self, url):
        opts = dict(url.query)
        return ([], opts)


dialect = IRISDialect_iris
