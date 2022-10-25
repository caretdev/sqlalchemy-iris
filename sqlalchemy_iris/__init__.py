from . import base
from . import iris
from .base import IRISDialect
from .iris import IRISDialect_iris

base.dialect = dialect = iris.dialect

__all__ = [
    IRISDialect,
    IRISDialect_iris,
    dialect,
]
