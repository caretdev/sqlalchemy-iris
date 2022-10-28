from sqlalchemy.dialects import registry as _registry

from . import base
from . import iris

base.dialect = dialect = iris.dialect

_registry.register("iris.iris", "sqlalchemy_iris.iris", "IRISDialect_iris")

__all__ = [
    dialect,
]
