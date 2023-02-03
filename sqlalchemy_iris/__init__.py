from sqlalchemy.dialects import registry as _registry

from . import base
from . import iris

base.dialect = dialect = iris.dialect

_registry.register("iris.iris", "sqlalchemy_iris.iris", "IRISDialect_iris")
_registry.register("iris.emb", "sqlalchemy_iris.embedded", "IRISDialect_emb")

__all__ = [
    dialect,
]
